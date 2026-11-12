# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8d5218f5-c362-45bb-a663-df63815935f9",
# META       "default_lakehouse_name": "lh_dev_hackathon",
# META       "default_lakehouse_workspace_id": "1dcb2f95-5ab2-4d2a-a53e-f93affa50786",
# META       "known_lakehouses": [
# META         {
# META           "id": "8d5218f5-c362-45bb-a663-df63815935f9"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # One Number | DAX Discrepancy Audit
# 
# **Goal:** Compare the revenue calculations actually referenced by the published **Sales**, **Finance**, and **Merchandising** reports. Identify their DAX/aggregation differences, execute them under the same analysis context, and generate a fresh audit result.
# 
# **Scope:** One revenue calculation per report, evaluated for the same period and channel. **No calculation is treated as approved.** Results here compare formulas under an identical filter context; they are not necessarily identical to totals shown in report visuals with their own slicers and filters.
# 
# Run the five Python cells in order. This notebook only reads Fabric definitions and queries the semantic model; it doesn't change the reports.

# MARKDOWN ********************

# ## 1. Retrieve the current report and semantic-model definitions
# 
# This cell connects to Microsoft Fabric, downloads all three report definitions and the shared model's TMDL, and handles asynchronous definition retrieval. The report IDs identify what to inspect, **not** the expected findings.

# CELL ********************

import base64
import json
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import pandas as pd
import requests
from notebookutils import credentials

WORKSPACE_ID = "1dcb2f95-5ab2-4d2a-a53e-f93affa50786"
MODEL_ID = "fb6f1a00-5267-4958-97ca-22765e7f7edc"
REPORTS = {
    "Sales": "d9da1939-86cc-418b-b487-557613fb10d7",
    "Finance": "35149051-03cd-4b21-a73c-322fbb691a08",
    "Merchandising": "d8a11cd1-5568-45e6-a5a2-91eac168a82b",
}
FABRIC_API = "https://api.fabric.microsoft.com/v1"
PBI_API = "https://api.powerbi.com/v1.0/myorg"

# Configure one comparable revenue calculation per report.
# These bindings select what to audit; they do not define or predict causes.
# The code checks that each referenced calculation still exists in Fabric.
# Finance's current visual uses SM Net Sales; its name does not imply approval.
KPI_BINDINGS = {
    "Sales": {"kind": "local", "name": "Sales Revenue (Legacy)", "entity": "factsales"},
    "Finance": {"kind": "model", "name": "SM Net Sales", "entity": "_SM_Measures"},
    "Merchandising": {"kind": "implicit", "name": "Sum(factsales.GrossSalesSignedUSD)"},
}


def api_request(method, url, body=None, tries=5):
    p = urlparse(url)
    if p.scheme != "https" or p.hostname not in {
        "api.fabric.microsoft.com", "api.powerbi.com"
    }:
        raise ValueError("Unexpected API URL: " + url)
    for attempt in range(tries):
        response = requests.request(
            method, url,
            headers={
                "Authorization": "Bearer " + credentials.getToken("pbi"),
                "Content-Type": "application/json",
            },
            json=body, timeout=60,
        )
        if response.status_code in (429, 503) and attempt < tries - 1:
            delay = int(response.headers.get("Retry-After", min(2 ** attempt, 30)))
            time.sleep(max(1, delay))
            continue
        if response.status_code >= 400:
            raise RuntimeError(
                f"HTTP {response.status_code} {method} {url}: {response.text[:2000]}"
            )
        return response
    raise RuntimeError("Fabric request failed after retries")


def get_definition(url, max_seconds=360):
    response = api_request("POST", url)
    if response.status_code == 200:
        return response.json()
    if response.status_code != 202:
        raise RuntimeError(f"Unexpected definition response: {response.status_code}")
    operation_id = response.headers.get("x-ms-operation-id")
    if not operation_id:
        raise RuntimeError("Async response missing x-ms-operation-id")
    status_url = f"{FABRIC_API}/operations/{operation_id}"
    result_url = f"{FABRIC_API}/operations/{operation_id}/result"
    deadline = time.monotonic() + max_seconds
    delay = int(response.headers.get("Retry-After", 5))
    while time.monotonic() < deadline:
        time.sleep(max(1, min(delay, 30)))
        poll = api_request("GET", status_url)
        status = poll.json().get("status", "")
        if status.lower() == "succeeded":
            return api_request("GET", result_url).json()
        if status.lower() in ("failed", "cancelled", "canceled"):
            raise RuntimeError("Fabric definition operation failed: " + poll.text[:1500])
        delay = int(poll.headers.get("Retry-After", 5))
    raise TimeoutError("Fabric getDefinition did not finish before deadline")


def decode_definition(response):
    parts = response.get("definition", {}).get("parts", [])
    if not parts:
        raise ValueError("No definition parts were returned")
    decoded = {}
    for part in parts:
        if part.get("payloadType") == "InlineBase64":
            decoded[part["path"]] = base64.b64decode(
                part["payload"]
            ).decode("utf-8", errors="replace")
    return decoded


report_files = {}
for report, report_id in REPORTS.items():
    endpoint = f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/reports/{report_id}/getDefinition"
    report_files[report] = decode_definition(get_definition(endpoint))
    print(f"{report}: retrieved {len(report_files[report])} definition parts")

model_endpoint = (
    f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/semanticModels/{MODEL_ID}"
    "/getDefinition?format=TMDL"
)
model_files = decode_definition(get_definition(model_endpoint))
print(f"Shared semantic model: retrieved {len(model_files)} definition parts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Discover the formulas used by the reports
# 
# The notebook checks the current report bindings and obtains the associated DAX from the report extensions or the shared model. For an implicit aggregation, it reconstructs the equivalent DAX SUM. Review the **formula inventory** before interpreting any numerical differences.
# 
# If a report's binding changes, the notebook stops with an explicit error instead of substituting an old formula.

# CELL ********************

def collect_queryrefs(obj):
    refs = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key.lower() == "queryref" and isinstance(value, str):
                refs.append(value)
            elif isinstance(value, (dict, list)):
                refs.extend(collect_queryrefs(value))
    elif isinstance(obj, list):
        for value in obj:
            refs.extend(collect_queryrefs(value))
    return refs


def report_queryrefs(files):
    refs = set()
    for path, content in files.items():
        if path.lower().endswith(".json"):
            try:
                refs.update(collect_queryrefs(json.loads(content)))
            except json.JSONDecodeError:
                pass
    return refs


def find_local_measure(files, entity, name):
    hits = []
    for path, content in files.items():
        if path.lower().endswith("reportextensions.json"):
            doc = json.loads(content)
            for table in doc.get("entities", []):
                if table.get("name", "").lower() == entity.lower():
                    for m in table.get("measures", []):
                        if m.get("name", "").lower() == name.lower():
                            hits.append((m.get("expression", ""), path))
    if len(hits) != 1 or not hits[0][0]:
        raise ValueError(f"Expected one report-local DAX expression for {entity}[{name}]; found {len(hits)}")
    return hits[0]


def find_tmdl_measure(files, entity, name):
    hits = []
    rx = re.compile(r"^\s*measure\s+('(?:[^']|'')+'|[^=]+?)\s*=\s*(.*)$", re.I)
    property_rx = re.compile(
        r"^(lineageTag|sourceLineageTag|annotation|formatString|displayFolder|"
        r"description|isHidden|changedProperty|dataCategory|summarizeBy)\b", re.I
    )
    declaration_rx = re.compile(r"^(measure|column|table|partition|hierarchy)\s", re.I)
    for path, content in files.items():
        if not path.lower().endswith(".tmdl"):
            continue
        if path.rsplit("/", 1)[-1].removesuffix(".tmdl").lower() != entity.lower():
            continue
        lines = content.splitlines()
        for i, line in enumerate(lines):
            match = rx.match(line)
            if not match:
                continue
            declared = match.group(1).strip()
            if declared.startswith("'") and declared.endswith("'"):
                declared = declared[1:-1].replace("''", "'")
            if declared.lower() != name.lower():
                continue
            expression_lines = [match.group(2)]
            # Gather DAX continuation, stopping before TMDL metadata/declarations.
            for next_line in lines[i + 1:]:
                stripped = next_line.strip()
                if property_rx.match(stripped) or declaration_rx.match(stripped):
                    break
                if stripped:
                    expression_lines.append(stripped)
            hits.append(("\n".join(expression_lines).strip(), path))
    if len(hits) != 1 or not hits[0][0]:
        raise ValueError(f"Expected one TMDL expression for {entity}[{name}]; found {len(hits)}")
    return hits[0]


def implicit_to_dax(ref):
    m = re.fullmatch(
        r"\s*(SUM|COUNT|MIN|MAX|AVERAGE)\s*\(\s*'?([\w ]+)'?\.([\w ]+)\s*\)\s*",
        ref, re.I
    )
    if not m:
        raise ValueError("Unsupported implicit aggregation: " + ref)
    agg, table, column = m.groups()
    return f"{agg.upper()}('{table.strip()}'[{column}])"


resolved = {}
for report, binding in KPI_BINDINGS.items():
    refs = report_queryrefs(report_files[report])
    if binding["kind"] == "implicit":
        ref = binding["name"]
        if ref.lower() not in {r.lower() for r in refs}:
            raise ValueError(f"{report}: binding {ref} no longer appears in the report")
        dax, path = implicit_to_dax(ref), "visual queryRef (implicit aggregation)"
    else:
        ref = f"{binding['entity']}.{binding['name']}"
        if ref.lower() not in {r.lower() for r in refs}:
            raise ValueError(f"{report}: binding {ref} no longer appears in the report")
        if binding["kind"] == "local":
            dax, path = find_local_measure(report_files[report], binding["entity"], binding["name"])
        else:
            dax, path = find_tmdl_measure(model_files, binding["entity"], binding["name"])
    resolved[report] = {
        "Measure": binding["name"],
        "DAX": dax,
        "Origin": binding["kind"],
        "SourceFile": path,
    }

formula_inventory = pd.DataFrame([
    {"Report": report, **rec} for report, rec in resolved.items()
])
display(formula_inventory[["Report", "Measure", "DAX", "Origin", "SourceFile"]])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Execute the selected calculations under one common context
# 
# This cell evaluates all three retrieved calculations using the same date and channel filters, and independently computes simple transaction-component controls for mathematical validation.
# 
# All results below are fetched **live from your semantic model when you run the cell**. They are not copied from earlier notebook outputs.
# 
# The tests compare *selected formulas*, not complete Power BI visual contexts.

# CELL ********************

# Execute the three currently selected report calculations with identical filters.
# Numerical controls are derived from fact columns, not historical audit outputs.

sales_formula = resolved["Sales"]["DAX"]
merch_formula = resolved["Merchandising"]["DAX"]
finance_binding = KPI_BINDINGS["Finance"]
finance_ref = f"'{finance_binding['entity']}'[{finance_binding['name']}]"

dax_query = f"""
DEFINE
    MEASURE 'factsales'[__AuditSales] = {sales_formula}
    MEASURE 'factsales'[__AuditMerch] = {merch_formula}
EVALUATE
CALCULATETABLE(
    ROW(
        "Sales", [__AuditSales],
        "Finance", {finance_ref},
        "Merchandising", [__AuditMerch],
        "AllNet", SUM('factsales'[NetSalesSignedUSD]),
        "SaleNet", CALCULATE(
            SUM('factsales'[NetSalesSignedUSD]),
            KEEPFILTERS('factsales'[EventType] = "Sale")
        ),
        "AllGross", SUM('factsales'[GrossSalesSignedUSD]),
        "SaleGross", CALCULATE(
            SUM('factsales'[GrossSalesSignedUSD]),
            KEEPFILTERS('factsales'[EventType] = "Sale")
        )
    ),
    DATESBETWEEN(
        'dimdate'[CalendarDate],
        DATE(2026, 9, 1),
        DATE(2026, 9, 30)
    ),
    'dimchannel'[ChannelName] = "Online"
)
"""

query_url = f"{PBI_API}/groups/{WORKSPACE_ID}/datasets/{MODEL_ID}/executeQueries"
response = api_request(
    "POST", query_url,
    body={
        "queries": [{"query": dax_query}],
        "serializerSettings": {"includeNulls": True},
    },
)
result = response.json()
if result.get("error"):
    raise RuntimeError(json.dumps(result["error"], indent=2))
table = result["results"][0]["tables"][0]
if table.get("error"):
    raise RuntimeError(json.dumps(table["error"], indent=2))
if not table.get("rows"):
    raise ValueError("No query results returned")

raw = table["rows"][0]
def value(name):
    number = raw.get(f"[{name}]", raw.get(name))
    if number is None:
        raise ValueError(f"Query returned a blank value for {name}; review the context")
    return float(number)

names = ["Sales", "Finance", "Merchandising", "AllNet", "SaleNet", "AllGross", "SaleGross"]
numbers = {name: value(name) for name in names}

print("Current model calculation results | Sep 2026 | Online | USD")
display(pd.DataFrame([numbers]).round(2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Generate new findings from the formulas and query results
# 
# The analyzer identifies **observable** differences in the retrieved DAX (SUM columns, event filters, `KEEPFILTERS`). It does not use prewritten report-specific reasons.
# 
# For mathematical validation, only a small set of **exact supported formula shapes** can be mapped to the independently computed component controls. If a formula has other logic, the notebook reports **Not evaluated** rather than claiming to know its financial explanation.
# 
# A reconciled numerical bridge validates the values for the selected formulas and common filter context; it does not approve a KPI or validate every report visual.

# CELL ********************

# Compare freshly retrieved formulas and compute an audit report.
# No previous discrepancy values or report-specific causes are embedded here.

SUM_RX = re.compile(r"\bSUM\s*\(\s*'?([\w ]+)'?\s*\[\s*([^\]]+)\s*\]\s*\)", re.I)
EVENT_RX = re.compile(r'\[EventType\]\s*=\s*"([^"]+)"', re.I)


def dax_features(expr):
    return {
        "SumColumns": {
            f"{t.strip().lower()}[{c.strip().lower()}]"
            for t, c in SUM_RX.findall(expr)
        },
        "EventFilters": {v.lower() for v in EVENT_RX.findall(expr)},
        "KeepFilters": bool(re.search(r"\bKEEPFILTERS\s*\(", expr, re.I)),
    }


def describe_formula_difference(a_name, b_name):
    a = dax_features(resolved[a_name]["DAX"])
    b = dax_features(resolved[b_name]["DAX"])
    details = []
    for feature in ("SumColumns", "EventFilters"):
        if a[feature] != b[feature]:
            details.append(
                f"{feature}: {a_name}={sorted(a[feature]) or ['none detected']} | "
                f"{b_name}={sorted(b[feature]) or ['none detected']}"
            )
    if a["KeepFilters"] != b["KeepFilters"]:
        details.append("KEEPFILTERS appears in only one calculation")
    return "; ".join(details) or "No differences detected by supported patterns"


def normalized(expr):
    # Limited canonical form for EXACT templates, not a general DAX parser.
    expr = re.sub(r"/\*.*?\*/", "", expr, flags=re.S)
    expr = re.sub(r"//[^\n]*", "", expr)
    return re.sub(r"\s+", "", expr).replace("'", "").lower()


# Only recognize exact expression shapes. Unknown/complex DAX is not assigned
# a mathematical explanation from these simple transaction controls.
SUPPORTED_FORMULAS = {}
for column, prefix in [
    ("netsalessignedusd", "Net"),
    ("grosssalessignedusd", "Gross"),
]:
    base = f"sum(factsales[{column}])"
    SUPPORTED_FORMULAS[base] = "All" + prefix
    SUPPORTED_FORMULAS[
        f'calculate({base},keepfilters(factsales[eventtype]="sale"))'
    ] = "Sale" + prefix
    SUPPORTED_FORMULAS[
        f'calculate({base},factsales[eventtype]="sale")'
    ] = "Sale" + prefix

report_component = {
    report: SUPPORTED_FORMULAS.get(normalized(info["DAX"]))
    for report, info in resolved.items()
}

rows = []
for a, b in [
    ("Sales", "Finance"),
    ("Merchandising", "Finance"),
    ("Merchandising", "Sales"),
]:
    observed_gap = numbers[a] - numbers[b]
    component_a, component_b = report_component[a], report_component[b]
    if component_a and component_b:
        expected_gap = numbers[component_a] - numbers[component_b]
        remainder = observed_gap - expected_gap
        status = "Reconciled" if abs(remainder) <= 0.01 else "Needs investigation"
        bridge = f"{component_a} - {component_b}"
    else:
        expected_gap = None
        remainder = None
        status = "Not evaluated: unsupported or complex DAX"
        bridge = "No simple mathematical bridge assigned"

    rows.append({
        "Report A": a,
        "Report B": b,
        "Measure A": resolved[a]["Measure"],
        "Measure B": resolved[b]["Measure"],
        "Value A USD": round(numbers[a], 2),
        "Value B USD": round(numbers[b], 2),
        "Variance A-B USD": round(observed_gap, 2),
        "Variance % of B": round(observed_gap / numbers[b] * 100, 2)
            if numbers[b] else None,
        "Detected DAX differences": describe_formula_difference(a, b),
        "Mathematical bridge": bridge,
        "Expected gap USD": round(expected_gap, 2) if expected_gap is not None else None,
        "Unexplained USD": round(remainder, 2) if remainder is not None else None,
        "Validation": status,
        "DAX A": resolved[a]["DAX"],
        "DAX B": resolved[b]["DAX"],
        "Definition source A": resolved[a]["SourceFile"],
        "Definition source B": resolved[b]["SourceFile"],
        "Period": "2026-09-01 to 2026-09-30",
        "Channel": "Online",
        "Currency": "USD",
        "KPI approval": "Not assessed",
        "AuditUTC": datetime.now(timezone.utc).isoformat(),
    })

final_audit = pd.DataFrame(rows)
print("Fresh DAX discrepancy audit")
display(final_audit[[
    "Report A", "Report B", "Variance A-B USD", "Variance % of B",
    "Detected DAX differences", "Mathematical bridge", "Unexplained USD", "Validation"
]])

print("\nComponent mapping (calculated from exact current DAX):")
display(pd.DataFrame([
    {"Report": r, "Detected component": comp or "Unsupported formula"}
    for r, comp in report_component.items()
]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Save the KPI Audit Report to Fabric Lakehouse

# CELL ********************


# STEP 5 — Generate and publish business-friendly KPI audit

import pandas as pd
from pathlib import Path

if "final_audit" not in globals() or final_audit.empty:
    raise ValueError("Run the DAX audit analysis first.")

# Translate detected calculation characteristics
# into business-friendly descriptions.

def calculation_description(report):
    features = dax_features(resolved[report]["DAX"])
    columns = features["SumColumns"]
    events = features["EventFilters"]

    uses_net = any("netsalessignedusd" in c for c in columns)
    uses_gross = any("grosssalessignedusd" in c for c in columns)

    if uses_net:
        base = "Uses signed net sales"
    elif uses_gross:
        base = "Uses signed gross sales"
    else:
        base = "Uses a calculation requiring further review"

    if events == {"sale"}:
        return base + " and includes only Sale transactions"

    if events:
        return base + (
            " with EventType restricted to "
            + ", ".join(sorted(events))
        )

    return base + "; no EventType equality filter detected"


def business_explanation(row):
    report_a = row["Report A"]
    report_b = row["Report B"]

    a = dax_features(resolved[report_a]["DAX"])
    b = dax_features(resolved[report_b]["DAX"])

    findings = []

    if a["SumColumns"] != b["SumColumns"]:
        findings.append(
            f"{report_a} and {report_b} aggregate "
            "different revenue columns"
        )

    if a["EventFilters"] != b["EventFilters"]:
        a_sale = a["EventFilters"] == {"sale"}
        b_sale = b["EventFilters"] == {"sale"}

        if a_sale and not b["EventFilters"]:
            findings.append(
                f"{report_a} includes only Sale "
                f"transactions, while {report_b} "
                "has no equivalent event restriction"
            )
        elif b_sale and not a["EventFilters"]:
            findings.append(
                f"{report_b} includes only Sale "
                f"transactions, while {report_a} "
                "has no equivalent event restriction"
            )
        else:
            findings.append(
                "The calculations apply different "
                "transaction-type restrictions"
            )

    if not findings:
        return (
            "No differences identified by the "
            "supported formula checks"
        )

    return ". ".join(findings) + "."


summary = pd.DataFrame({
    "Report Comparison":
        final_audit["Report A"] + " vs " + final_audit["Report B"],

    "First Report Revenue (USD)":
        final_audit["Value A USD"],

    "Second Report Revenue (USD)":
        final_audit["Value B USD"],

    "Revenue Difference (USD)":
        final_audit["Variance A-B USD"],

    "Revenue Difference (%)":
        final_audit["Variance % of B"],

    "First Report Calculation":
        final_audit["Report A"].apply(calculation_description),

    "Second Report Calculation":
        final_audit["Report B"].apply(calculation_description),

    "Why the Calculations Differ":
        final_audit.apply(business_explanation, axis=1),

    "Unexplained Difference (USD)":
        final_audit["Unexplained USD"],

    "Validation Status":
        final_audit["Validation"],

    "Reporting Period":
        final_audit["Period"],

    "Sales Channel":
        final_audit["Channel"],

    "KPI Approval":
        final_audit["KPI approval"]
})

# Save the business summary and full technical evidence.

folder = Path("/lakehouse/default/Files/KPI_Audit")

if not folder.parent.is_dir():
    raise RuntimeError(
        "Attach lh_dev_hackathon as the default Lakehouse."
    )

folder.mkdir(parents=True, exist_ok=True)

summary_path = folder / "OneNumber_KPI_Audit_Summary.csv"
technical_path = folder / "OneNumber_KPI_Audit_Technical.csv"

summary.to_csv(
    summary_path, index=False, encoding="utf-8-sig"
)

final_audit.to_csv(
    technical_path, index=False, encoding="utf-8-sig"
)

print("KPI audit reports saved successfully.")
print("Business summary:", summary_path)
print("Technical evidence:", technical_path)
print("Comparisons:", len(summary))

display(summary)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pathlib import Path

folder = Path("/lakehouse/default/Files/KPI_Audit")

print("Folder exists:", folder.exists())

if folder.exists():
    print("\nFiles:")
    for file in folder.iterdir():
        print(file.name)
else:
    print("KPI_Audit folder not found.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Publish KPI Audit results as a queryable Delta table

# CELL ********************

import pandas as pd
import re
from pathlib import Path

csv_path = Path(
    "/lakehouse/default/Files/KPI_Audit/"
    "OneNumber_KPI_Audit_Technical.csv"
)

# Read existing audit results
audit_df = pd.read_csv(csv_path)

# Convert column names to Delta-compatible names
def clean_column_name(name):
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")

audit_df.columns = [
    clean_column_name(col)
    for col in audit_df.columns
]

print("Updated column names:")
print(audit_df.columns.tolist())

# Preserve numeric types where possible
numeric_cols = [
    "Value_A_USD",
    "Value_B_USD",
    "Variance_A_B_USD",
    "Variance_of_B",
    "Expected_gap_USD",
    "Unexplained_USD"
]

for col in numeric_cols:
    if col in audit_df.columns:
        audit_df[col] = pd.to_numeric(
            audit_df[col], errors="coerce"
        )

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(
    audit_df.astype(object).where(
        pd.notnull(audit_df), None
    )
)

# Save as a Lakehouse Delta table
table_name = "OneNumber_KPI_Audit_Technical"

(
    spark_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)

print("Lakehouse table created:", table_name)
print("Rows:", spark.table(table_name).count())

display(spark.table(table_name))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
