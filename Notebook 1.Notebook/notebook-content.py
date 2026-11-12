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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import os

folder = "Files/initial_files"

for f in notebookutils.fs.ls(folder):
    if f.name.lower().endswith(".csv"):
        table_name = os.path.splitext(f.name)[0].lower()
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(f.path))
        df.write.mode("overwrite").format("delta").saveAsTable(f"dbo.{table_name}")
        print(f"Loaded {f.name} -> dbo.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
