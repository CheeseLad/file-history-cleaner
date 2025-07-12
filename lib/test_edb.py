import os
from edb_extractor import export_table_to_csv

catalog_dir = r".\edb"
directory = r"Z:\Jake\JAKE-E7450"  # Change this
edb_path = os.path.join(directory, "Configuration", "Catalog1.edb")
tables = ["backupset", "global", "library", "namespace", "file", "string"]
#tables = ["global"]
   
for table in tables:
  print(f"Exporting table: {table}")
  export_table_to_csv(edb_path, table, os.path.join(catalog_dir, f"{table}.csv"))
  print(f"Exported table: {table} to {os.path.join(catalog_dir, f'{table}.csv')}")