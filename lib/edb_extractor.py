import pyesedb
import csv
import os
from datetime import datetime, timedelta


def filetime_to_dt(filetime):
    # Windows FILETIME to datetime
    return datetime(1601, 1, 1) + timedelta(microseconds=filetime / 10)


def export_table_to_csv(edb_file, table_name, output_csv):
    print(f"Exporting table {table_name} to {output_csv}")
    esedb = pyesedb.file()
    esedb.open(edb_file)
    table = None
    for i in range(esedb.get_number_of_tables()):
        t = esedb.get_table(i)
        if t.get_name().lower() == table_name.lower():
            table = t
            break
    if not table:
        print(f"Table {table_name} not found.")
        esedb.close()
        return

    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = None
        for row_idx in range(table.get_number_of_records()):
            record = table.get_record(row_idx)
            row = {}
            for col_idx in range(table.get_number_of_columns()):
                col = table.get_column(col_idx)
                col_name = col.get_name()
                raw_value = record.get_value_data(col_idx)
                # List of columns that should be integers in the file table
                int_columns = {
                    "id",
                    "parentId",
                    "childId",
                    "state",
                    "status",
                    "fileSize",
                    "tQueued",
                    "tCaptured",
                    "tUpdated",
                }

                if table_name == "string" and col_name == "id":
                    # Try to extract as integer
                    if isinstance(raw_value, int):
                        value = raw_value
                    elif isinstance(raw_value, bytes):
                        value = int.from_bytes(
                            raw_value, byteorder="little", signed=False
                        )
                    else:
                        value = raw_value
                elif table_name == "string" and col_name == "string":
                    # Try UTF-16LE first, then UTF-8, then hex
                    if isinstance(raw_value, bytes):
                        try:
                            value = raw_value.decode("utf-16le")
                        except UnicodeDecodeError:
                            try:
                                value = raw_value.decode("utf-8")
                            except UnicodeDecodeError:
                                value = raw_value.hex()
                    else:
                        value = raw_value
                elif table_name == "file" and col_name in int_columns:
                    if isinstance(raw_value, int):
                        value = raw_value
                    elif isinstance(raw_value, bytes):
                        value = int.from_bytes(
                            raw_value, byteorder="little", signed=False
                        )
                    else:
                        value = raw_value
                else:
                    if raw_value is None:
                        value = ""
                    elif isinstance(raw_value, bytes):
                        try:
                            value = raw_value.decode("utf-8")
                        except UnicodeDecodeError:
                            value = raw_value.hex()
                    else:
                        value = raw_value
                if isinstance(value, str):
                    value = value.rstrip("\x00\r\n ")
                row[col_name] = value
            if writer is None:
                writer = csv.DictWriter(
                    csvfile, fieldnames=row.keys(), quoting=csv.QUOTE_ALL
                )
                writer.writeheader()
            writer.writerow(row)
    print(f"Exported table {table_name} to {output_csv}")
    esedb.close()


if __name__ == "__main__":
    edb_path = os.path.join("Configuration", "Catalog1.edb")
    tables = ["file", "string"]
    for table in tables:
        export_table_to_csv(edb_path, table, f"catalog_py/{table}.csv")
