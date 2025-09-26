import pyesedb
import csv
import os
from datetime import datetime, timedelta


def filetime_to_dt(filetime):
    # Windows FILETIME to datetime
    return datetime(1601, 1, 1) + timedelta(microseconds=filetime / 10)


def bytes_to_int(bytes_data, signed=False):
    """Convert bytes to integer with proper byte order"""
    if isinstance(bytes_data, bytes):
        return int.from_bytes(bytes_data, byteorder="little", signed=signed)
    return bytes_data


def bytes_to_hex(bytes_data):
    """Convert bytes to hex string"""
    if isinstance(bytes_data, bytes):
        return ' '.join(f'{b:02X}' for b in bytes_data)
    return bytes_data


def export_table_to_csv(edb_file, table_name, output_csv):
    #print(f"Exporting table {table_name} to {output_csv}")
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

    # Try to get the number of records with error handling
    try:
        num_records = table.get_number_of_records()
        print(f"Found {num_records} records in table {table_name}")
    except OSError as e:
        print(f"Error getting number of records for table {table_name}: {e}")
        print("Attempting to process records one by one...")
        num_records = None  # We'll process until we get an error

    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = None
        
        # Process records with error handling
        row_idx = 0
        while True:
            try:
                if num_records is not None and row_idx >= num_records:
                    break
                    
                record = table.get_record(row_idx)
                row = {}
                
                # Get all column names first
                column_names = []
                for col_idx in range(table.get_number_of_columns()):
                    col = table.get_column(col_idx)
                    column_names.append(col.get_name())
                
                # Process based on table type
                if table_name.lower() == "backupset":
                    # backupset table - case 1 in PowerShell
                    for col_idx in range(table.get_number_of_columns()):
                        col_name = column_names[col_idx]
                        raw_value = record.get_value_data(col_idx)
                        
                        if col_name == "id":
                            row["id"] = bytes_to_int(raw_value)
                        elif col_name == "name":
                            if isinstance(raw_value, bytes):
                                try:
                                    row["name"] = raw_value.decode("utf-16le").rstrip('\x00')
                                except UnicodeDecodeError:
                                    row["name"] = raw_value.hex()
                            else:
                                row["name"] = raw_value
                        elif col_name == "description":
                            if isinstance(raw_value, bytes):
                                try:
                                    row["description"] = raw_value.decode("utf-16le").rstrip('\x00')
                                except UnicodeDecodeError:
                                    row["description"] = raw_value.hex()
                            else:
                                row["description"] = raw_value
                        elif col_name == "tCreated":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tCreated"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tCreated"] = raw_value
                        elif col_name == "tModified":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tModified"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tModified"] = raw_value
                        elif col_name == "tExpires":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tExpires"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tExpires"] = raw_value
                        elif col_name == "tQueued":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tQueued"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tQueued"] = raw_value
                        elif col_name == "tCaptured":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tCaptured"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tCaptured"] = raw_value
                        elif col_name == "tUpdated":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tUpdated"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tUpdated"] = raw_value
                        elif col_name == "tCompleted":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tCompleted"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tCompleted"] = raw_value
                        elif col_name == "state":
                            row["state"] = bytes_to_int(raw_value)
                        elif col_name == "status":
                            row["status"] = bytes_to_int(raw_value)
                        elif col_name == "fileCount":
                            row["fileCount"] = bytes_to_int(raw_value)
                        elif col_name == "directoryCount":
                            row["directoryCount"] = bytes_to_int(raw_value)
                        elif col_name == "totalFileSize":
                            row["totalFileSize"] = bytes_to_int(raw_value)
                        elif col_name == "totalDirectorySize":
                            row["totalDirectorySize"] = bytes_to_int(raw_value)
                        elif col_name == "timestamp":
                            # Handle timestamp field specifically
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["timestamp"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["timestamp"] = raw_value
                        else:
                            # For any other columns, try to convert bytes to hex
                            if isinstance(raw_value, bytes):
                                row[col_name] = raw_value.hex()
                            else:
                                row[col_name] = raw_value
                
                elif table_name.lower() == "file":
                    # file table - case 2 in PowerShell
                    for col_idx in range(table.get_number_of_columns()):
                        col_name = column_names[col_idx]
                        raw_value = record.get_value_data(col_idx)
                        
                        if col_name == "id":
                            row["id"] = bytes_to_int(raw_value)
                        elif col_name == "backupsetId":
                            row["backupsetId"] = bytes_to_int(raw_value)
                        elif col_name == "parentId":
                            row["parentId"] = bytes_to_int(raw_value)
                        elif col_name == "nameId":
                            row["nameId"] = bytes_to_int(raw_value)
                        elif col_name == "size":
                            row["size"] = bytes_to_int(raw_value)
                        elif col_name == "tCreated":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tCreated"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tCreated"] = raw_value
                        elif col_name == "tModified":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tModified"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tModified"] = raw_value
                        elif col_name == "tAccessed":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tAccessed"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tAccessed"] = raw_value
                        elif col_name == "attributes":
                            row["attributes"] = bytes_to_int(raw_value)
                        elif col_name == "hash":
                            if isinstance(raw_value, bytes):
                                row["hash"] = raw_value.hex()
                            else:
                                row["hash"] = raw_value
                        elif col_name == "timestamp":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["timestamp"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["timestamp"] = raw_value
                        else:
                            # For any other columns, try to convert bytes to hex
                            if isinstance(raw_value, bytes):
                                row[col_name] = raw_value.hex()
                            else:
                                row[col_name] = raw_value
                
                elif table_name.lower() == "string":
                    # string table - case 3 in PowerShell
                    for col_idx in range(table.get_number_of_columns()):
                        col_name = column_names[col_idx]
                        raw_value = record.get_value_data(col_idx)
                        
                        if col_name == "id":
                            row["id"] = bytes_to_int(raw_value)
                        elif col_name == "value":
                            if isinstance(raw_value, bytes):
                                try:
                                    row["value"] = raw_value.decode("utf-16le").rstrip('\x00')
                                except UnicodeDecodeError:
                                    row["value"] = raw_value.hex()
                            else:
                                row["value"] = raw_value
                        elif col_name == "timestamp":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["timestamp"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["timestamp"] = raw_value
                        else:
                            # For any other columns, try to convert bytes to hex
                            if isinstance(raw_value, bytes):
                                row[col_name] = raw_value.hex()
                            else:
                                row[col_name] = raw_value
                
                elif table_name.lower() == "namespace":
                    # namespace table - case 4 in PowerShell
                    for col_idx in range(table.get_number_of_columns()):
                        col_name = column_names[col_idx]
                        raw_value = record.get_value_data(col_idx)
                        
                        if col_name == "id":
                            row["id"] = bytes_to_int(raw_value)
                        elif col_name == "parentId":
                            row["parentId"] = bytes_to_int(raw_value)
                        elif col_name == "nameId":
                            row["nameId"] = bytes_to_int(raw_value)
                        elif col_name == "tCreated":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tCreated"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tCreated"] = raw_value
                        elif col_name == "tModified":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tModified"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tModified"] = raw_value
                        elif col_name == "tAccessed":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tAccessed"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tAccessed"] = raw_value
                        elif col_name == "attributes":
                            row["attributes"] = bytes_to_int(raw_value)
                        elif col_name == "timestamp":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["timestamp"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["timestamp"] = raw_value
                        else:
                            # For any other columns, try to convert bytes to hex
                            if isinstance(raw_value, bytes):
                                row[col_name] = raw_value.hex()
                            else:
                                row[col_name] = raw_value
                
                elif table_name.lower() == "library":
                    # library table - case 5 in PowerShell
                    for col_idx in range(table.get_number_of_columns()):
                        col_name = column_names[col_idx]
                        raw_value = record.get_value_data(col_idx)
                        
                        if col_name == "id":
                            row["id"] = bytes_to_int(raw_value)
                        elif col_name == "backupsetId":
                            row["backupsetId"] = bytes_to_int(raw_value)
                        elif col_name == "nameId":
                            row["nameId"] = bytes_to_int(raw_value)
                        elif col_name == "tCreated":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tCreated"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tCreated"] = raw_value
                        elif col_name == "tModified":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tModified"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tModified"] = raw_value
                        elif col_name == "tAccessed":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["tAccessed"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["tAccessed"] = raw_value
                        elif col_name == "attributes":
                            row["attributes"] = bytes_to_int(raw_value)
                        elif col_name == "timestamp":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["timestamp"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["timestamp"] = raw_value
                        else:
                            # For any other columns, try to convert bytes to hex
                            if isinstance(raw_value, bytes):
                                row[col_name] = raw_value.hex()
                            else:
                                row[col_name] = raw_value
                
                elif table_name.lower() == "global":
                    # global table - case 6 in PowerShell
                    for col_idx in range(table.get_number_of_columns()):
                        col_name = column_names[col_idx]
                        raw_value = record.get_value_data(col_idx)
                        
                        if col_name == "id":
                            row["id"] = bytes_to_int(raw_value)
                        elif col_name == "nameId":
                            row["nameId"] = bytes_to_int(raw_value)
                        elif col_name == "valueId":
                            row["valueId"] = bytes_to_int(raw_value)
                        elif col_name == "timestamp":
                            if isinstance(raw_value, (int, bytes)):
                                filetime = bytes_to_int(raw_value)
                                row["timestamp"] = filetime_to_dt(filetime).isoformat()
                            else:
                                row["timestamp"] = raw_value
                        else:
                            # For any other columns, try to convert bytes to hex
                            if isinstance(raw_value, bytes):
                                row[col_name] = raw_value.hex()
                            else:
                                row[col_name] = raw_value
                
                else:
                    # Generic processing for unknown tables
                    for col_idx in range(table.get_number_of_columns()):
                        col_name = column_names[col_idx]
                        raw_value = record.get_value_data(col_idx)
                        
                        # Try to convert bytes to hex for unknown columns
                        if isinstance(raw_value, bytes):
                            row[col_name] = raw_value.hex()
                        else:
                            row[col_name] = raw_value
                
                # Write the row to CSV
                if writer is None:
                    writer = csv.DictWriter(csvfile, fieldnames=row.keys())
                    writer.writeheader()
                
                writer.writerow(row)
                row_idx += 1
                
            except (OSError, IndexError) as e:
                print(f"Error processing record {row_idx} in table {table_name}: {e}")
                break  # Stop processing if we encounter an error
            except Exception as e:
                print(f"Unexpected error processing record {row_idx} in table {table_name}: {e}")
                row_idx += 1  # Continue to next record for other errors
                continue
    
    esedb.close()
    print(f"Successfully exported {row_idx} records from table {table_name} to {output_csv}")
