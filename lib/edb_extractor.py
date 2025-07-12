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

    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = None
        
        for row_idx in range(table.get_number_of_records()):
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
                        row[col_name] = raw_value
                        
            elif table_name.lower() == "global":
                # global table - case 2 in PowerShell
                for col_idx in range(table.get_number_of_columns()):
                    col_name = column_names[col_idx]
                    raw_value = record.get_value_data(col_idx)
                    
                    if col_name == "id":
                        row["id"] = bytes_to_int(raw_value)
                    elif col_name == "key":
                        if isinstance(raw_value, bytes):
                            try:
                                row["key"] = raw_value.decode("utf-16le").rstrip('\x00')
                            except UnicodeDecodeError:
                                try:
                                    row["key"] = raw_value.decode("utf-8").rstrip('\x00')
                                except UnicodeDecodeError:
                                    row["key"] = raw_value.hex()
                        else:
                            row["key"] = raw_value
                    elif col_name == "value":
                        # Handle binary values intelligently based on content
                        if isinstance(raw_value, bytes):
                            if len(raw_value) == 0:
                                row["value"] = ""
                            elif len(raw_value) == 1:
                                # Single byte - treat as integer
                                row["value"] = str(raw_value[0])
                            elif len(raw_value) == 4:
                                # 4 bytes - likely an integer
                                try:
                                    int_val = int.from_bytes(raw_value, byteorder="little", signed=False)
                                    row["value"] = str(int_val)
                                except:
                                    row["value"] = bytes_to_hex(raw_value)
                            elif len(raw_value) == 8:
                                # 8 bytes - could be FILETIME or 64-bit integer
                                try:
                                    int_val = int.from_bytes(raw_value, byteorder="little", signed=False)
                                    # Check if it looks like a FILETIME (large number)
                                    if int_val > 100000000000000000:  # Rough threshold for FILETIME
                                        row["value"] = filetime_to_dt(int_val).isoformat()
                                    else:
                                        row["value"] = str(int_val)
                                except:
                                    row["value"] = bytes_to_hex(raw_value)
                            else:
                                # Special handling for known complex structures
                                # Get the key name to handle specific cases
                                key_name = row.get("key", "")
                                if key_name == "EventListenerWatermarks":
                                    # Parse EventListenerWatermarks structure
                                    if len(raw_value) >= 8:
                                        watermark_count = int.from_bytes(raw_value[0:4], byteorder="little", signed=False)
                                        listener_count = int.from_bytes(raw_value[4:8], byteorder="little", signed=False)
                                        # Try to extract the path if present
                                        path_part = raw_value[8:]
                                        path_str = ""
                                        if path_part:
                                            # Look for the path structure - it might be UTF-16LE with null terminators
                                            # Find the first double null (UTF-16LE string terminator)
                                            null_pos = path_part.find(b'\x00\x00')
                                            if null_pos != -1:
                                                path_part = path_part[:null_pos]
                                                # Remove single null bytes (UTF-16LE encoding)
                                                clean_path = b''
                                                for i in range(0, len(path_part), 2):
                                                    if i + 1 < len(path_part):
                                                        if path_part[i] != 0 or path_part[i+1] != 0:
                                                            clean_path += path_part[i:i+2]
                                                if clean_path:
                                                    try:
                                                        path_str = clean_path.decode("utf-16le")
                                                    except:
                                                        path_str = clean_path.decode("utf-8", errors='ignore')
                                            else:
                                                # If no double null found, try to decode as-is
                                                try:
                                                    path_str = path_part.decode("utf-16le", errors='ignore')
                                                except:
                                                    path_str = path_part.decode("utf-8", errors='ignore')
                                        row["value"] = f"Watermarks:{watermark_count} Listeners:{listener_count} Path:{path_str}"
                                    else:
                                        row["value"] = bytes_to_hex(raw_value)
                                elif key_name == "VersionInformation":
                                    # Parse version information (3 32-bit integers)
                                    if len(raw_value) >= 12:
                                        major = int.from_bytes(raw_value[0:4], byteorder="little", signed=False)
                                        minor = int.from_bytes(raw_value[4:8], byteorder="little", signed=False)
                                        build = int.from_bytes(raw_value[8:12], byteorder="little", signed=False)
                                        row["value"] = f"{major}.{minor}.{build}"
                                    else:
                                        row["value"] = bytes_to_hex(raw_value)
                                else:
                                    # For other complex binary structures, try to parse intelligently
                                    try:
                                        # Remove null bytes first
                                        clean_bytes = raw_value.rstrip(b'\x00')
                                        if clean_bytes:
                                            # Try UTF-16LE first
                                            decoded = clean_bytes.decode("utf-16le")
                                            if decoded.strip():
                                                row["value"] = decoded
                                            else:
                                                # If it's mostly null bytes, it might be a structured binary
                                                # Try to parse as structured data
                                                non_null_bytes = [b for b in raw_value if b != 0]
                                                if len(non_null_bytes) < len(raw_value) * 0.3:  # Mostly nulls
                                                    # Try to extract meaningful parts
                                                    parts = []
                                                    for i in range(0, len(raw_value), 4):
                                                        if i + 4 <= len(raw_value):
                                                            val = int.from_bytes(raw_value[i:i+4], byteorder="little", signed=False)
                                                            if val != 0:
                                                                parts.append(str(val))
                                                    if parts:
                                                        row["value"] = " | ".join(parts)
                                                    else:
                                                        row["value"] = bytes_to_hex(raw_value)
                                                else:
                                                    row["value"] = bytes_to_hex(raw_value)
                                        else:
                                            row["value"] = ""
                                    except UnicodeDecodeError:
                                        # If string decoding fails, show as hex
                                        row["value"] = bytes_to_hex(raw_value)
                        else:
                            row["value"] = raw_value
                    else:
                        row[col_name] = raw_value
                        
            elif table_name.lower() == "library":
                # library table - case 3 in PowerShell
                for col_idx in range(table.get_number_of_columns()):
                    col_name = column_names[col_idx]
                    raw_value = record.get_value_data(col_idx)
                    
                    if col_name == "id":
                        row["id"] = bytes_to_int(raw_value)
                    elif col_name == "parentId":
                        row["parentId"] = bytes_to_int(raw_value)
                    elif col_name == "childId":
                        row["childId"] = bytes_to_int(raw_value)
                    elif col_name == "tCreated":
                        row["tCreated"] = bytes_to_int(raw_value)
                    elif col_name == "tVisible":
                        row["tVisible"] = bytes_to_int(raw_value)
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
                    elif col_name == "childId":
                        row["childId"] = bytes_to_int(raw_value)
                    elif col_name == "status":
                        row["status"] = bytes_to_int(raw_value, signed=True)  # Int16
                    elif col_name == "fileAttrib":
                        row["fileAttrib"] = bytes_to_int(raw_value)
                    elif col_name == "fileCreated":
                        if isinstance(raw_value, (int, bytes)):
                            filetime = bytes_to_int(raw_value)
                            row["fileCreated"] = filetime_to_dt(filetime).isoformat()
                        else:
                            row["fileCreated"] = raw_value
                    elif col_name == "fileModified":
                        if isinstance(raw_value, (int, bytes)):
                            filetime = bytes_to_int(raw_value)
                            row["fileModified"] = filetime_to_dt(filetime).isoformat()
                        else:
                            row["fileModified"] = raw_value
                    elif col_name == "usn":
                        row["usn"] = bytes_to_int(raw_value)
                    elif col_name == "tCreated":
                        row["tCreated"] = bytes_to_int(raw_value)
                    elif col_name == "tVisible":
                        row["tVisible"] = bytes_to_int(raw_value)
                    elif col_name == "fileRecordId":
                        row["fileRecordId"] = bytes_to_int(raw_value)
                    else:
                        row[col_name] = raw_value
                        
            elif table_name.lower() == "string":
                # string table - case 5 in PowerShell
                for col_idx in range(table.get_number_of_columns()):
                    col_name = column_names[col_idx]
                    raw_value = record.get_value_data(col_idx)
                    
                    if col_name == "id":
                        row["id"] = bytes_to_int(raw_value)
                    elif col_name == "string":
                        if isinstance(raw_value, bytes):
                            try:
                                row["string"] = raw_value.decode("utf-16le").rstrip('\x00')
                            except UnicodeDecodeError:
                                try:
                                    row["string"] = raw_value.decode("utf-8").rstrip('\x00')
                                except UnicodeDecodeError:
                                    row["string"] = raw_value.hex()
                        else:
                            row["string"] = raw_value
                    else:
                        row[col_name] = raw_value
                        
            elif table_name.lower() == "file":
                # file table handling
                for col_idx in range(table.get_number_of_columns()):
                    col_name = column_names[col_idx]
                    raw_value = record.get_value_data(col_idx)
                    
                    if col_name == "id":
                        row["id"] = bytes_to_int(raw_value)
                    elif col_name == "parentId":
                        row["parentId"] = bytes_to_int(raw_value)
                    elif col_name == "childId":
                        row["childId"] = bytes_to_int(raw_value)
                    elif col_name == "state":
                        row["state"] = bytes_to_int(raw_value)
                    elif col_name == "status":
                        row["status"] = bytes_to_int(raw_value)
                    elif col_name == "fileSize":
                        row["fileSize"] = bytes_to_int(raw_value)
                    elif col_name == "tQueued":
                        row["tQueued"] = bytes_to_int(raw_value)
                    elif col_name == "tCaptured":
                        row["tCaptured"] = bytes_to_int(raw_value)
                    elif col_name == "tUpdated":
                        row["tUpdated"] = bytes_to_int(raw_value)
                    else:
                        row[col_name] = raw_value
            else:
                # Default handling for other tables
                for col_idx in range(table.get_number_of_columns()):
                    col_name = column_names[col_idx]
                    raw_value = record.get_value_data(col_idx)
                    
                    if raw_value is None:
                        row[col_name] = ""
                    elif isinstance(raw_value, bytes):
                        try:
                            row[col_name] = raw_value.decode("utf-8").rstrip('\x00')
                        except UnicodeDecodeError:
                            row[col_name] = raw_value.hex()
                    else:
                        row[col_name] = raw_value
            
            if writer is None:
                writer = csv.DictWriter(
                    csvfile, fieldnames=row.keys(), quoting=csv.QUOTE_ALL
                )
                writer.writeheader()
            writer.writerow(row)
    
    #print(f"Exported table {table_name} to {output_csv}")
    esedb.close()
