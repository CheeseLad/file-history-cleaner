import sys
import os
import re
import json
from datetime import datetime
import base64
import shutil

def encode_string(s):
    return base64.urlsafe_b64encode(s.encode()).decode()

def decode_string(encoded):
    return base64.urlsafe_b64decode(encoded.encode()).decode()

def get_date_from_filename(filename):
    match = re.search(r"\((\d{4}_\d{2}_\d{2} \d{2}_\d{2}_\d{2}) UTC\)", filename)
    if match:
        timestamp_str = match.group(1)
        return datetime.strptime(timestamp_str, "%Y_%m_%d %H_%M_%S")
    return None

def remove_date_from_filename(filename):
    return re.sub(r"\s*\(\d{4}_\d{2}_\d{2} \d{2}_\d{2}_\d{2} UTC\)", "", filename)

def main(directory):  

    json_data = {
        "delete_count": 0,
        "delete_size": 0,
        "keep_count": 0,
        "keep_size": 0,
        "total_count": 0,
        "files": {}
    }

    for root, dirs, files in os.walk(directory):
        # Skip processing if we're inside the $OF directory
        if os.path.abspath(root).startswith(os.path.abspath(os.path.join(directory, '$OF'))):
            continue
        
        for file in files:
            file_path = os.path.abspath(os.path.join(root, file))
            file_info = os.stat(file_path)
            folder_path = os.path.relpath(root, start=directory)

            timestamp_dt = get_date_from_filename(file)
            timestamp_iso = timestamp_dt.isoformat() + "Z" if timestamp_dt else None

            base_name = remove_date_from_filename(file)
            base_id = encode_string(base_name)
            
            destination_path = os.path.join(root, base_name)
            destination_path = os.path.relpath(destination_path, start=directory)

            if base_id not in json_data["files"]:
                json_data["files"][base_id] = {
                    #"original_path": os.path.abspath(os.path.join(root, base_name)),
                    "versions": {}
                }

            version_key = timestamp_dt.strftime("v%Y%m%d%H%M%S") if timestamp_dt else "v_unknown"

            json_data["files"][base_id]["versions"][version_key] = {
                "current_name": file,
                "original_name": base_name,
                "src_folder": folder_path,
                "src_path": file_path,
                "dst_path": destination_path,
                "size": file_info.st_size,
                "timestamp": timestamp_iso,
                "timestamp_dt": timestamp_dt  # store for sorting, will remove before saving
            }
            
            json_data["keep_count"] += 1
            json_data["total_count"] += 1
            json_data["keep_size"] += file_info.st_size
            
            print(f"Current file count: {json_data['keep_count']}")

    # Mark versions to delete (all but the most recent one per file)
    for file_data in json_data["files"].values():
        versions = file_data["versions"]
        # Sort by timestamp descending
        sorted_versions = sorted(
            versions.items(),
            key=lambda item: item[1]["timestamp_dt"] or datetime.min,
            reverse=True
        )
        # Keep the most recent
        for i, (v_key, v_data) in enumerate(sorted_versions):
            v_data["to_delete"] = i != 0
            del v_data["timestamp_dt"]  # clean up
            del v_data["timestamp"]  # clean up
            del v_data["current_name"]  # clean up
            del v_data["original_name"]  # clean up
            if v_data["to_delete"]:
                json_data["delete_count"] += 1
                json_data["keep_count"] -= 1
                json_data["delete_size"] += v_data["size"]
                json_data["keep_size"] -= v_data["size"]
                
                print(f"Current delete count: {json_data['delete_count']}")

    output_file = "output.json"
    keep_size_gb = int(json_data['keep_size']) / (1024 ** 3)
    delete_size_gb = int(json_data["delete_size"]) / (1024 ** 3)

    with open(output_file, "w") as f:
        json.dump(json_data, f, indent=2)

    print(f"\nJSON data saved to {output_file}")
    print(f"Total files processed: {json_data['total_count']}")
    print(f"Files to keep: {json_data["keep_count"]} (Total size: {keep_size_gb:.2f} GB)")
    print(f"Files to delete: {json_data["delete_count"]} (Total size: {delete_size_gb:.2f} GB)")
    print("Done.")

def copy_and_rename_files(json_data, output_root):
    for base_id, file_group in json_data["files"].items():
        for version_key, file_entry in file_group["versions"].items():
            src_file = file_entry["src_path"]

            # Normalize dst_path
            dst_path = file_entry["dst_path"].replace(":", "")
            dst_parts = dst_path.replace("\\", "/").split("/")
            dst_parts = [p for p in dst_parts if p]
            
            to_delete = file_entry.get("to_delete", False)
            if to_delete:
                print(f"Skipping deletion for {src_file} as it is marked for deletion.")
                continue

            if not dst_parts:
                print(f"Error: Invalid dst_path: {file_entry['dst_path']}")
                sys.exit(1)

            dest_dir = os.path.join(output_root, *dst_parts[:-1])
            dest_name = dst_parts[-1]
            new_name = file_entry.get("string") or dest_name
            dest_file = os.path.join(dest_dir, new_name)
            
            try:
                os.makedirs(dest_dir, exist_ok=True)
            except Exception as e:
                print(f"Failed to create directory {dest_dir}: {e}")
                sys.exit(1)
            # Check for path length issues (Windows default MAX_PATH is 260)
            if os.name == 'nt' and (len(dest_file) > 255 or len(dest_dir) > 240):
                print(f"Error {dest_file}: path too long")
                sys.exit(1)
            # Check if source file exists
            if not os.path.exists(src_file):
                print(f"Source file does not exist: {src_file}")
                sys.exit(1)
            if os.path.exists(dest_file):
                print(f"File already exists, overwriting: {dest_file}")
            try:
                shutil.copy2(src_file, dest_file)
                print(f"Copied {src_file} -> {dest_file}")
            except Exception as e:
                print(f"Failed to copy {src_file} to {dest_file}: {e}")
                sys.exit(1)
                
if __name__ == "__main__":
        
    directory = r'D:\FileHistory\Jake\JAKE-E7450'  # Change this
    directory = directory + r'\Data'
    
    main(directory)
    folder_info = json.load(open("output.json"))  # This is already a dict, don't load again
    
    of_directory = os.path.join(directory, 'Data', '$OF')
    output_dir = r'./output'

    copy_and_rename_files(folder_info, output_dir)