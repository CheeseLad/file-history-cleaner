import sys
import os
import re
import json
from datetime import datetime
import base64

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

args = sys.argv[1:]
if len(args) == 0:
    print("Please provide a directory to scan.")
    sys.exit(1)
    
directory = args[0]    

json_data = {
    "delete_count": 0,
    "delete_size": 0,
    "keep_count": 0,
    "keep_size": 0,
    "total_count": 0,
    "files": {}
}

for root, dirs, files in os.walk(directory):
    for file in files:
        file_path = os.path.abspath(os.path.join(root, file))
        file_info = os.stat(file_path)

        timestamp_dt = get_date_from_filename(file)
        timestamp_iso = timestamp_dt.isoformat() + "Z" if timestamp_dt else None

        base_name = remove_date_from_filename(file)
        base_id = encode_string(base_name)

        if base_id not in json_data["files"]:
            json_data["files"][base_id] = {
                "original_path": os.path.abspath(os.path.join(root, base_name)),
                "versions": {}
            }

        version_key = timestamp_dt.strftime("v%Y%m%d%H%M%S") if timestamp_dt else "v_unknown"

        json_data["files"][base_id]["versions"][version_key] = {
            "current_name": file,
            "original_name": base_name,
            "path": file_path,
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