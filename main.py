import sys
import os
import re
import json
from datetime import datetime
import base64
import shutil
from lib.copy_and_rename_files import copy_and_rename_files


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


def main(directory, directories_to_skip=[], save_json=True, has_data_directory=True):

    if has_data_directory:
        data_directory = directory + r"\Data"
    else:
        data_directory = directory

    json_data = {
        "delete_count": 0,
        "delete_size": 0,
        "keep_count": 0,
        "keep_size": 0,
        "total_count": 0,
        "total_size": 0,
        "files": {},
        "deleted_files": [],
    }

    for root, dirs, files in os.walk(data_directory):
        if os.path.abspath(root).startswith(
            os.path.abspath(os.path.join(data_directory, "$OF"))
        ):
            print(f"[INFO] Skipping directory: {root}")
            continue

        dirs[:] = [d for d in dirs if d not in directories_to_skip]

        for file in files:
            file_path = os.path.abspath(os.path.join(root, file))
            file_info = os.stat(file_path)
            folder_path = os.path.relpath(root, start=data_directory)

            timestamp_dt = get_date_from_filename(file)
            timestamp_iso = timestamp_dt.isoformat() + "Z" if timestamp_dt else None

            base_name = remove_date_from_filename(file)

            destination_path = os.path.join(root, base_name)
            destination_path = os.path.relpath(destination_path, start=data_directory)

            base_id = encode_string(destination_path)

            if base_id not in json_data["files"]:
                json_data["files"][base_id] = {"versions": {}}

            version_key = (
                timestamp_dt.strftime("v%Y%m%d%H%M%S") if timestamp_dt else "v_unknown"
            )

            json_data["files"][base_id]["versions"][version_key] = {
                "current_name": file,
                "original_name": base_name,
                "src_folder": folder_path,
                "src_path": file_path,
                "dst_path": destination_path,
                "size": file_info.st_size,
                "timestamp": timestamp_iso,
                "timestamp_dt": timestamp_dt,  # store for sorting, will remove before saving
            }

            json_data["total_count"] += 1
            json_data["total_size"] += file_info.st_size
            print(f"[INFO] Processed file: {file_path}")

    # Mark versions to delete (all but the most recent one per file)
    for file_data in json_data["files"].values():
        versions = file_data.get("versions", {})

        # Step 1: Parse ISO 8601 timestamps into datetime objects
        for version in versions.values():
            ts = version.get("timestamp")
            if ts:
                version["timestamp_dt"] = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
            else:
                version["timestamp_dt"] = datetime.min

        # Step 2: Sort by timestamp_dt (newest first)
        sorted_versions = sorted(
            versions.items(), key=lambda item: item[1]["timestamp_dt"], reverse=True
        )

        file_data["versions"] = {k: v for k, v in sorted_versions}

        for i, version in enumerate(file_data["versions"].values()):
            version.pop("timestamp_dt", None)
            # remove current_name as it's not needed in final output
            version.pop("current_name", None)
            # remove original_name as it's not needed in final output
            version.pop("original_name", None)
            if i == 0:
                version["to_delete"] = False  # keep newest version
                json_data["keep_count"] += 1
                json_data["keep_size"] += version["size"]
            else:
                version["to_delete"] = True  # mark others to delete
                json_data["delete_count"] += 1
                json_data["delete_size"] += version["size"]
                json_data["deleted_files"].append(version["src_path"])

    if save_json:
        output_file = "output.json"

        with open(output_file, "w") as f:
            json.dump(json_data, f, indent=2)
            print(f"[INFO] JSON data saved to '{output_file}'")

    return json_data


if __name__ == "__main__":

    directory = r"D:\FileHistory\Jake\CHEESEMACHINE"
    #directory = r"F:\Semi-Bin\D"
    #directory = r"Z:\Jake\JAKE-E7450"
    #output_directory = f'./output_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    output_directory = r"D:\CHEESEMACHINE"
    dry_run = False
    save_json = True
    directories_to_skip = []
    has_data_directory = True
    
    # directories_to_skip = [
    #    ".vscode",
    # ]

    folder_info = main(directory, directories_to_skip, save_json, has_data_directory)

    files_copied, copied_size, files_skipped, skipped_size = copy_and_rename_files(
        folder_info, output_directory, dry_run
    )

    print(
        f"Total files processed: {folder_info['total_count']} (Total size: {folder_info['total_size'] / (1024 ** 3):.2f} GB)"
    )
    print(
        f"Files to keep: {folder_info['keep_count']} (Total size: {folder_info['keep_size'] / (1024 ** 3):.2f} GB)"
    )
    print(
        f"Files to delete: {folder_info['delete_count']} (Total size: {folder_info['delete_size'] / (1024 ** 3):.2f} GB)"
    )

    print(
        f"Files copied: {files_copied} (Total size: {copied_size / (1024 ** 3):.2f} GB)"
    )
    print(
        f"Files skipped: {files_skipped} (Total size: {skipped_size / (1024 ** 3):.2f} GB)"
    )
