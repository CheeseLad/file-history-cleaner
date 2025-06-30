import csv
from collections import defaultdict
import os
import json
import shutil
from lib.edb_extractor import export_table_to_csv
import sys


def load_string_map(string_csv_path):
    string_map = {}
    with open(string_csv_path, mode="r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            string_map[row["id"]] = row["string"]
    return string_map


def load_file_map(file_csv_path):
    file_map = {}
    with open(file_csv_path, mode="r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            file_map[row["id"]] = row
    return file_map


def list_folders_with_files_and_strings(
    directory_path, string_csv_path, file_csv_path, output_file="folders.json"
):
    string_map = load_string_map(string_csv_path)
    file_map = load_file_map(file_csv_path)
    folder_info = {}

    for name in os.listdir(directory_path):
        folder_path = os.path.join(directory_path, name)
        print(f"Checking: {name}")  # Debug line
        if os.path.isdir(folder_path):
            files = [
                f
                for f in os.listdir(folder_path)
                if os.path.isfile(os.path.join(folder_path, f))
            ]
            file_entries = []

            # Use the folder id (name) to look up the path string directly
            folder_id = name
            path_string = string_map.get(folder_id)

            for f in files:
                # Extract file id (assume it's the first part of the filename, before the first space or parenthesis)
                file_id = f.split(" ", 1)[0].split("(", 1)[0].strip()
                string_value = string_map.get(file_id, None)
                file_entries.append(
                    {
                        "name": f,
                        "id": file_id,
                        "string": string_value,
                        "path": path_string,
                    }
                )
            folder_info[name] = {"files": file_entries, "count": len(files)}

    # Sort by numeric folder names
    sorted_folder_info = dict(sorted(folder_info.items(), key=lambda x: int(x[0])))

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(sorted_folder_info, f, indent=2)

    print(f"Saved info for {len(sorted_folder_info)} folders to {output_file}")
    return sorted_folder_info


def parse_csv(filepath):
    with open(filepath, mode="r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        data = [row for row in reader]

    # Build indexes
    id_index = {clean_id(row["id"]): row for row in data}
    parent_index = defaultdict(list)
    for row in data:
        parent_index[row["parentId"]].append(row)

    return id_index, parent_index


def copy_and_rename_files(folder_info, source_root, output_root):
    bad_paths = []
    for folder_id, folder in folder_info.items():
        for file_entry in folder["files"]:
            src_folder = os.path.join(source_root, folder_id)
            src_file = os.path.join(src_folder, file_entry["name"])
            rel_path = file_entry["path"]
            # Remove colon from drive letter and replace backslashes with slashes, but keep original case
            rel_path_norm = (
                rel_path.replace(":", "").replace("\\", "/").replace("\\", "/")
            )
            rel_path_parts = [p for p in rel_path_norm.split("/") if p]
            dest_dir = os.path.join(output_root, *rel_path_parts)
            try:
                os.makedirs(dest_dir, exist_ok=True)
            except Exception as e:
                print(f"Failed to create directory {dest_dir}: {e}")
                bad_paths.append(
                    {
                        "src": src_file,
                        "dest": dest_dir,
                        "reason": f"Failed to create directory: {e}",
                    }
                )
                continue
            new_name = (
                file_entry["string"] if file_entry["string"] else file_entry["name"]
            )
            dest_file = os.path.join(dest_dir, new_name)
            # Check for path length issues (Windows default MAX_PATH is 260)
            if os.name == "nt" and (len(dest_file) > 255 or len(dest_dir) > 240):
                print(f"Skipping {dest_file}: path too long")
                bad_paths.append(
                    {"src": src_file, "dest": dest_file, "reason": "path too long"}
                )
                continue
            # Check if source file exists
            if not os.path.exists(src_file):
                print(f"Source file does not exist: {src_file}")
                bad_paths.append(
                    {
                        "src": src_file,
                        "dest": dest_file,
                        "reason": "source file does not exist",
                    }
                )
                continue
            if os.path.exists(dest_file):
                print(f"File already exists, overwriting: {dest_file}")
            try:
                shutil.copy2(src_file, dest_file)
                print(f"Copied {src_file} -> {dest_file}")
            except Exception as e:
                print(f"Failed to copy {src_file} to {dest_file}: {e}")
                bad_paths.append({"src": src_file, "dest": dest_file, "reason": str(e)})
    # Write bad paths to a file for review
    if bad_paths:
        with open("bad_paths_2.txt", "a", encoding="utf-8") as f:
            for entry in bad_paths:
                f.write(str(entry) + "\n")


def clean_id(id_str):
    return id_str.strip(" \t\n\r'\"")


def main():
    catalog_dir = r"./catalog_py"
    filepath = os.path.join(catalog_dir, "file.csv")
    string_csv_path = os.path.join(catalog_dir, "string.csv")
    directory = r"D:\FileHistory\Jake\JAKE-E7450"  # Change this
    of_directory = os.path.join(directory, "Data", "$OF")
    edb_path = os.path.join(directory, "Configuration", "Catalog1.edb")
    output_dir = r"./output"
    tables = ["file", "string"]
    for table in tables:
        export_table_to_csv(edb_path, table, os.path.join(catalog_dir, f"{table}.csv"))
    sorted_folder_info = list_folders_with_files_and_strings(
        of_directory, string_csv_path, filepath
    )
    id_index, parent_index = parse_csv(filepath)

    for folder, info in sorted_folder_info.items():
        print(f"\nFolder: {folder}, File Count: {info['count']}, Files:")
        for file_entry in info["files"]:
            print(file_entry)
        target_id = clean_id(folder)
        # print("Checking folder ID:", repr(target_id))
        # print("All CSV IDs:", [repr(k) for k in id_index.keys()])

        if target_id in id_index:
            # print(f"Found target row: {id_index[target_id]}")

            children = parent_index.get(target_id, [])
            # print(f"Children of {target_id}:")
            for child in children:
                print(child)
        else:
            print(f"ID {target_id} not found.")

    if output_dir:
        copy_and_rename_files(sorted_folder_info, of_directory, output_dir)


if __name__ == "__main__":
    main()
