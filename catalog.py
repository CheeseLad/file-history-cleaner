import csv
from collections import defaultdict
import os
import json
import shutil
from lib.edb_extractor import export_table_to_csv
import sys
from datetime import datetime
import logging
from tqdm import tqdm

# Configure logging
def setup_logging():
    """Configure logging to output to both console and file"""
    # Create logger
    logger = logging.getLogger('catalog')
    logger.setLevel(logging.DEBUG)
    
    # Create formatters
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create file handler - use 'w' mode to clear the file on each run
    file_handler = logging.FileHandler('catalog.log', mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)  # Only show warnings and errors on console
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Initialize logger
logger = setup_logging()

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
    
    # Get list of directories to process
    directories = [name for name in os.listdir(directory_path) 
                  if os.path.isdir(os.path.join(directory_path, name))]
    
    # Create progress bar for folder checking
    pbar = tqdm(total=len(directories), desc="Checking folders", unit="folder")

    for name in directories:
        folder_path = os.path.join(directory_path, name)
        logger.info(f"Checking: {name}")
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
            
            # Update progress bar
            pbar.update(1)

    # Close progress bar
    pbar.close()

    # Sort by numeric folder names
    sorted_folder_info = dict(sorted(folder_info.items(), key=lambda x: int(x[0])))

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(sorted_folder_info, f, indent=2)

    logger.info(f"Saved info for {len(sorted_folder_info)} folders to {output_file}")
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


def copy_and_rename_files(folder_info, source_root, output_root, dry_run):
    logger.info(f"Starting file copy process. Total folders: {len(folder_info)}")
    bad_paths = []
    total_files = sum(len(folder["files"]) for folder in folder_info.values())
    
    # Create progress bar
    pbar = tqdm(total=total_files, desc="Copying files", unit="file")
    
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
                if not dry_run:
                    os.makedirs(dest_dir, exist_ok=True)
            except Exception as e:
                logger.error(f"Failed to create directory {dest_dir}: {e}")
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
                logger.warning(f"Skipping {dest_file}: path too long")
                bad_paths.append(
                    {"src": src_file, "dest": dest_file, "reason": "path too long"}
                )
                continue
            # Check if source file exists
            if not os.path.exists(src_file):
                logger.error(f"Source file does not exist: {src_file}")
                bad_paths.append(
                    {
                        "src": src_file,
                        "dest": dest_file,
                        "reason": "source file does not exist",
                    }
                )
                continue
            if os.path.exists(dest_file):
                logger.info(f"File already exists, overwriting: {dest_file}")
            try:
                if not dry_run:
                    shutil.copy2(src_file, dest_file)
                # logger.info(f"Copied {src_file} -> {dest_file}")
            except Exception as e:
                logger.error(f"Failed to copy {src_file} to {dest_file}: {e}")
                bad_paths.append({"src": src_file, "dest": dest_file, "reason": str(e)})
                
            # Update progress bar
            pbar.update(1)
    
    # Close progress bar
    pbar.close()
    logger.info(f"File copy process completed. Processed {total_files} files.")
    # Write bad paths to a file for review
    if bad_paths:
        logger.warning(f"Found {len(bad_paths)} problematic files. Writing to bad_paths.log")
        with open("bad_paths.log", "w", encoding="utf-8") as f:
            for entry in bad_paths:
                f.write(str(entry) + "\n")
    else:
        logger.info("No problematic files found.")


def clean_id(id_str):
    return id_str.strip(" \t\n\r'\"")


def main():
    logger.info("Starting catalog processing...")
    catalog_dir = r"."
    filepath = os.path.join(catalog_dir, "file.csv")
    string_csv_path = os.path.join(catalog_dir, "string.csv")
    #directory = r"Z:\Jake\JAKE-E7450"  # Change this
    directory = r"D:\FileHistory\Jake\CHEESEMACHINE"
    of_directory = os.path.join(directory, "Data", "$OF")
    edb_path = os.path.join(directory, "Configuration", "Catalog1.edb")
    output_dir = f'./output_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    tables = ["file", "string"]
    dry_run = True
    
    logger.info(f"Processing directory: {directory}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Dry run mode: {dry_run}")

    for table in tables:
        logger.info(f"Exporting table: {table}")
        export_table_to_csv(edb_path, table, os.path.join(catalog_dir, f"{table}.csv"))
        
    sorted_folder_info = list_folders_with_files_and_strings(
        of_directory, string_csv_path, filepath
    )
    id_index, parent_index = parse_csv(filepath)

    for folder, info in sorted_folder_info.items():
        logger.info(f"\nFolder: {folder}, File Count: {info['count']}, Files:")
        for file_entry in info["files"]:
            logger.debug(f"  {file_entry}")
        target_id = clean_id(folder)

        if target_id in id_index:

            children = parent_index.get(target_id, [])
        else:
            logger.warning(f"ID {target_id} not found.")

    if output_dir:
        logger.info("Starting file copy process...")
        copy_and_rename_files(sorted_folder_info, of_directory, output_dir, dry_run)
    
    logger.info("Catalog processing completed.")


if __name__ == "__main__":
    main()
