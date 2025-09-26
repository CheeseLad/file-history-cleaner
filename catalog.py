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
    try:
        with open(file_csv_path, mode="r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                file_map[row["id"]] = row
                file_map[row["childId"]] = row
    except FileNotFoundError:
        logger.warning(f"file.csv not found at {file_csv_path}")
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
        #logger.info(f"Checking: {name}")
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

    #with open(output_file, "w", encoding="utf-8") as f:
    #    json.dump(sorted_folder_info, f, indent=2)

    #logger.info(f"Saved info for {len(sorted_folder_info)} folders to {output_file}")
    logger.info(f"Found {len(sorted_folder_info)} folders with files.")
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


def load_namespace_map(namespace_csv_path):
    """Load namespace data to help find missing path information"""
    namespace_map = {}
    id_to_child_map = {}
    try:
        with open(namespace_csv_path, mode="r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Store by childId since that's what we need to look up
                namespace_map[row["childId"]] = row
                # Also create a mapping from id to childId
                id_to_child_map[row["id"]] = row["childId"]
    except FileNotFoundError:
        logger.warning(f"namespace.csv not found at {namespace_csv_path}")
    return namespace_map, id_to_child_map


def find_path_from_namespace(file_id, namespace_map, string_map, id_to_child_map, file_map=None):
    """Try to find path information using namespace.csv, then file.csv as fallback with recursive parent traversal"""
    # First check if file_id is a childId
    if file_id in namespace_map:
        namespace_entry = namespace_map[file_id]
        parent_id = namespace_entry.get("parentId")
        # Try to get the path from the parent's string entry
        if parent_id and parent_id in string_map:
            return string_map[parent_id]
        # If parent not found in string_map, try to find parent's parent recursively
        if parent_id and parent_id in namespace_map:
            return find_path_from_namespace(parent_id, namespace_map, string_map, id_to_child_map, file_map)
    # Check if file_id corresponds to an id field (which maps to a childId)
    elif file_id in id_to_child_map:
        child_id = id_to_child_map[file_id]
        return find_path_from_namespace(child_id, namespace_map, string_map, id_to_child_map, file_map)
    # Fallback: check file.csv for parentId and try string_map with recursive traversal
    elif file_map and file_id in file_map:
        file_entry = file_map[file_id]
        parent_id = file_entry.get("parentId")
        if parent_id and parent_id in string_map:
            return string_map[parent_id]
        # If parent not found in string_map, try to find parent's parent recursively in file.csv
        if parent_id and parent_id in file_map:
            return find_path_from_namespace(parent_id, namespace_map, string_map, id_to_child_map, file_map)
    return None


def sanitize_path(path):
    """Remove null characters and other invalid characters from file paths"""
    if path is None:
        return None
    
    # Remove null characters (\0)
    path = path.replace('\0', '')
    
    # Remove other invalid characters for Windows file systems
    invalid_chars = '<>"|?*'
    for char in invalid_chars:
        path = path.replace(char, '_')
    
    # Remove leading/trailing whitespace and dots
    path = path.strip(' .')
    
    # Replace multiple consecutive slashes with single slash
    while '//' in path:
        path = path.replace('//', '/')
    
    return path





def copy_and_rename_files(folder_info, source_root, output_root, dry_run, namespace_csv_path=None, string_map=None, file_map=None):
    logger.info(f"Starting file copy process. Total folders: {len(folder_info)}")
    bad_paths = []
    total_files = sum(len(folder["files"]) for folder in folder_info.values())
    
    # Load namespace data if available
    namespace_map = {}
    id_to_child_map = {}
    namespace_found_count = 0
    if namespace_csv_path:
        namespace_map, id_to_child_map = load_namespace_map(namespace_csv_path)
        logger.info(f"Loaded {len(namespace_map)} namespace entries")
    
    # Create progress bar
    pbar = tqdm(total=total_files, desc="Copying files", unit="file")
    
    for folder_id, folder in folder_info.items():
        for file_entry in folder["files"]:
            src_folder = os.path.join(source_root, folder_id)
            src_file = os.path.join(src_folder, file_entry["name"])
            rel_path = file_entry["path"]
            
            # Skip files with no path information
            if rel_path is None:
                # Try to find path using namespace.csv or file.csv fallback
                if namespace_map and string_map:
                    file_id = file_entry["id"]
                    rel_path = find_path_from_namespace(file_id, namespace_map, string_map, id_to_child_map, file_map)
                    if rel_path:
                        namespace_found_count += 1
                        #logger.info(f"Found path for file {file_entry['name']} using namespace.csv or file.csv: {rel_path}")
                        pass
                    else:
                        logger.warning(f"Skipping file {file_entry['name']} in folder {folder_id}: no path information found in namespace.csv or file.csv")
                        bad_paths.append(
                            {
                                "src": src_file,
                                "reason": "no path information found in namespace.csv or file.csv",
                            }
                        )
                            
                        pbar.update(1)
                        continue
                else:
                    logger.warning(f"Skipping file {file_entry['name']} in folder {folder_id}: no path information")
                    bad_paths.append(
                        {
                            "src": src_file,
                            "reason": "no path information",
                        }
                    )
                    pbar.update(1)
                    continue
                
            # Sanitize the path to remove null characters and other invalid characters
            rel_path = sanitize_path(rel_path)
            if rel_path is None or rel_path.strip() == "":
                logger.warning(f"Skipping file {file_entry['name']} in folder {folder_id}: path is empty after sanitization")
                bad_paths.append(
                    {
                        "src": src_file,
                        "reason": "path is empty after sanitization",
                    }
                )
                pbar.update(1)
                continue
            
            # Remove colon from drive letter and replace backslashes with slashes, but keep original case
            rel_path_norm = (
                rel_path.replace(":", "").replace("\\", "/").replace("\\", "/")
            )
            rel_path_parts = [p for p in rel_path_norm.split("/") if p]
            
            # Validate path parts - skip if any part is too long or invalid
            if any(len(part) > 255 for part in rel_path_parts):
                logger.warning(f"Skipping file {file_entry['name']} in folder {folder_id}: path contains component longer than 255 characters")
                bad_paths.append(
                    {
                        "src": src_file,
                        "reason": "path contains component longer than 255 characters",
                    }
                )
                pbar.update(1)
                continue
                
            dest_dir = os.path.join(output_root, *rel_path_parts)
            
            # Check for path length issues (Windows default MAX_PATH is 260)
            # Check both directory length and estimated final file path length
            estimated_final_path = os.path.join(dest_dir, file_entry["string"] if file_entry["string"] else file_entry["name"])
            if os.name == "nt" and (len(dest_dir) > 240 or len(estimated_final_path) > 255):
                # Log and skip files with paths that are too long
                logger.warning(f"Skipping {src_file}: path too long (length: {len(estimated_final_path)})")
                bad_paths.append(
                    {"src": src_file, "dest": dest_file, "reason": f"path too long (length: {len(estimated_final_path)})"}
                )
                pbar.update(1)
                continue
            
            try:
                if not dry_run:
                    os.makedirs(dest_dir, exist_ok=True)
            except OSError as e:
                logger.warning(f"Failed to create directory {dest_dir}: {e}")
                bad_paths.append(
                    {
                        "src": src_file,
                        "dest": dest_dir,
                        "reason": f"Failed to create directory: {e}",
                    }
                )
                pbar.update(1)
                continue
            except Exception as e:
                logger.error(f"Unexpected error creating directory {dest_dir}: {e}")
                bad_paths.append(
                    {
                        "src": src_file,
                        "dest": dest_dir,
                        "reason": f"Unexpected error creating directory: {e}",
                    }
                )
                pbar.update(1)
                continue
            new_name = (
                file_entry["string"] if file_entry["string"] else file_entry["name"]
            )
            # Sanitize the filename as well
            new_name = sanitize_path(new_name)
            if new_name is None or new_name.strip() == "":
                logger.warning(f"Skipping file {file_entry['name']} in folder {folder_id}: filename is empty after sanitization")
                bad_paths.append(
                    {
                        "src": src_file,
                        "reason": "filename is empty after sanitization",
                    }
                )
                pbar.update(1)
                continue
                
            dest_file = os.path.join(dest_dir, new_name)
            # Check for final path length issues (this should rarely happen now with simplified paths)
            if os.name == "nt" and len(dest_file) > 255:
                logger.warning(f"Skipping {dest_file}: final path still too long even with simplified path")
                bad_paths.append(
                    {"src": src_file, "dest": dest_file, "reason": "final path still too long"}
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
            except PermissionError as e:
                logger.warning(f"Permission denied copying {src_file} to {dest_file}: {e}")
                bad_paths.append({"src": src_file, "dest": dest_file, "reason": f"Permission denied: {e}"})
            except Exception as e:
                logger.error(f"Failed to copy {src_file} to {dest_file}: {e}")
                bad_paths.append({"src": src_file, "dest": dest_file, "reason": str(e)})
                
            # Update progress bar
            pbar.update(1)
    
    # Close progress bar
    pbar.close()
    
    # Log namespace lookup statistics
    if namespace_map:
        logger.info(f"Successfully found paths for {namespace_found_count} files using namespace.csv")
    
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
    #catalog_dir = r".\catalog_data"
    catalog_dir = r".\ps_CHEESEMACHINE"
    #catalog_dir = r".\ps_JAKE-E7450"  # Change this to your catalog directory
    filepath = os.path.join(catalog_dir, "file.csv")
    string_csv_path = os.path.join(catalog_dir, "string.csv")
    namespace_csv_path = os.path.join(catalog_dir, "namespace.csv")
    #directory = r"E:\JAKE-E7450"  # Change this to your directory
    directory = r"D:\FileHistory\Jake\CHEESEMACHINE"
    of_directory = os.path.join(directory, "Data", "$OF")
    edb_path = os.path.join(directory, "Configuration", "Catalog1.edb")
    output_dir = f'./output_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    tables = ["file", "string", "namespace"]
    dry_run = False
    
    logger.info(f"Processing directory: {directory}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Dry run mode: {dry_run}")
    
    #for table in tables:
    #    logger.info(f"Exporting table: {table}")
    #     export_table_to_csv(edb_path, table, os.path.join(catalog_dir, f"{table}.csv"))

    sorted_folder_info = list_folders_with_files_and_strings(
        of_directory, string_csv_path, filepath
    )
    id_index, parent_index = parse_csv(filepath)

    for folder, info in sorted_folder_info.items():
        #logger.info(f"\nFolder: {folder}, File Count: {info['count']}, Files:")
        #for file_entry in info["files"]:
        #    logger.debug(f"  {file_entry}")
        target_id = clean_id(folder)

        if target_id in id_index:
            pass

            #children = parent_index.get(target_id, [])
        else:
            logger.warning(f"ID {target_id} not found.")

    if output_dir:
        logger.info("Starting file copy process...")
        # Load string map for namespace lookup
        string_map = load_string_map(string_csv_path)
        file_map = load_file_map(filepath)
        copy_and_rename_files(sorted_folder_info, of_directory, output_dir, dry_run, namespace_csv_path, string_map, file_map)
    
    logger.info("Catalog processing completed.")


if __name__ == "__main__":
    main()
