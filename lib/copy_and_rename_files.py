import os
import sys
import shutil
import json

def copy_and_rename_files(json_data, output_root, dry_run=True):
    logs = []
    files_copied = 0
    copied_size = 0
    files_skipped = 0
    skipped_size = 0
    if not os.path.exists(output_root):
        print(f"Output directory does not exist: {output_root}. Creating it now...")
        if not dry_run:
            try:
                os.makedirs(output_root)
            except Exception as e:
                print(f"Failed to create output directory {output_root}: {e}")
                sys.exit(1)
        

    for base_id, file_group in json_data["files"].items():
        for version_key, file_entry in file_group["versions"].items():
            src_file = file_entry["src_path"]

            # Normalize dst_path
            dst_path = file_entry["dst_path"].replace(":", "")
            dst_parts = dst_path.replace("\\", "/").split("/")
            dst_parts = [p for p in dst_parts if p]
            
            to_delete = file_entry.get("to_delete", False)
            if to_delete:
                print(f"[SKIP] Marked for deletion: {src_file}")
                files_skipped += 1
                skipped_size += file_entry["size"]
                continue

            if not dst_parts:
                print(f"Error: Invalid dst_path: {file_entry['dst_path']}")
                sys.exit(1)

            dest_dir = os.path.join(output_root, *dst_parts[:-1])
            dest_name = dst_parts[-1]
            new_name = file_entry.get("string") or dest_name
            dest_file = os.path.join(dest_dir, new_name)
            
            if dry_run:
                print(f"[DRY RUN] Would copy: {src_file} → {dest_file}")
                files_copied += 1
                copied_size += file_entry["size"]
                continue
            
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
                #print(f"Copied {src_file} -> {dest_file}")
                files_copied += 1
                copied_size += file_entry["size"]
            except Exception as e:
                print(f"Failed to copy {src_file} to {dest_file}: {e}")
                sys.exit(1)
                
    return files_copied, copied_size, files_skipped, skipped_size