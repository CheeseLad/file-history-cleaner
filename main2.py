import re
import os
import sys
import sqlite3

def extract_file_info(filename):
    # Regular expression pattern to match the file components
    pattern = r'^(.*?)(\s*\(.*\))?\s*\((.*?)\sUTC\)\.(\w+)$'
    
    match = re.match(pattern, filename)
    
    if match:
        file_path = match.group(0)
        file_name = (match.group(1) + (match.group(2) or "")).strip()
        date_string = match.group(3).strip()
        extension = match.group(4)
        
        return file_path, file_name, date_string, extension
    else:
        return None
    

def count_files(directory):
    file_count = 0
    for _, _, files in os.walk(directory):
        file_count += len(files)
    return file_count
    
def print_files(directory):
    cnt = 0
    unprocessed_files = []
    total_file_count = count_files(directory)
    print(f"Total files to process: {total_file_count}")

    for root, _, files in os.walk(directory):

        for file in files:
            cnt += 1
            print(f"Processing file {cnt} of {total_file_count} ({(cnt / total_file_count) * 100:.2f}%)")
            file_info = extract_file_info(file)
            if file_info:
                db.execute("INSERT INTO files (folder, file_path, file_name, date_string, extension, duplicate_count, keep) VALUES (?, ?, ?, ?, ?, ?, ?)", (root, file_info[0], file_info[1], file_info[2], file_info[3], 0, True))
            else:
                unprocessed_files.append(file)

    return unprocessed_files

start_directory = sys.argv[1]

db = sqlite3.connect("files2.db")
db.execute("DROP TABLE IF EXISTS files;")
db.execute("CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY, folder TEXT, file_path TEXT, file_name TEXT, date_string TEXT, extension TEXT, duplicate_count INT, keep BOOLEAN);")
unprocessed = print_files(start_directory)
db.execute("""WITH duplicate_counts AS (
    SELECT 
        folder, 
        file_name, 
        extension, 
        COUNT(*) AS count
    FROM 
        files
    GROUP BY 
        folder, file_name, extension
    HAVING 
        COUNT(*) > 1
)
UPDATE files
SET duplicate_count = (
    SELECT count
    FROM duplicate_counts
    WHERE 
        duplicate_counts.folder = files.folder
        AND duplicate_counts.file_name = files.file_name
        AND duplicate_counts.extension = files.extension
)
WHERE EXISTS (
    SELECT 1
    FROM duplicate_counts
    WHERE 
        duplicate_counts.folder = files.folder
        AND duplicate_counts.file_name = files.file_name
        AND duplicate_counts.extension = files.extension
);""")
db.execute("""WITH latest_dates AS (
    SELECT 
        id,
        folder,
        file_name,
        extension,
        date_string,
        ROW_NUMBER() OVER (
            PARTITION BY folder, file_name, extension
            ORDER BY 
                CAST(SUBSTR(date_string, 1, 4) AS INTEGER) DESC,
                CAST(SUBSTR(date_string, 6, 2) AS INTEGER) DESC,
                CAST(SUBSTR(date_string, 9, 2) AS INTEGER) DESC,
                CAST(SUBSTR(date_string, 12, 2) AS INTEGER) DESC,
                CAST(SUBSTR(date_string, 15, 2) AS INTEGER) DESC,
                CAST(SUBSTR(date_string, 18, 2) AS INTEGER) DESC
        ) AS rn
    FROM 
        files
    WHERE EXISTS (
        SELECT 1
        FROM files f2
        WHERE 
            f2.folder = files.folder
            AND f2.file_name = files.file_name
            AND f2.extension = files.extension
        GROUP BY f2.folder, f2.file_name, f2.extension
        HAVING COUNT(*) > 1
    )
)
UPDATE files
SET keep = CASE 
    WHEN id IN (SELECT id FROM latest_dates WHERE rn = 1) THEN TRUE
    WHEN id IN (SELECT id FROM latest_dates WHERE rn > 1) THEN FALSE
    ELSE NULL
END
WHERE id IN (SELECT id FROM latest_dates);""")
db.commit()
print("Done")
for file in unprocessed:
    print(f"Could not process file: {file}")
db.close()