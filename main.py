#!/usr/bin/env python3

import os
import sys
import re
import sqlite3



file_dict = {}
all_files = {}
time_format = " UTC"
good_to_rename = []
duplicate_files = []
extension_list = {}

def date_extractor(file):
    pattern = r'\((.*?)\)'
    matches = re.findall(pattern, file)
    try:
      date_details = matches[0]
    except IndexError:
        #print(f"Date not found in {file}")
        return
    date_details = date_details.replace(time_format, "", 1)
    return date_details

def filename_extractor(file):
    pattern = r'^(.*?)\s*\((.*?)\)'
    filename = re.search(pattern, file)
    try:
        filename = filename.group(1)
        return filename
    except AttributeError:
        #print(f"Filename not found in {file}")
        return

def build_file_database(directory):
    
    file_count = 0
    processed_files = 0
    for root, dirs, files in os.walk(directory):
        file_count += len(files)
    for root, dirs, files in os.walk(directory):

        
        for file in files:
            #file_path = os.path.join(root, file)
            file_info_array = []
            date_info = date_extractor(file)
            if date_info:
                file_info = filename_extractor(file)
            if date_info:
                if file_info in all_files:
                    all_files[file_info].append(date_info)
                    basename, extension = os.path.splitext(file)
                    extension_list[basename] = extension
                else:
                    all_files[file_info] = [date_info]
                    basename, extension = os.path.splitext(file)
                    extension_list[basename] = extension
                if file_info in file_dict:
                    duplicate_files.append(file)
                file_dict[file_info] = date_info

                file_info2 = []
                file_info2.append(root)
                file_info2.append(file_info + extension)
                file_info2.append(file_info)
                file_info2.append(date_info)
                file_info2.append(extension)
                file_info2.append("N")

                file_info_array.append(file_info)

                processed_files += 1
                print(f"Processed {processed_files} of {file_count} files")

        for file_info in file_info_array:
            database.execute("INSERT INTO files (file_path, file_fullname, file_name, file_date, file_extension, remove_file) VALUES (?, ?, ?, ?, ?, ?)", (file_info2[0], file_info2[1], file_info2[2], file_info2[3], file_info2[4], file_info2[5]))
        
        database.commit()

def check_files_to_remove():
    cursor = database.execute("SELECT * FROM files WHERE remove_file = 'N'")
    files = cursor.fetchall()
    for file in files:
        file_selected = file[2]
        cursor2 = database.execute("SELECT * FROM files WHERE file_fullname = ? AND remove_file = 'N'", (file_selected,))
        dup_files = cursor2.fetchall()
        if len(dup_files) > 1:
            dates = []
            for dup_file in dup_files:
                dates.append(dup_file[4])
            max_date = max(dates)
            for dup_file in dup_files:
                if dup_file[4] != max_date:
                    print(f"Removing duplicate file: {dup_file[2]}")
                    database.execute("UPDATE files SET remove_file = 'Y' WHERE file_fullname = ? AND file_date <> ?", (dup_file[2], max_date))
                    database.commit()
        



   
    
if __name__ == "__main__":
    try:
        directory = sys.argv[1]
    except IndexError:
        directory = "."

    database = sqlite3.connect("files.db")
    database.execute("DROP TABLE IF EXISTS files;")
    database.execute("CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY, file_path TEXT, file_fullname TEXT, file_name TEXT, file_date TEXT, file_extension TEXT, remove_file TEXT);")
    
    build_file_database(directory)
    check_files_to_remove()
    """file_count = {}
    for file in all_files.keys():
        if len(all_files[file]) > 1:
            #print(f"File: {file}, Count: {all_files[file]}")
            # find max date
            max_date = max(all_files[file])
            #print(f"Max Date: {max_date}")
            all_files[file].remove(max_date)
            extension = extension_list[f"{file} ({max_date} UTC)"]
            print(f"File to keep:      {file} ({max_date} UTC){extension}")
            for date in all_files[file]:
                extension = extension_list[f"{file} ({date} UTC)"]
                print(f"File to remove:    {file} ({date} UTC){extension}")"""

        
