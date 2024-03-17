#!/usr/bin/env python3

import os
import sys
import re

file_dict = {}
time_format = " UTC"
duplicate_files = []

try:
    directory = sys.argv[1]
except IndexError:
    directory = "."

def date_extractor(file):
    pattern = r'\((.*?)\)'
    matches = re.findall(pattern, file)
    try:
      date_details = matches[0]
    except IndexError:
        print(f"Date not found in {file}")
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
        print(f"Filename not found in {file}")
        return

def rename_file(source, destination):
    try:
        # Check if the source file exists
        if os.path.exists(source):
            # Check if the destination file already exists
            if not os.path.exists(destination):
                # Rename the file
                os.rename(source, destination)
                print(f"File '{source}' renamed to '{destination}' successfully.")
            else:
                print(f"Destination file '{destination}' already exists.")
        else:
            print(f"Source file '{source}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

def list_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            date_info = date_extractor(file)
            if date_info:
                file_info = filename_extractor(file)
            if date_info:
                if file_info in file_dict:
                    #print(f"Duplicate file found: {file_info}")
                    duplicate_files.append(file)
                file_dict[file_info] = date_info

    print("Duplicate files: ", duplicate_files)
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file not in duplicate_files:
                #print(os.path.join(root, file))
                basename, extension = os.path.splitext(file)
                file_info2 = filename_extractor(file)
                #print(f"Renaming {file} to {file_info2 + extension}")
                rename_file(os.path.join(root, file), os.path.join(root, file_info2 + extension))
                #os.rename(os.path.join(directory, file), os.path.join(directory, file_info + extension))
            else:
                print(f"File {file} is a duplicate. Skipping.")
    
if __name__ == "__main__":
    list_files(directory)
