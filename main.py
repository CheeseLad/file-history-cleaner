#!/usr/bin/env python3

import os
import sys
import re

file_dict = {}
time_format = " UTC"

try:
    directory = sys.argv[1]
except IndexError:
    directory = "."

def date_extractor(file):
    pattern = r'\((.*?)\)'
    matches = re.findall(pattern, file)
    date_details = matches[0]
    date_details = date_details.replace(time_format, "", 1)
    return date_details

def filename_extractor(file):
    pattern = r'^(.*?)\s*\((.*?)\)'
    filename = re.search(pattern, file)
    return filename.group(1)

def list_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            date_info = date_extractor(file)
            file_info = filename_extractor(file)
            if date_info:
                if file_info in file_dict:
                    print(f"Duplicate file found: {file_info}")
                file_dict[file_info] = date_info
    
    
    #for key, value in file_dict.items():
        #print(f"{key} : {value}")



if __name__ == "__main__":
    list_files(directory)
