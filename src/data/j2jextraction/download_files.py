"""This script downloads the files from the given URL and saves them to a folder
"""

# Import libraries
import pandas as pd
import numpy as np
from rich import print
from rich.progress import track
from os import listdir, makedirs
from os.path import exists

import requests
import time
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

# Read the list of files to process
download_folder = "./data/j2j/raw/"
url_files = "https://lehd.ces.census.gov/data/j2j/latest_release/metro/j2jod/"
df = pd.read_html(url_files)[0]
df = df[df.Name == df.Name]
df = df[df.Name.str.contains("_sarhe_f_gb_ns_oslp_")]
file_list = df.Name.tolist()

# Create list of downloaded files
# if download_folder does not exist, create it
if exists(download_folder):
    list_files = listdir(download_folder)
else:
    makedirs(download_folder)
    list_files = []

# Filter files already downloaded
file_list = [file for file in file_list if file not in list_files]

# Create list of urls 
orig_list = [url_files + file for file in file_list]

# Create list of destinations
dest_list = [download_folder + file for file in file_list]

inputs = zip(orig_list, dest_list)

# Create a function to download the files
def download_url(args):
    t0 = time.time()
    url, fn = args[0], args[1]
    try:
        r = requests.get(url)
        with open(fn, 'wb') as f:
            f.write(r.content)
        return(url, time.time() - t0)
    except Exception as e:
        print('Exception in download_url():', e)

def download_parallel(args):
    cpus = cpu_count()
    results = ThreadPool(cpus - 1).imap_unordered(download_url, args)
    for result in results:
        print('url:', result[0], 'time (s):', np.round(result[1], 3))


print(f"Downloading {len(file_list)} files...")

# Download the files
download_parallel(inputs)