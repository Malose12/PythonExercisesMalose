import os
import zipfile
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
import requests

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    # "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

DOWNLOAD_DIR = 'Downloads'

def create_download_dir():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)


def get_filename_from_uri(uri):
    return uri.split("/")[-1]

def download_file(uri):
    try:
        filename = get_filename_from_uri(uri)
        filepath = os.path.join(DOWNLOAD_DIR,filename)
        response = requests.get(uri,stream=True)
        response.raise_for_status()
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=81920):
                f.write(chunk)
            return filepath
    except requests.RequestException as e:
        print(f"Failed to download {uri}: {e}")
        return None
    
def extract_and_delete_zip(filepath):
    try:
        with zipfile.ZipFile(filepath, "r") as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)
        os.remove(filepath)
    except zipfile.BadZipFile:
        print(f"Invalid ZIP File: {filepath}")

async def download_file_async(uri,session):
    try:
        filename = get_filename_from_uri(uri)
        filepath = os.path.join(DOWNLOAD_DIR,filename)
        async with session.get(uri) as response:
            response.raise_for_status()
            with open(filepath, "wb") as f:
                while True:
                    chunk = await response.content.read(8192)
                    if not  chunk:
                        break
                    f.write(chunk)
        return filepath
    except aiohttp.ClientError as e:
        print(f"Failed to download {uri}: {e}")
        return None

async def async_download_all(uris):
    async with aiohttp.ClientSession() as session:
        tasks = [download_file_async(uri,session) for uri in uris]
        filepaths = await asyncio.gather(*tasks)
        return [fp for fp in filepaths if fp]
    
def thread_download_all(uris):
    with ThreadPoolExecutor() as executor:
        filepaths = list(executor.map(download_file,uris))
        return [fp for fp in filepaths if fp]


def main():
    # your code here
    create_download_dir()

    # print("Downloading files sychronpusly...")
    # filepaths = [download_file(uri) for uri in download_uris if uri]
    
    # Asynchronous download (uncomment to use)
    print("Downloading files asynchronously...")
    filepaths = asyncio.run(async_download_all(download_uris))
    
    # Threaded download (uncomment to use)
    # print("Downloading files with ThreadPoolExecutor...")
    # filepaths = threaded_download_all(download_uris)
    for filepath in filepaths:
        if filepath:
            extract_and_delete_zip(filepath)
    print("Donwload and extraction complete.")
    # pass


if __name__ == "__main__":
    main()
