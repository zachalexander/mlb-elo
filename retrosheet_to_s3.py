# -*- coding: utf-8 -*-
import os
import requests
import boto3
from bs4 import BeautifulSoup
from botocore.exceptions import NoCredentialsError, ClientError

# S3 bucket settings
s3_bucket = "mlb-game-log-data-retrosheet"
s3_prefix = "raw/"
local_dir = "retrosheet_gamelogs"
os.makedirs(local_dir, exist_ok=True)

# Set up S3 client
try:
    s3 = boto3.client("s3")
    s3.list_buckets()  # confirm credentials work
except NoCredentialsError:
    print("AWS credentials not found. Run 'aws configure'.")
    exit()
except ClientError as e:
    print("AWS error: {}".format(e))
    exit()

# Retrosheet game logs base URL
base_url = "https://www.retrosheet.org/gamelogs/index.html"
print("Connecting to Retrosheet...")

try:
    resp = requests.get(base_url, timeout=10)
    resp.raise_for_status()
except requests.RequestException as e:
    print("Failed to connect to Retrosheet: {}".format(e))
    exit()

soup = BeautifulSoup(resp.text, "html.parser")

links = []
for a_tag in soup.find_all("a"):
    href = a_tag.get("href")
    if (
        href
        and "gl" in href.lower()
        and (href.endswith(".txt") or href.endswith(".zip"))
    ):
        links.append(href)


if not links:
    print("No game log files found on Retrosheet.")
    exit()

print("Found {} game log files.".format(len(links)))

# Download and upload files
for filename in links:
    file_url = filename
    local_path = os.path.join(local_dir, file_url)
    s3_key = "{}{}".format(s3_prefix, filename)

    print("\nDownloading: {}".format(filename))
    try:
        r = requests.get(file_url, stream=True, timeout=20)
        r.raise_for_status()

        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Saved to {}".format(local_path))
    except requests.RequestException as e:
        print("Download failed: {}".format(e))
        continue

    print("Uploading to s3://{}/{}".format(s3_bucket, s3_key))
    try:
        s3.upload_file(local_path, s3_bucket, s3_key)
        print("Upload successful.")
    except ClientError as e:
        print("Upload failed: {}".format(e))
