import zipfile
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from io import BytesIO
from tqdm import tqdm
import sys
import pandas as pd
import requests

def get_bikeshare_data(root_dir):
    """Stream bikeshare data into local csv files without downloading the individual zip files"""

    BUCKET_NAME = "capitalbikeshare-data"
    CSV_DIR = root_dir / "bikeshare_csv"

    # Create the directory where the csv files will be kept after streaming and extracting the zip files in the s3 bucket.
    if CSV_DIR.exists():
        sys.exit(f"Directory {CSV_DIR} exists.\nExiting to avoid duplication.\nManually clear directory and re-run the script if needed.")
    else:
        CSV_DIR.mkdir()
        print(f"Created {CSV_DIR}")

    # Initiate an s3 client to get data
    s3 = boto3.client(
        "s3",
        config=Config(signature_version=UNSIGNED)
    )

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET_NAME):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.lower().endswith(".zip"):
                continue

            size = obj["Size"]
            print(f"Streaming + extracting {key}")

            response = s3.get_object(Bucket=BUCKET_NAME, Key=key)

            # zip files are streamed into memory instead of downloaded into storage with progress bars.
            buffer = BytesIO()
            with tqdm(
                total=size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=key,
            ) as pbar:
                for chunk in response["Body"].iter_chunks(1024 * 1024):
                    buffer.write(chunk)
                    pbar.update(len(chunk))

            buffer.seek(0)

            # skipping all except the fist level csv files in the zipped folder
            with zipfile.ZipFile(buffer) as z:
                members = [
                    m for m in z.namelist()
                    if m.lower().endswith(".csv")
                    and not m.startswith("__MACOSX/")
                    and "/" not in m.strip("/")
                ]

                # Extracting and writing into csv files, extra logic to avoid file name collisions, which do exist based on inital manual investigation
                for member in tqdm(members, desc="Extracting CSVs", leave=False):
                    base = CSV_DIR / member

                    if base.exists():
                        stem = base.stem
                        suffix = base.suffix
                        i = 1
                        while True:
                            target = CSV_DIR / f"{stem}_{i}{suffix}"
                            if not target.exists():
                                break
                            i += 1
                    else:
                        target = base

                    with z.open(member) as src, open(target, "wb") as dst:
                        dst.write(src.read())

def get_weather_data():
    """Fetch weather data and return daily and hourly dataframes."""

    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        "?latitude=38.8951&longitude=-77.0364"
        "&start_date=2010-10-20&end_date=2025-11-30"
        "&daily=weather_code,temperature_2m_mean,temperature_2m_max,temperature_2m_min,"
        "apparent_temperature_mean,apparent_temperature_max,apparent_temperature_min,"
        "sunrise,sunset,daylight_duration,sunshine_duration,precipitation_sum,rain_sum,"
        "snowfall_sum,precipitation_hours,wind_speed_10m_max,wind_gusts_10m_max,"
        "wind_direction_10m_dominant,wind_gusts_10m_mean,wind_speed_10m_mean,"
        "wind_gusts_10m_min,wind_speed_10m_min,winddirection_10m_dominant,"
        "dew_point_2m_mean,cloud_cover_mean,cloud_cover_max,cloud_cover_min,"
        "relative_humidity_2m_mean,relative_humidity_2m_max,relative_humidity_2m_min,"
        "pressure_msl_mean,surface_pressure_mean"
        "&hourly=temperature_2m,weather_code,relative_humidity_2m,apparent_temperature,"
        "precipitation,rain,snowfall,snow_depth,pressure_msl,surface_pressure,cloud_cover,"
        "wind_speed_10m,wind_direction_10m,wind_direction_100m,wind_gusts_10m,"
        "wind_speed_100m,is_day,dew_point_2m,cloud_cover_low,cloud_cover_mid,"
        "cloud_cover_high,sunshine_duration"
        "&timezone=America%2FNew_York"
    )

    print("Fetching weather data from Open-Meteo API...")

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    data = resp.json()

    daily_weather = pd.DataFrame(data["daily"])
    hourly_weather = pd.DataFrame(data["hourly"])

    print("SUCCESS: Historical weather data fetched.")
    return (daily_weather, hourly_weather)
