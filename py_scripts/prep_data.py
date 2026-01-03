import pandas as pd

CANONICAL_COLS = [
    "started_at",
    "ended_at",
    "start_station_id",
    "start_station_name",
    "end_station_id",
    "end_station_name",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "rideable_type",
    "member_casual",
]

MAP_OLD = {
    "Start date": "started_at",
    "End date": "ended_at",
    "Start station number": "start_station_id",
    "Start station": "start_station_name",
    "End station number": "end_station_id",
    "End station": "end_station_name",
    "Member type": "member_casual",
}

MAP_NEW = {c: c for c in CANONICAL_COLS}

daily_weather_columns = [
    "time",
    "weather_code",
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_mean",
    "apparent_temperature_max",
    "apparent_temperature_min",
    "sunrise",
    "sunset",
    "daylight_duration",
    "sunshine_duration",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "wind_direction_10m_dominant",
    "wind_gusts_10m_mean",
    "wind_speed_10m_mean",
    "wind_gusts_10m_min",
    "wind_speed_10m_min",
    "winddirection_10m_dominant",
    "dew_point_2m_mean",
    "cloud_cover_mean",
    "cloud_cover_max",
    "cloud_cover_min",
    "relative_humidity_2m_mean",
    "relative_humidity_2m_max",
    "relative_humidity_2m_min",
    "pressure_msl_mean",
    "surface_pressure_mean"
]

hourly_weather_columns = [
    "time",
    "temperature_2m",
    "weather_code",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_direction_100m",
    "wind_gusts_10m",
    "wind_speed_100m",
    "is_day",
    "dew_point_2m",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "sunshine_duration"
]

def normalize_bikeshare_df(df: pd.DataFrame) -> pd.DataFrame:
    # rename to canonical
    if "Start date" in df.columns:
        df = df.rename(columns=MAP_OLD)
    else:
        df = df.rename(columns=MAP_NEW)

    # enforce column order
    df = df.reindex(columns=CANONICAL_COLS)

    # timestamps
    df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce")
    df["ended_at"] = pd.to_datetime(df["ended_at"], errors="coerce")

    # ids
    for col in ("start_station_id", "end_station_id"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # coordinates
    for col in ("start_lat", "start_lng", "end_lat", "end_lng"):
        df[col] = pd.to_numeric(df[col], errors="coerce").round(6)

    # strings
    for col in (
        "start_station_name",
        "end_station_name",
        "rideable_type",
        "member_casual",
    ):
        df[col] = df[col].astype("string")
    
    # Noticed some 'Member' and 'member' entries thus, normalize text fields: trim whitespace and lowercase. 
    for col in ("rideable_type", "member_casual"):
        df[col] = df[col].str.strip().str.lower()

    return df
