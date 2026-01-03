import os
import psycopg2
from io import StringIO
import pandas as pd

def get_conn(conn_info):
    """Create a psycopg2 connection to the db with autocommit enabled"""
    conn = psycopg2.connect(
        host=conn_info["host"],
        port=conn_info["port"],
        user="postgres",
        dbname=conn_info["dbname"],
        # TO-DO: Need to set the password for the postgres user as an PGPW environment variable
        password=os.environ["PGPW"], 
    )
    conn.autocommit = True
    return conn

def get_conn_analytics(conn_info):
    """Create a psycopg2 connection to the db with the analytics user."""
    conn = psycopg2.connect(
        host=conn_info["host"],
        port=conn_info["port"],
        user="rouser",
        dbname=conn_info["dbname"],
        # TO-DO: Need to set the password for the postgres user as an PGPW environment variable
        password=os.environ["ROUSRPW"], 
    )
    return conn

def create_db_tables(cur):
    """Create inital database tables"""
    # SQL statements
    create_rides_raw = """
    CREATE TABLE IF NOT EXISTS rides_raw (
        started_at           TIMESTAMP,
        ended_at             TIMESTAMP,
        start_station_id     INTEGER,
        start_station_name   TEXT,
        end_station_id       INTEGER,
        end_station_name     TEXT,
        start_lat            NUMERIC(9,6),
        start_lng            NUMERIC(9,6),
        end_lat              NUMERIC(9,6),
        end_lng              NUMERIC(9,6),
        rideable_type        TEXT,
        member_casual        TEXT
    );
    """

    create_daily_weather = """
    CREATE TABLE IF NOT EXISTS daily_weather (
        time DATE,
        weather_code INTEGER,
        temperature_2m_mean NUMERIC(4,1),
        temperature_2m_max  NUMERIC(4,1),
        temperature_2m_min  NUMERIC(4,1),
        apparent_temperature_mean NUMERIC(4,1),
        apparent_temperature_max  NUMERIC(4,1),
        apparent_temperature_min  NUMERIC(4,1),
        sunrise TIMESTAMP,
        sunset  TIMESTAMP,
        daylight_duration  NUMERIC(8,2),
        sunshine_duration NUMERIC(8,2),
        precipitation_sum NUMERIC(5,2),
        rain_sum          NUMERIC(5,2),
        snowfall_sum      NUMERIC(5,2),
        precipitation_hours NUMERIC(4,1),
        wind_speed_10m_max NUMERIC(4,1),
        wind_gusts_10m_max NUMERIC(4,1),
        wind_direction_10m_dominant INTEGER,
        wind_gusts_10m_mean NUMERIC(4,1),
        wind_speed_10m_mean NUMERIC(4,1),
        wind_gusts_10m_min  NUMERIC(4,1),
        wind_speed_10m_min  NUMERIC(4,1),
        winddirection_10m_dominant INTEGER,
        dew_point_2m_mean NUMERIC(4,1),
        cloud_cover_mean INTEGER,
        cloud_cover_max  INTEGER,
        cloud_cover_min  INTEGER,
        relative_humidity_2m_mean INTEGER,
        relative_humidity_2m_max  INTEGER,
        relative_humidity_2m_min  INTEGER,
        pressure_msl_mean     NUMERIC(6,1),
        surface_pressure_mean NUMERIC(6,1)
    );
    """

    create_hourly_weather = """
    CREATE TABLE IF NOT EXISTS hourly_weather (
        time TIMESTAMP,
        temperature_2m NUMERIC(4,1),
        weather_code INTEGER,
        relative_humidity_2m INTEGER,
        apparent_temperature NUMERIC(4,1),
        precipitation NUMERIC(5,2),
        rain          NUMERIC(5,2),
        snowfall      NUMERIC(5,2),
        snow_depth    NUMERIC(5,2),
        pressure_msl     NUMERIC(6,1),
        surface_pressure NUMERIC(6,1),
        cloud_cover INTEGER,
        wind_speed_10m NUMERIC(4,1),
        wind_direction_10m INTEGER,
        wind_direction_100m INTEGER,
        wind_gusts_10m NUMERIC(4,1),
        wind_speed_100m NUMERIC(4,1),
        is_day INTEGER,
        dew_point_2m NUMERIC(4,1),
        cloud_cover_low INTEGER,
        cloud_cover_mid INTEGER,
        cloud_cover_high INTEGER,
        sunshine_duration NUMERIC(6,1)
    );
    """

    cur.execute(create_rides_raw)
    cur.execute(create_daily_weather)
    cur.execute(create_hourly_weather)

    print("Tables created successfully.")

def is_table_populated(cur, table_name: str) -> bool:
    """Check if a table exists and has at least one row."""
    # Check if table exists
    cur.execute(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
        """,
        (table_name,)
    )
    table_exists = cur.fetchone()[0]
    
    if not table_exists:
        print(f"Table {table_name} does not exist.")
        return False
    
    # Check if table has any rows
    cur.execute(f'SELECT COUNT(*) FROM "{table_name}";')
    row_count = cur.fetchone()[0]
    
    return row_count > 0

def copy_df_bikeshare(cur, df: pd.DataFrame):
    """Copy the content of a data frame into the database using StringIO"""

    buf = StringIO()

    print("Reading data frame into buffer...")
    df.to_csv(buf, index=False, header=True)
    buf.seek(0)

    print("Sending to database...")
    cur.copy_expert(
        """
        COPY rides_raw (
            started_at,
            ended_at,
            start_station_id,
            start_station_name,
            end_station_id,
            end_station_name,
            start_lat,
            start_lng,
            end_lat,
            end_lng,
            rideable_type,
            member_casual
        )
        FROM STDIN
        WITH (FORMAT CSV, HEADER TRUE)
        """,
        buf,
    )

def copy_df_weather(cur, df: pd.DataFrame, table: str, columns: list[str]):
    """
    Stream a weather DataFrame directly into PostgreSQL using COPY FROM STDIN.

    Assumes:
      - df comes directly from get_weather_data()
      - target table already exists
      - column names and types are compatible
    """

    # Enforce column presence + order (NO type normalization)
    df = df.reindex(columns=columns)

    buf = StringIO()

    print(f"Streaming data into buffer for table '{table}'...")
    df.to_csv(
        buf,
        index=False,
        header=True,
        na_rep="",   # empty fields -> NULL in Postgres
    )
    buf.seek(0)

    print(f"Copying data into '{table}'...")
    cur.copy_expert(
        f"""
        COPY {table} (
            {", ".join(columns)}
        )
        FROM STDIN
        WITH (FORMAT CSV, HEADER TRUE)
        """,
        buf,
    )

def create_rouser(cur):
    """Create a read-only analytics role with limited connections and SELECT-only privileges on all current and future tables in the public schema."""

    rousrpwd = os.environ["ROUSRPW"]

    sql = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_roles WHERE rolname = 'rouser'
        ) THEN
            CREATE ROLE rouser WITH LOGIN PASSWORD '{rousrpwd}';
        END IF;
    END
    $$;

    GRANT CONNECT ON DATABASE bikesharedb TO rouser;
    GRANT USAGE ON SCHEMA public TO rouser;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO rouser;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT ON TABLES TO rouser;
    REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public FROM rouser;
    ALTER ROLE rouser CONNECTION LIMIT 10;
    """

    print("Creating / updating read-only analytics role 'rouser'...")
    cur.execute(sql)
