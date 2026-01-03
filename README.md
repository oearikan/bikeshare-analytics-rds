# Bikeshare Trips & Weather Analytics Database
Read-only Analytics PostgreSQL Database (hosted on AWS RDS)
This repository contains scripts and configuration to build a **read-only analytics database** hosted on AWS, intended for use by **students and researchers** analyzing bikeshare trips and associated weather data.

The database is designed to act as a **shared, immutable source of truth** that supports exploratory analysis, custom feature engineering, and scalable querying without requiring collaborators to manage large local datasets.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Project structure](#project-structure)
- [Setup](#setup)
- [Usage](#usage)
- [References](#references)

## Overview
This project originated from a group assignment that relied on a bikeshare dataset originally prepared for an October 2013 study [*]. That dataset combined individual trip records with daily and hourly weather data for the years 2011–2012.

Several limitations of the original dataset motivated the creation of this read-only analytics database:
- **Data freshness**
Research naturally benefits from the most complete and up-to-date data available. Individual bikeshare trip data is freely published (link here), the original study is quite dated, therefore the insight it would provide relevant to this day would be limited.
- **Preprocessed, "frozen" CSVs**
The original dataset was heavily preprocessed and feature-engineered to answer specific research questions. By contrast, this database preserves the raw data, allowing researchers to define their own preprocessing steps and engineered features while keeping the underlying data unchanged.
- **Weather data consistency** 
The meteorological data source used in the original study was no longer available as of October 2025. This project uses a new weather data source (ref here), ensuring consistent coverage across all year.
- **Collaboration friction** 
Sharing large CSV files across collaborators often leads to duplicated storage, version drift, and confusion over which file is authoritative.
- **Local compute limitations** 
Once datasets exceed ~10 million rows, local analysis becomes slow and memory-constrained. Hosting the data in the cloud allows heavier queries to be executed close to the data, while smaller result sets can be pulled into local environments for further analysis.

## Features
- Automated data fetching from Capital Bikeshare S3 bucket and Open-Meteo API
- Data normalization across multiple CSV schema versions (pre-2020 vs post-2020)
- AWS RDS PostgreSQL instance provisioning via Python/boto3
- Read-only analytics user with connection limits and proper permissions
- Sample analytics queries and visualization examples

## Prerequisites
There is no mention in this document on how to satisfy below requirements, it is left to the user. Relevant documentation and modern AI tools can help fill the gap.
### AWS
- Active AWS account
- Ability to create RDS instances, EC2 security groups, and manage VPC settings
- AWS credentials configured (via `~/.aws/credentials` or environment variables)
- `VpcSecurityGroupIds` and `DBSubnetGroupName`

### Python
- Python 3.8+ installed
- pip package manager

### Environment variables
Set the following environment variables before running the setup steps script.
- `export PGPW="your_postgres_password"` _# Password for postgres admin user_
- `export ROUSRPW="your_readonly_password"` _# Password for rouser (read-only analytics user)_

## Project structure
bikeshare-analytics-rds/
├── main.py # Main setup script - provisions RDS, creates tables, and populates data
├── analytics.py # Example analytics queries and visualizations using the read-only user
├── requirements.txt # Python dependencies
├── LICENSE # Unlicense - public domain dedication
├── README.md # This file
├── py_scripts/
│ ├── rds_provision.py # AWS RDS instance creation, deletion, and connection management
│ ├── db_operations.py # Database table creation, data loading, and user management
│ ├── fetch_raw_data.py # Data fetching from Capital Bikeshare S3 and Open-Meteo API
│ └── prep_data.py # Data normalization and schema mapping utilities
├── bikeshare_csv/ # (Generated) Directory containing downloaded bikeshare CSV files
└── images/ # (Generated) Directory for visualization outputs

## Setup
### 1. Clone the Repository
`git clone <repository-url>`

`cd bikeshare-analytics-rds`
Replace `<repository-url>` with your actual repository URL.

### 2. Create a Virtual Environment
**On macOS/Linux:**
python3 -m venv venv
source venv/bin/activate

### 3. Install Dependencies
`pip install -r requirements.txt`

### 4. Configure AWS Credentials
Ensure your AWS credentials are configured. You can do this by:
- Setting up AWS CLI: `aws configure`
- Or setting environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`

### 5. Configure Environment Variables
Set the required passwords:
**On macOS/Linux:**
`export PGPW="your_postgres_password"`
`export ROUSRPW="your_readonly_password"`

### 6. Update AWS Configuration

Edit `py_scripts/rds_provision.py` and update the following values in the `create_rds()` function:
- `VpcSecurityGroupIds`: Your VPC security group ID(s)
- `DBSubnetGroupName`: Your DB subnet group name

You can find these values in your AWS Console under RDS → Subnet groups and EC2 → Security Groups.

### 7. Run the Setup Script
Execute the main setup script to provision the RDS instance, create tables, and populate data:
`python main.py`
**Note:** This process may take 15-30 minutes depending on:
- RDS instance provisioning time (~10-15 minutes)
- Data download and ingestion time (varies with dataset size)

The script will:
1. Create a PostgreSQL RDS instance (`db.t4g.micro` class) and a database named _bikesharedb_ in it.
2. Configure security group inbound rules for your IP
3. Create database tables (`rides_raw`, `daily_weather`, `hourly_weather`)
4. Download bikeshare data from S3 and normalize it
5. Fetch weather data from Open-Meteo API
6. Populate all tables with the fetched data
7. Create a read-only analytics user (`rouser`) with appropriate permissions

## Usage
Based on feedback from the scripts, some steps might need to run again or in isolation. There are also couple of helper funtions incorporated such as deleting the rds, fetching information via boto client that one might want to use.

Once the database is set and ready, use the `analytics.py` script to run a very simple analytics query and observe its results. The analytics team members can use the connection string and create all sorts of scripts for specific preprocessing and/or analytics steps such as `multiple_linear_regression.py`, `XGBoost.py` or `identify_circular_rides.py` etc. anc collaboratively grow their project and since everyone is working on the same data source, it will allow each team member to contribute to the code without having to worry about breaking references to data or rebuilding what's already been done on their specific environment.

## References
### The article the course project is based on
[*]: Fanaee-T, H., & Gama, J. (2014). Event labeling combining ensemble detectors and background knowledge. _Progress in Artificial Intelligence_, 2(2–3), 113–127. https://doi.org/10.1007/s13748-013-0040-3

### Data Sources
- **Capital Bikeshare Data**: [S3 Bucket](https://s3.amazonaws.com/capitalbikeshare-data/index.html)
- **Historical Weather Data**: [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)