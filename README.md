# Spotify Data Pipline Python
This is Spotify data pipeline, where python is used to fetch data from spotify api, transform it and create 3 tables for further analysis

Tables created :
  - songs
  - Albums
  - artists

## Data Pipeline
![alt text](https://github.com/harish95/Spotify_Data_Pipline_Python/blob/main/Spotify%20Data%20Pipeling%20using%20Python%20%26%20AWS.png)

## Objective
Ensure Availability of data tables(Songs, Artists and Albums) for analysis for given playlist and timely updation from spotify api to  existing data sources

## Pipeline
- Spotify API Configuration
- Data Fetched from Spotify API in JSON format using aws lambda (python)
- Data Stored in Raw format in S3 bucket
- Lambda function for Data Transformation gets trigger based upload file event in S3
- Data is filtered and stored into 3 tables
    - Songs
    - Albums
    - Artists
- AWS Glue Crawler is run to infer schema for 3 tables
- All 3 tables accessed using AWS Athena for further analysis

