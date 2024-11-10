Before working on BuildSummary DAG, make sure you have two tables available (change database and schema according to your environment)

1. Create two tables
```
CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'  
);

CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp  
);
```

2. Populate two tables
```
-- for the following query to run, 
-- the S3 bucket should have LIST/READ privileges for everyone
CREATE OR REPLACE STAGE dev.raw_data.blob_stage
url = 's3://s3-geospatial/readonly/'
file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');

COPY INTO dev.raw_data.user_session_channel
FROM @dev.raw_data.blob_stage/user_session_channel.csv;

COPY INTO dev.raw_data.session_timestamp
FROM @dev.raw_data.blob_stage/session_timestamp.csv;
```
