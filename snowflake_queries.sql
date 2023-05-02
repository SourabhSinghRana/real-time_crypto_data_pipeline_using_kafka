CREATE DATABASE KAFKA_LIVE_DATA;

USE DATABASE KAFKA_LIVE_DATA;


CREATE TABLE top_100_crypto_data (
    SYSTEM_INSERTED_TIMESTAMP TIMESTAMP,
    RANK INTEGER,
    NAME VARCHAR,
    SYMBOL VARCHAR,
    PRICE NUMBER,
    PERCENT_CHANGE_24H FLOAT,
    VOLUME_24H NUMBER,
    MARKET_CAP NUMBER,
    CURRENCY VARCHAR
);


CREATE TABLE top_100_crypto_data (
    json_data VARIANT
);

CREATE OR REPLACE STAGE ext_stage
URL = 's3://coinmarketcap-bucket/real_time_data/'
CREDENTIALS = (
    AWS_KEY_ID='<key-id>',
    AWS_SECRET_KEY='<secret-key>'
);





CREATE OR REPLACE PIPE live_crypto_data
AUTO_INGEST = TRUE
AS
COPY INTO KAFKA_LIVE_DATA.PUBLIC.TOP_100_CRYPTO_DATA
FROM @ext_stage
FILE_FORMAT = (TYPE=JSON);


--for manually refreshing the snowpipe
ALTER PIPE live_crypto_data REFRESH;

SHOW PIPES;


SELECT count(*) AS COUNT FROM kafka_live_data.public.top_100_crypto_data;


CREATE TABLE top_100_crypto_data_sink (
    SYSTEM_INSERTED_TIMESTAMP TIMESTAMP,
    RANK INTEGER,
    NAME VARCHAR,
    SYMBOL VARCHAR,
    PRICE NUMBER,
    PERCENT_CHANGE_24H FLOAT,
    VOLUME_24H NUMBER,
    MARKET_CAP NUMBER,
    CURRENCY VARCHAR
);

