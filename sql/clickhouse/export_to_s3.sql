INSERT INTO
    FUNCTION s3(
        'https://s3.eu-central-2.wasabisys.com/spotifycharts/tableName.parquet',
        -- replace with actual key
        's3_key',
        -- replace with actual secret
        's3_secret',
        'Parquet'
    )
SELECT
    *
FROM
    tableName;