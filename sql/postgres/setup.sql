CREATE DATABASE IF NOT EXISTS charts;

USE charts;

CREATE TABLE IF NOT EXISTS top200 (
  `track_id` FixedString(22),
  `region_code` FixedString(2),
  `pos` UInt8,
  `streams` UInt32,
  `date` Date
) ENGINE = MergeTree() -- The primary key is also the sorting key, so the data is sorted by date, region_code, track_id
PRIMARY KEY (date, region_code, track_id);