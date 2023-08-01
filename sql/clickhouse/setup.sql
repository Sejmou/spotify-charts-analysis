-- Creates database and table for spotify_charts
CREATE DATABASE IF NOT EXISTS spotify_charts;

USE spotify_charts;

CREATE TABLE IF NOT EXISTS top200 (
  `track_id` FixedString(22),
  `region_code` FixedString(2),
  `pos` UInt8,
  `streams` UInt32,
  `date` Date
) ENGINE = MergeTree() PRIMARY KEY (date, region_code, track_id);

-- The primary key is also the sorting key, so the data is sorted by date, region_code, track_id