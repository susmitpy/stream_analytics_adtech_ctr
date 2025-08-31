CREATE TABLE IF NOT EXISTS clicks (
      click_id STRING,
      impr_id STRING,
      user_id STRING,
      ts BIGINT,
      event_time AS TO_TIMESTAMP_LTZ(ts, 3),
      WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'clicks',
      'properties.bootstrap.servers' = 'kafka:9092',
      'properties.group.id' = 'pyflink-ctr',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json'
    )