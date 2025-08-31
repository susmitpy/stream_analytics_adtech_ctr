CREATE TABLE IF NOT EXISTS ctr_by_campaign (
      campaign_id STRING,
      window_start TIMESTAMP_LTZ(3),
      window_end   TIMESTAMP_LTZ(3),
      impressions BIGINT,
      clicks      BIGINT,
      ctr         DOUBLE
    ) PARTITIONED BY (campaign_id) WITH (
      'connector' = 'filesystem',
      'path' = '/job-src/output/ctr_results',
      'format' = 'csv',
      'csv.field-delimiter' = ',',
      'sink.rolling-policy.file-size' = '128MB',
      'sink.rolling-policy.rollover-interval' = '15 min',
      'sink.partition-commit.trigger' = 'process-time',
      'sink.partition-commit.delay' = '1 min',
      'sink.partition-commit.policy.kind' = 'success-file'
    )