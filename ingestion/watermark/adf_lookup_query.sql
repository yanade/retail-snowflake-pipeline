-- ADF Lookup Query
-- Returns the current ingestion state and configuration for a pipeline.
-- ADF passes @pipeline_name as a dataset parameter.

SELECT
    w.last_watermark,
    c.source_type,
    c.watermark_column,
    c.lookback_days,
    c.window_size_hours
FROM dbo.pipeline_watermark_control AS w
INNER JOIN dbo.pipeline_config AS c
    ON w.pipeline_name = c.pipeline_name
WHERE w.pipeline_name = @pipeline_name;
