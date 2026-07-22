-- ADF Lookup Query
-- Used by the ADF Lookup activity at the start of each pipeline run.
-- Reference in ADF: @activity('LookupWatermark').output.firstRow

SELECT
    w.last_watermark,     -- остання успішно оброблена мітка часу
    c.source_type,        -- тип джерела: CSV | SQL | PostgreSQL | API (для розгалуження в ADF)
    c.watermark_column,   -- назва колонки в джерелі, яка використовується як watermark
    c.lookback_days,      -- скільки днів назад переглядати для запізнілих записів
    c.window_size_hours   -- розмір вікна обробки в годинах (зазвичай 24)
FROM pipeline_watermark_control w
JOIN pipeline_config c ON w.pipeline_name = c.pipeline_name
-- з'єднуємо таблицю стану з таблицею конфігурації по назві пайплайну
-- NOTE: 'uci_retail_sales' is hardcoded here for reference only.
-- In ADF, replace with a pipeline parameter to make the Lookup reusable:
-- WHERE w.pipeline_name = '@{pipeline().parameters.pipeline_name}'
WHERE w.pipeline_name = 'uci_retail_sales'
