-- ip_address, latitude, longitude dropped — PII
SELECT
    _id                     AS event_id,
    transaction_id,
    device_type,
    user_agent,
    latency_ms,
    timestamp::TIMESTAMP    AS event_at
FROM {{ ref('stg_telemetry_events') }}
WHERE _ab_cdc_deleted_at IS NULL
