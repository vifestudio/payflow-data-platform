-- Singular test: fraud_rate must be between 0 and 1 in ALL marts.
--
-- dbt singular tests: SQL that returns FAILING rows.
-- If this query returns 0 rows -> test passes.
-- If this query returns any rows -> test fails and shows which rows broke the rule.
--
-- This catches numerical bugs: a division error, a wrong CASE, or a cast that
-- produces a rate > 1 would corrupt every Looker chart that uses fraud_rate.
SELECT 'mart_fraud_by_merchant' AS source, merchant AS key, fraud_rate
FROM {{ ref('mart_fraud_by_merchant') }}
WHERE fraud_rate < 0 OR fraud_rate > 1

UNION ALL

SELECT 'mart_fraud_daily' AS source, CAST(transaction_date AS VARCHAR) AS key, fraud_rate
FROM {{ ref('mart_fraud_daily') }}
WHERE fraud_rate < 0 OR fraud_rate > 1
