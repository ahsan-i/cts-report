WITH raw_data AS (
	SELECT "amazon-web-services" AS cloud_provider, billing_account_id, export_time,r.margin as r_margin, r.cost as r_cost, DATE(PARSE_TIMESTAMP('%Y%m', invoice.month)) as invoice_month_date  
FROM `doitintl-cmp-aws-data.aws_billing.doitintl_billing_export_v1`  
LEFT JOIN UNNEST (report) r

UNION ALL

SELECT "google-cloud" AS cloud_provider, billing_account_id, export_time,r.margin as r_margin, r.cost as r_cost, DATE(PARSE_TIMESTAMP('%Y%m', invoice.month)) as invoice_month_date 
FROM `doitintl-cmp-gcp-data.gcp_billing_0033B9_BB2726_9A3CB4.doitintl_billing_export_v1_0033B9_BB2726_9A3CB4` 
LEFT JOIN UNNEST (report) r
),

identification_data_aws AS (
SELECT 
    cmp_id,
    customer_name,
    billing_account_id_aws,
    cht_id
  FROM
    `me-doit-intl-com.measurement.customer_features_identification`
  WHERE 
    timestamp = (SELECT MAX(timestamp) FROM `me-doit-intl-com.measurement.customer_features_identification`)
  GROUP BY 1,2,3,4
),

identification_data_gcp AS (
SELECT 
    cmp_id,
    customer_name,
    billing_account_id_gcp,
    cht_id
  FROM
    `me-doit-intl-com.measurement.customer_features_identification`
  WHERE 
    timestamp = (SELECT MAX(timestamp) FROM `me-doit-intl-com.measurement.customer_features_identification`)
  GROUP BY 1,2,3,4
),

margin_data AS (

SELECT 
      cloud_provider,
      invoice_month_date,
      ci_gcp.cmp_id AS gcp_customer_id,
      ci_gcp.customer_name AS gcp_customer_name,
      ci_aws.cmp_id AS aws_customer_id,
      ci_aws.customer_name AS aws_customer_name,
      (CASE WHEN cloud_provider = 'google-cloud' THEN FORMAT("%'.2f", COALESCE(SUM(r_margin), 0))  
      ELSE NULL
      END) AS gcp_total_margin,
      (CASE WHEN cloud_provider = 'amazon-web-services' THEN FORMAT("%'.2f", COALESCE(SUM(r_margin), 0))  
      ELSE NULL
      END) AS aws_total_margin,
       (CASE WHEN cloud_provider = 'google-cloud' THEN FORMAT("%'.2f", COALESCE(SUM(r_cost), 0))  
      ELSE NULL
      END) AS gcp_total_cost,
      (CASE WHEN cloud_provider = 'amazon-web-services' THEN FORMAT("%'.2f", COALESCE(SUM(r_cost), 0))  
      ELSE NULL
      END) AS aws_total_cost
   
FROM raw_data as T 
LEFT JOIN identification_data_aws ci_aws ON billing_account_id = ci_aws.billing_account_id_aws
LEFT JOIN identification_data_gcp ci_gcp ON billing_account_id = ci_gcp.billing_account_id_gcp
WHERE 
  DATE(T.export_time) >= DATE("2024-12-01")
	AND DATE(T.export_time) <= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)  
  AND invoice_month_date BETWEEN DATE("2024-12-01") and DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) 
GROUP BY 1,2,3,4,5,6
),

margin_data_final as (
SELECT 
COALESCE(gcp_customer_id,aws_customer_id) AS customer_id, 
COALESCE(gcp_customer_name, aws_customer_name) AS customer_name,
invoice_month_date,
SUM(CAST(REPLACE(gcp_total_margin, ',', '') AS FLOAT64)) AS gcp_total_margin, 
SUM(CAST(REPLACE(aws_total_margin, ',', '') AS FLOAT64)) AS aws_total_margin ,
SUM(CAST(REPLACE(gcp_total_cost, ',', '') AS FLOAT64)) AS gcp_total_cost,
SUM(CAST(REPLACE(aws_total_cost, ',', '') AS FLOAT64)) AS aws_total_cost  
FROM margin_data
GROUP BY 1,2,3
ORDER BY 4
)

SELECT 
margin_data_final.invoice_month_date,
margin_data_final.customer_id,
margin_data_final.customer_name,
COALESCE(gcp_total_margin,0) as gcp_total_margin, 
COALESCE(aws_total_margin,0) as aws_total_margin,
COALESCE(gcp_total_cost,0) as gcp_total_cost, 
COALESCE(aws_total_cost,0) as aws_total_cost,
(COALESCE(gcp_total_margin,0) + COALESCE(aws_total_margin,0)) AS total_margin_resold, (COALESCE(gcp_total_cost,0) + COALESCE(aws_total_cost,0)) AS total_cost_resold,
COALESCE(dci_invoices.invoice_total,0) as dci_margin,
(COALESCE(gcp_total_cost,0) + COALESCE(aws_total_cost,0) + (COALESCE(dci_invoices.invoice_total,0))) as total_cost_resold_and_dci,
(COALESCE(gcp_total_margin,0) + COALESCE(aws_total_margin,0) + (COALESCE(dci_invoices.invoice_total,0))) as total_margin_resold_and_dci
FROM margin_data_final
LEFT JOIN `doit-zendesk-analysis.otherdata.dci_invoices` dci_invoices on dci_invoices.customer_id = margin_data_final.customer_id and dci_invoices.invoice_month = margin_data_final.invoice_month_date



