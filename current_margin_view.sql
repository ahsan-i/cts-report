
WITH net_revenue_cte AS ( 
SELECT
  CAST(fnr.month AS DATE) as invoice_month_date,
  lower(domain) as customer_name,
  ci.customer_id as customer_id, 
CAST(SUM(CASE WHEN platform = 'GCP' THEN profit ELSE 0 END) AS FLOAT64) AS gcp_total_profit,
CAST(SUM(CASE WHEN platform = 'AWS' THEN profit ELSE 0 END) AS FLOAT64) AS aws_total_profit,
CAST(SUM(CASE WHEN platform = 'GCP' THEN revenue ELSE 0 END) AS FLOAT64) AS gcp_total_revenue,
CAST(SUM(CASE WHEN platform = 'AWS' THEN revenue ELSE 0 END) AS FLOAT64) AS aws_total_revenue,

--delete
CAST(SUM(CASE WHEN platform = 'GCP' THEN profit ELSE 0 END) AS FLOAT64) AS gcp_total_margin,
CAST(SUM(CASE WHEN platform = 'AWS' THEN profit ELSE 0 END) AS FLOAT64) AS aws_total_margin,
CAST(SUM(CASE WHEN platform = 'GCP' THEN revenue ELSE 0 END) AS FLOAT64) AS gcp_total_cost,
CAST(SUM(CASE WHEN platform = 'AWS' THEN revenue ELSE 0 END) AS FLOAT64) AS aws_total_cost,
FROM
  `me-doit-intl-com.analytics.finance_net_revenue` fnr 
LEFT JOIN (SELECT distinct customer_id, primary_domain from `me-doit-intl-com.cloud_analytics.doitintl_csp_metadata_v1`) ci on ci.primary_domain = lower(domain) 
GROUP BY
1,2,3
ORDER BY
1,2
)
SELECT net_revenue_cte.*, 

(COALESCE(dci_invoices.invoice_total,0)) as dci_margin,  --delete
(COALESCE(gcp_total_revenue,0) + COALESCE(aws_total_revenue,0) + (COALESCE(dci_invoices.invoice_total,0))) as total_cost_resold_and_dci, --delete
(COALESCE(gcp_total_profit,0) + COALESCE(aws_total_profit,0) + (COALESCE(dci_invoices.invoice_total,0))) as total_margin_resold_and_dci, --delete
(COALESCE(gcp_total_margin,0) + COALESCE(aws_total_margin,0)) AS total_margin_resold, --delete
(COALESCE(gcp_total_cost,0) + COALESCE(aws_total_cost,0)) AS total_cost_resold, --delete

(COALESCE(dci_invoices.invoice_total,0)) as dci_profit,
(COALESCE(gcp_total_profit,0) + COALESCE(aws_total_profit,0)) AS total_profit_resold, 
(COALESCE(gcp_total_revenue,0) + COALESCE(aws_total_revenue,0)) AS total_revenue_resold, 
(COALESCE(gcp_total_revenue,0) + COALESCE(aws_total_revenue,0) + (COALESCE(dci_invoices.invoice_total,0))) as total_revenue_resold_and_dci,
(COALESCE(gcp_total_profit,0) + COALESCE(aws_total_profit,0) + (COALESCE(dci_invoices.invoice_total,0))) as total_profit_resold_and_dci
FROM net_revenue_cte
LEFT JOIN `doit-zendesk-analysis.otherdata.dci_invoices` dci_invoices on dci_invoices.customer_id = net_revenue_cte.customer_id and dci_invoices.invoice_month = net_revenue_cte.invoice_month_date
