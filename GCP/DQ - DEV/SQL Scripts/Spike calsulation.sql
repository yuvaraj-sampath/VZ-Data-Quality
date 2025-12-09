WITH AggregatedAverages AS (
  SELECT 
    rule_id, 
    AVG(CASE 
          WHEN DATE(rule_run_dt) BETWEEN DATE_TRUNC(CURRENT_DATE(), WEEK(SUNDAY)) - INTERVAL 1 WEEK 
                                    AND DATE_TRUNC(CURRENT_DATE(), WEEK(SUNDAY)) - INTERVAL 1 DAY 
          THEN col_invld_pct 
        END) AS last_week_avg,
    AVG(CASE 
          WHEN DATE(rule_run_dt) BETWEEN DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 1 MONTH 
                                    AND DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 1 DAY 
          THEN col_invld_pct 
        END) AS last_month_avg,
    AVG(CASE 
          WHEN DATE(rule_run_dt) BETWEEN DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 3 MONTH 
                                    AND DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 1 DAY 
          THEN col_invld_pct 
        END) AS last_3_months_avg,
    AVG(CASE 
          WHEN DATE(rule_run_dt) BETWEEN DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 6 MONTH 
                                    AND DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 1 DAY 
          THEN col_invld_pct 
        END) AS last_6_months_avg
  FROM `vz-it-np-j0nv-dev-oddo-0.od_dq.dqaas_onecorp_rule_prfl_rpt` 
  --WHERE DATE(rule_run_dt)<  DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) --we can use this to reduce the data scan if possible
  GROUP BY rule_id
),
CurrentDayData AS (
  SELECT 
    DATE(rule_run_dt) AS current_date,
    col_invld_pct AS today_invalid_pct,
    col_tot_cnt AS total_count,
    rule_id
  FROM `vz-it-np-j0nv-dev-oddo-0.od_dq.dqaas_onecorp_rule_prfl_rpt`
  WHERE DATE(rule_run_dt) = CURRENT_DATE() -1 --actual is current date for testing purpose considering prev day 
),
TableDetails AS ( 
 SELECT DISTINCT rule_id, 
        db_name,
        src_tbl, 
        data_dmn
 FROM `vz-it-np-j0nv-dev-oddo-0.od_dq.dqaas_onecorp_rule_prfl_mtd`
), 
Comparison AS (
  SELECT
    c.rule_id,
    c.current_date,
    c.today_invalid_pct,
    c.total_count,
    t.db_name,
    t.src_tbl,
    t.data_dmn,
    a.last_week_avg,
    a.last_month_avg,
    a.last_3_months_avg,
    a.last_6_months_avg,
    c.today_invalid_pct - COALESCE(a.last_week_avg,0) AS diff_from_last_week,
    c.today_invalid_pct - COALESCE(a.last_month_avg,0) AS diff_from_last_month,
    c.today_invalid_pct - COALESCE(a.last_3_months_avg,0) AS diff_from_last_3_months,
    c.today_invalid_pct - COALESCE(a.last_6_months_avg,0) AS diff_from_last_6_months
  FROM CurrentDayData c
  LEFT JOIN AggregatedAverages a ON c.rule_id = a.rule_id 
  LEFT JOIN TableDetails t ON a.rule_id = t.rule_id
)

SELECT 
 db_name,
 src_tbl AS table_name,
 data_dmn AS domain ,
 MAX(C.current_date) AS current_date , 
 SUM(today_invalid_pct) AS invalid_percent ,
 SUM(total_count) AS total_count, 
 SUM(last_month_avg) AS month_avg,
 SUM(last_3_months_avg) AS quarter_avg,
 SUM(last_6_months_avg) AS biannual_avg,
 SUM(diff_from_last_week) AS weekly_diff, 
 SUM(diff_from_last_month) AS monthly_diff,
 SUM(diff_from_last_3_months) AS quarterly_diff,
 SUM(diff_from_last_6_months) AS biannual_diff
FROM Comparison C 
GROUP BY 1,2,3
--WHERE upper(data_dmn) = '{domain}'
--HAVING weekly_diff>1 OR monthly_diff >1 OR quarterly_diff >1 OR   biannual_diff > 1
ORDER BY current_date;