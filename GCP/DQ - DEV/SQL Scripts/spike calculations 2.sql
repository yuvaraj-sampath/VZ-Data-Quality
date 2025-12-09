WITH filtered_hist AS (
  SELECT 
    M.src_tbl,
    M.rule_id,
    R.col_tot_cnt,
    DATE(R.rule_run_dt) AS rule_run_date
  FROM 
    `vz-it-np-j0nv-dev-oddo-0.od_dq.dqaas_onecorp_rule_prfl_rpt` R
  JOIN 
    `vz-it-np-j0nv-dev-oddo-0.od_dq.dqaas_onecorp_rule_prfl_mtd` M
  ON 
    R.rule_id = M.rule_id
  WHERE 
    M.is_active_flg = 'Y' 
    AND DATE(rule_run_dt) > DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
), 

filtered_curnt AS (
  SELECT  
    M.src_tbl,
    M.rule_id,
    R.col_tot_cnt
  FROM 
    `vz-it-np-j0nv-dev-oddo-0.od_dq.dqaas_onecorp_rule_prfl_rpt` R
  JOIN 
    `vz-it-np-j0nv-dev-oddo-0.od_dq.dqaas_onecorp_rule_prfl_mtd` M
  ON 
    R.rule_id = M.rule_id
  WHERE 
    M.is_active_flg = 'Y' 
    AND DATE(R.rule_run_dt) = CURRENT_DATE() - 1
),

aggregated AS ( 
  SELECT 
    h.src_tbl, 
    MAX(c.col_tot_cnt) AS today_tot_count,
    AVG(
      CASE 
        WHEN DATE(rule_run_date) BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(SUNDAY)), INTERVAL 1 WEEK) 
                                      AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(SUNDAY)), INTERVAL 1 DAY)
        THEN h.col_tot_cnt
      END
    ) AS last_week_avg,
    AVG(
      CASE 
        WHEN DATE(rule_run_date) BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 4 MONTH) 
                                      AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
        THEN h.col_tot_cnt
      END
    ) AS last_3_months_avg
  FROM 
    filtered_hist h
  JOIN 
    filtered_curnt c 
  ON 
    h.rule_id = c.rule_id
  GROUP BY 
    h.src_tbl
)

SELECT 
  src_tbl,
  today_tot_count,
  ROUND(last_week_avg, 2) AS last_week_avg,
  ROUND(today_tot_count - COALESCE(last_week_avg, 0), 2) AS week_diff,
  ROUND(last_3_months_avg, 2) AS last_3_months_avg,
  ROUND(today_tot_count - COALESCE(last_3_months_avg, 0), 2) AS quarter_diff
FROM 
  aggregated 
WHERE 
  -- src_tbl = 'scm_exp_rtn'
  today_tot_count - last_week_avg > 1 
  OR today_tot_count - COALESCE(last_week_avg, 0) > 1 
  OR today_tot_count - COALESCE(last_3_months_avg, 0) > 1 
ORDER BY 
  src_tbl; 