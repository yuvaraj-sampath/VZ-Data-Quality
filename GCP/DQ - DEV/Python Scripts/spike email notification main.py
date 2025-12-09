import configparser
import os
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud import bigquery
from datetime import datetime

def send_email(subject, body, to_email):
    """
    Sends an email with the specified subject and body to the given email addresses.
    """
    sender_email = "do-not-reply@verizon.com"
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(to_email)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))
    
    try:
        server = smtplib.SMTP('tpaapd1kva109.verizon.com')
        server.starttls()
        server.sendmail(sender_email, to_email, msg.as_string())
        server.quit()
        print(f"Email sent to {to_email}")
    except Exception as e:
        print(f"Error sending email to {to_email}: {e}")

def fetch_spike_data(config_params):
    """
    Fetches spike data from BigQuery based on the SQL logic.
    """
    client = bigquery.Client()
    query = f"""
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
      FROM `{config_params['project_name']}.{config_params['dataset_name']}.{config_params['reporting_tbl']}`
      GROUP BY rule_id
    ),
    CurrentDayData AS (
      SELECT 
        DATE(rule_run_dt) AS current_date,
        col_invld_pct AS today_invalid_pct,
        col_tot_cnt AS total_count,
        rule_id
      FROM `{config_params['project_name']}.{config_params['dataset_name']}.{config_params['reporting_tbl']}`
      WHERE DATE(rule_run_dt) = CURRENT_DATE() - 1
    ),
    TableDetails AS ( 
      SELECT DISTINCT rule_id, db_name, src_tbl, data_dmn
      FROM `{config_params['project_name']}.{config_params['dataset_name']}.{config_params['metadata_tbl']}`
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
        c.today_invalid_pct - COALESCE(a.last_week_avg, 0) AS diff_from_last_week,
        c.today_invalid_pct - COALESCE(a.last_month_avg, 0) AS diff_from_last_month,
        c.today_invalid_pct - COALESCE(a.last_3_months_avg, 0) AS diff_from_last_3_months,
        c.today_invalid_pct - COALESCE(a.last_6_months_avg, 0) AS diff_from_last_6_months
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
        SUM(last_week_avg) AS week_avg,
        SUM(last_month_avg) AS month_avg,
        SUM(last_3_months_avg) AS quarter_avg,
        SUM(last_6_months_avg) AS biannual_avg,
        SUM(diff_from_last_week) AS weekly_diff, 
        SUM(diff_from_last_month) AS monthly_diff,
        SUM(diff_from_last_3_months) AS quarterly_diff,
        SUM(diff_from_last_6_months) AS biannual_diff
        FROM Comparison C 
        GROUP BY 1,2,3
    WHERE ABS(diff_from_last_week) > 50 OR ABS(diff_from_last_month) > 50 
          OR ABS(diff_from_last_3_months) > 50 OR ABS(diff_from_last_6_months) > 50
    ORDER BY current_date;
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        return [dict(row) for row in results]
    except Exception as e:
        print(f"Error fetching spike data: {e}")
        return []

def fetch_data_owners(domain, config_params):
    """
    Fetches data owners for a specific domain from BigQuery.
    """
    client = bigquery.Client()
    query = f"""
    SELECT tech_owner_email
    FROM `{config_params['project_name']}.{config_params['dataset_name']}.{config_params['data_owners_tbl']}`
    GROUP BY tech_owner_email, domain, table_name
    WHERE domain = '{domain}'
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        emails = {email for row in results for email in row['tech_owner_email'].split(",")}
        return list(emails)
    except Exception as e:
        print(f"Error fetching data owners for domain {domain}: {e}")
        return []

def format_spike_results_as_html(results, config_params):
    """
    Formats the spike results as an HTML table.
    """
    df = pd.DataFrame(results)
    df = df.sort_values(by='today_invalid_pct', ascending=False)
    total_count = len(df) 
    headers = df.columns.tolist()
    html = """
    <html>
    <body>
    <p>Hi,</p>
    <p>Today's invalid records got spiked more than 50%. Please find the spike details below:</p>
    <table border='1'>
    <tr style='background-color: #ADD8E6;'>
    """
    html += "<tr>"
    html += f"<td colspan='{len(headers)}'><strong>Total Count of Invalid Rule: {total_count}</strong></td>"
    html += "</tr>"
    html += "<tr>"
    html += f"<td colspan='{len(headers)}'><a href={config_params['qlik_link']}>Qlik DashBoard Link</a></td>"
    html += "</tr> <table border='1'> <tr style='background-color: #ADD8E6;'>"

    for header in headers:
        html += f"<th>{header}</th>"
    html += "</tr>"
    for _, row in df.iterrows():
        html += "<tr>"
        for header in headers:
            html += f"<td>{row[header]}</td>"
        html += "</tr>"
    html += """
    </table>
    <p>Please Reach out to <a href="mailto:OneCorpDataQuality@verizon.com">OneCorpDataQuality</a> in case of any concerns</p>
    <p>Thanks,<br/>DQ Team</p>
    </body>
    </html>
    """
    return html

def _read_config(env):
    """
    Reads the configuration for the specified environment.
    """
    config = configparser.ConfigParser()
    env_config = {}
    try:
        config.read('env_config.ini')
        if config.has_section('commons'):
            env_config.update(dict(config.items('commons')))
        if config.has_section(env):
            env_config.update(dict(config.items(env)))
        return env_config
    except Exception as e:
        print(f"Error reading config: {e}")
        return {}

def main(request):
    """
    Main function to fetch spike data and send emails to data owners.
    """
    environment = os.getenv('ENVIRONMENT', 'DEV')
    print(f"Environment: {environment}")
    config_params = _read_config(environment)
    
    # Fetch spike data
    spike_data = fetch_spike_data(config_params)
    if not spike_data:
        print("No spike data found.")
        return 'No spike data found.'
    
    # Process each domain
    for domain, group in pd.DataFrame(spike_data).groupby('domain'):
        to_email = fetch_data_owners(domain, config_params)
        if not to_email:
            print(f"No data owners found for domain: {domain}")
            continue
        
        # Format email content and send
        html_body = format_spike_results_as_html(group.to_dict('records'),config_params)
        subject = f"<{environment}> Spike Alert - {domain} - {datetime.now().strftime('%d %B %Y')}"
        send_email(subject, html_body, to_email)
        print(f"Email sent for domain: {domain}")
    
    return 'Spike alert emails sent successfully.'