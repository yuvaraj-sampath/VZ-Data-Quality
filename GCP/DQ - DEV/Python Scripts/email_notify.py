import configparser
import os
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud import bigquery
from email.message import EmailMessage
from datetime import datetime
from pandas.core.config_init import string_storage_doc

def send_email(subject, body, to_email):
    """
    Sends an email with the specified subject and body to the given email addresses.
    
    Parameters:
    subject (str): The subject of the email.
    body (str): The HTML body of the email.
    to_email (list): List of recipient email addresses.
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
        text = msg.as_string()
        server.sendmail(sender_email, to_email, text)
        server.quit()
        print(f"Email sent to {to_email}")
    except Exception as e:
        print(f"Error sending email to {to_email}: {e}")

def fetch_invalid_records(domain:str , config_params:dict):
    """
    Fetches invalid records for a given domain from BigQuery.
    
    Parameters:
    domain (str): The domain to fetch invalid records for.
    
    Returns:
    list: A list of invalid records.
    """
    client = bigquery.Client()
    reporting_tbl = f"{config_params['project_name']}.{config_params['dataset_name']}.{config_params['reporting_tbl']}"
    metadata_tbl = f"{config_params['project_name']}.{config_params['dataset_name']}.{config_params['metadata_tbl']}"
    query = f"""
    WITH filtered_rules AS (
    SELECT
        rpt.rule_id as `Rule ID`,
        rpt.rule_run_dt as `Rule Run Date`,
        rpt.col_tot_cnt as `Total Count`,
        rpt.col_invld_cnt as `Invalid Count`,
        rpt.col_invld_pct as `Invalid Percent`,
        mtd.src_tbl as `Table Name`,
        mtd.src_col as `Column Name`,
        mtd.dq_pillar as `DQ Pillar`,
        mtd.meas_rule_desc as `Rule Description`,
        mtd.data_dmn
    FROM (
        SELECT *
        FROM `{reporting_tbl}` rpt
        WHERE SAFE_CAST(col_invld_pct AS STRING) != 'NA'
          AND col_invld_pct < 100
          AND col_invld_pct > 0
          AND col_invld_pct IS NOT NULL
          AND rpt.col_tot_cnt !=0
          AND DATE(rule_run_dt) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    ) rpt
    JOIN `{metadata_tbl}` mtd
      ON rpt.rule_id = mtd.rule_id
)
    SELECT * 
    FROM filtered_rules 
    WHERE upper(data_dmn) = '{domain}';
    """
    try:
        query_job = client.query(query)
        results = list(query_job.result())
        return results
    except Exception as e:
        print(f"Error fetching records for domain {domain}: {e}")
        return None

def fetch_distinct_domains(config_params:dict) -> list:
    """
    Fetches distinct domains from BigQuery.
    
    Returns:
    list: A list of distinct domains.
    """
    client = bigquery.Client()
    metadata_tbl = f"{config_params['project_name']}.{config_params['dataset_name']}.{config_params['metadata_tbl']}"
    query = f"""
    SELECT DISTINCT upper(data_dmn) as data_dmn
    FROM `{metadata_tbl}`
    where is_active_flg = 'Y';
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        return [row['data_dmn'] for row in results]
    except Exception as e:
        print(f"Error fetching distinct domains: {e}")
        return []

def fetch_data_owners(domain:str, config_params:dict) -> list:
    """
    Fetches data owners from BigQuery.

    Returns:
    list: A list of email ids from data owner.
    """
    client = bigquery.Client()
    one_corp_dq_data_owners_tbl_name = f"{config_params['project_name']}.{config_params['dataset_name']}.{config_params['one_corp_dq_data_owners_tbl_name']}"
    query = f"""
    SELECT tech_owner_email
    FROM `{one_corp_dq_data_owners_tbl_name}`
    WHERE domain = '{domain}'
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        emails_list = list({email for row in results for email in row['tech_owner_email'].split(",")})
        return emails_list
    except Exception as e:
        print(f"Error fetching distinct domains: {e}")
        return []

def format_results_as_html(results, config_params):
    """
    Formats the results as an HTML table.
    
    Parameters:
    results (list): The list of results to format.
    
    Returns:
    str: The HTML formatted results.
    """
    rows = [dict(row) for row in results]
    df = pd.DataFrame(rows)
    # Sort by col_invld_pct from highest to lowest
    df = df.sort_values(by='Invalid Percent', ascending=False)
    
    # Trim the date part to YYYY-MM-DD
    df['Rule Run Date'] = df['Rule Run Date'].dt.strftime('%Y-%m-%d')
    
    # Rearrange columns and remove DQ_Indicator
    df = df[['Rule ID', 'Rule Run Date', 'Table Name', 'Column Name', 'DQ Pillar','Rule Description',
             'Total Count', 'Invalid Count', 'Invalid Percent']]
    total_count = len(df)
    headers = df.columns.tolist()
    html = """
    <html>
    <body>
    <p>Hi,</p>
    
    Please find Invalid Records
    """
    html += "<tr>"
    html += f"<td colspan='{len(headers)}'><strong>Total Count of Invalid Rule: {total_count}</strong></td>"
    html += "</tr>"
    html += "<tr>"
    html += f"<td colspan='{len(headers)}'><a href={config_params['qlik_link']}>Qlik DashBoard Link</a></td>"
    html += "</tr> <table border='1'> <tr style='background-color: #ADD8E6;'>"

    for header in headers:
        html += f"<th style='white-space: nowrap;'>{header}</th>"
    html += "</tr>"
    
    for _, row in df.iterrows():
        html += "<tr>"
        for header in headers:
            cell_value = row[header]
            html += f"<td>{cell_value}</td>"
        html += "</tr>"
    html += """
    </table>
    <p>Please Reach out to <a href="mailto:OneCorpDataQuality@verizon.com">OneCorpDataQuality</a> in case of any concerns</p>
    <p>Thanks,<br/>DQ Team</p>
    </body>
    </html>
    """
    return html



def _read_config(env: str) -> dict:
    """
    Reads the configuration for the specified environment from the 'env_config.ini' file.

    Parameters:
    env (str): The environment to read the configuration for (e.g., 'dev', 'test', 'qa', 'prod').

    Returns:
    dict: A dictionary containing the configuration for the specified environment.
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
        print(f"Error occurred while reading environment config file: {e}")
        return env_config


def main(request):
    """
    Main function to fetch invalid records for each domain and send an email with the results.
    """
    environment = os.getenv('ENVIRONMENT', 'DEV')
    print(f"Environment: {environment}")
    current_date = datetime.now().strftime('%d %B %Y')
    config_params = _read_config(environment)
    domains = fetch_distinct_domains(config_params)
    print(f"Fetched domains: {domains}")
    for domain in domains:
        results = fetch_invalid_records(domain, config_params)
        to_email = fetch_data_owners(domain, config_params)
        if results and len(results) > 0:
            html_body = format_results_as_html(results, config_params)
            subject = f"<{environment}> DQ Invalid Records - {current_date} - {domain}"
            print(f"Sending email to {to_email} ,subject ={subject} for domain {domain}")
            send_email(
                subject=subject,
                body=html_body,
                to_email=to_email
            )

        else:
            print(f"{environment} - No invalid records found for domain - {current_date} - {domain}")
    return 'Function executed successfully'