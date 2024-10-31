import psycopg2 as pg
import datetime
from configparser import ConfigParser
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests  # Import requests for Slack notifications

# Log file for recording operations and errors
log_file = 'dropduplicates.log'

def log_to_file(message):
    """Append a message to the log file with a timestamp."""
    with open(log_file, 'a') as log:
        log.write(f"{datetime.datetime.now()} - {message}\n")

def send_email(subject, body, email_config):
    """Send an email using SMTP, getting configurations from email configuration."""
    try:
        # Set up the SMTP server
        server = smtplib.SMTP_SSL(email_config['smtp_host'], email_config['smtp_port'])
        server.login(email_config['smtp_username'], email_config['smtp_password'])

        # Create email message
        msg = MIMEMultipart()
        msg['From'] = email_config['sender_email']
        msg['To'] = email_config['receiver_email']
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        # Send the email
        server.sendmail(email_config['sender_email'], email_config['receiver_email'], msg.as_string())
        server.quit()
        print(f"Email sent at {datetime.datetime.now()}\n")
    except Exception as e:
        log_to_file(f"Failed to send email: {e}")

def send_slack_alert(message, slack_webhook_url):
    """Send an alert message to a Slack channel using a webhook URL."""
    try:
        payload = {
            "text": message,
            "username": "Duplicates Checker",
            "icon_emoji": ":warning:"
        }
        response = requests.post(slack_webhook_url, json=payload)
        if response.status_code != 200:
            log_to_file(f"Failed to send Slack alert: {response.text}")
        else:
            print("Slack alert sent successfully.")
    except Exception as e:
        log_to_file(f"Error sending Slack alert: {e}")

def server_config(filename, section):
    """Read configuration from an ini file."""
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return db

def get_table_config(section, config_path='/r_duplicates.ini'):
    """Get the table configuration for a given section from an ini file."""
    return server_config(config_path, section)

def print_duplicate_info(database_name, table_name, grouped_duplicates):
    """Print and return formatted information about duplicates."""
    output = f"\nDuplicates found in {database_name}.{table_name}:\n"
    for duplicate_count, rows in grouped_duplicates.items():
        output += f"{len(rows)} row(s) affected: with {duplicate_count} duplicates per row\n"
    print(output)
    return output

def get_duplicates_and_alert():
    """Check for duplicates in specified tables and send alerts via email and Slack."""
    parser = ConfigParser()
    parser.read('/r_duplicates.ini')
    
    email_config = server_config('/r_emailConfig.ini', 'email_config')
    slack_webhook_url = email_config['slack_webhook_url']  # Fetching Slack webhook URL

    try:
        # Establish connection to Redshift
        with pg.connect(**server_config('/r_duplicates.ini', 'yoda_r_lake')) as conn:
            with conn.cursor() as cur:
                # Loop through each section (table) in the config
                for section in parser.sections():
                    if section.startswith('yoda_hub'):
                        table_config = get_table_config(section)

                        # Extract necessary table parameters
                        unique_key = table_config['unique_key']
                        database_name = table_config['database']
                        table_name = table_config['table']
                        host_name = table_config['host']
                        replication_task = table_config['replication_task']

                        # Query to find duplicates in Unique_key column
                        query = f"""
                            SELECT dateCreated, {unique_key}, COUNT(*)
                            FROM {database_name}.{table_name}
                            GROUP BY dateCreated, {unique_key}
                            HAVING COUNT(*) > 1
                            ORDER BY dateCreated DESC;
                        """

                        # Execute the query and fetch the results
                        cur.execute(query)
                        duplicate_results = cur.fetchall()

                        if duplicate_results:
                            total_rows = len(duplicate_results)
                            grouped_duplicates = {}
                            for row in duplicate_results:
                                date_created, row_key, duplicate_count = row
                                grouped_duplicates.setdefault(duplicate_count, []).append(row_key)

                            # Construct the email message
                            subject = f"Duplicate(s) found in {database_name}.{table_name} at {datetime.datetime.now()}"
                            text = (f"Duplicate(s) found in {database_name}.{table_name} at {datetime.datetime.now()}.\n\n"
                                    f"DETAILS:\n"
                                    f"Source Host: {host_name}\n"
                                    f"Source Replication Task: {replication_task}\n"
                                    f"Source Database: {database_name}\n"
                                    f"Source Table: {table_name}\n"
                                    f"Source Column: {unique_key}\n\n"
                                    f"Total number of rows = {total_rows}\n\n")

                            output_text = print_duplicate_info(database_name, table_name, grouped_duplicates)
                            text += output_text

                            # Send Email Alert
                            send_email(subject, text, email_config)

                            # Send Slack Alert
                            slack_message = f"Alert: Duplicates found in {database_name}.{table_name}:\n{output_text}"
                            send_slack_alert(slack_message, slack_webhook_url)
                        else:
                            print(f"No duplicate found in {database_name}.{table_name} at {datetime.datetime.now()}")

    except Exception as err:
        log_to_file(f"Fetching duplicates from Redshift failed with error: {err}")

def main():
    """Main function to start the duplicate checking job."""
    start_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print("\n........Starting Job..................", start_time, "\n")
    get_duplicates_and_alert()
    end_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print("\n.......Job Finished.......", end_time, "\n")

if __name__ == '__main__':
    main()
