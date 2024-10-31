import psycopg2 as pg  # Import PostgreSQL adapter for Python
import datetime  # Import datetime module to work with date and time
from configparser import ConfigParser  # Import ConfigParser to handle configuration files
import smtplib  # Import smtplib to send emails
from email.mime.multipart import MIMEMultipart  # Import for creating multi-part email
from email.mime.text import MIMEText  # Import for creating email body

# Log file for recording operations and errors
log_file = 'dropduplicates.log'

def log_to_file(message):
    """Append a message to the log file with a timestamp."""
    with open(log_file, 'a') as log:
        log.write(f"{datetime.datetime.now()} - {message}\n")

def send_email(subject, body):
    """Send an email using SMTP, getting configurations from r_emailConfig.ini."""
    try:
        # Read email configuration from the specified ini file
        email_config = server_config('r_emailConfig.ini', 'email_config')

        # Set up the SMTP server using SSL for security
        server = smtplib.SMTP_SSL(email_config['smtp_host'], email_config['smtp_port'])

        # Log in to the email server with the provided credentials
        server.login(email_config['smtp_username'], email_config['smtp_password'])

        # Create an email message with the specified subject and body
        msg = MIMEMultipart()
        msg['From'] = email_config['sender_email']
        msg['To'] = email_config['receiver_email']
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))  # Attach the email body in plain text

        # Send the email to the recipient
        server.sendmail(email_config['sender_email'], email_config['receiver_email'], msg.as_string())

        # Quit the SMTP server after sending the email
        server.quit()

    except Exception as email_error:
        # Log any errors that occur during the email sending process
        log_to_file(f"Failed to send email: {email_error}")

def server_config(filename, section):
    """Read the database or email configuration from a file."""
    parser = ConfigParser()  # Create a ConfigParser object
    parser.read(filename)  # Read the configuration file

    config = {}
    if parser.has_section(section):
        params = parser.items(section)  # Get all parameters in the specified section
        for param in params:
            config[param[0]] = param[1]  # Store parameters in a dictionary
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return config  # Return the configuration dictionary

def check_for_duplicates(cur, db_table, unique_key):
    """Check if there are duplicates in the specified table."""
    try:
        # SQL query to find duplicates based on the unique key
        duplicate_check_query = f"""
                            SELECT dateCreated, {unique_key}, COUNT(*)
                            FROM {db_table}
                            GROUP BY dateCreated, {unique_key}
                            HAVING COUNT(*) > 1
                            ORDER BY dateCreated DESC;
        """
        cur.execute(duplicate_check_query)  # Execute the query
        duplicate_keys = cur.fetchall()  # Fetch all duplicate keys

        return duplicate_keys  # Return duplicates found (or empty if none)

    except Exception as e:
        # Log any errors encountered while checking for duplicates
        log_to_file(f"Error checking duplicates in {db_table}: {str(e)}")
        send_email("Error Checking Duplicates", f"Error checking duplicates in {db_table}: {str(e)}")
        return None  # Return None to indicate an error occurred

def remove_duplicates_from_table(cur, db_table, unique_key):
    """Function to remove duplicates from a specific table using procedural SQL steps."""
    try:
        # Step 1: Insert duplicates into a temporary table
        step1_sql = f"""
        BEGIN;

        WITH duplicates_cte AS (
            SELECT
                COUNT(*) OVER (PARTITION BY {unique_key}) AS total_duplicates,  -- Count duplicates
                ROW_NUMBER() OVER (PARTITION BY {unique_key} ORDER BY dateCreated) AS duplicate_rn,  -- Assign row number
                *
            FROM {db_table}
        )
        SELECT * 
        INTO {db_table}_duplicates  -- Create a new table for duplicates
        FROM duplicates_cte
        WHERE total_duplicates > 1 AND duplicate_rn = 1;  -- Filter for duplicates

        COMMIT;
        """

        cur.execute(step1_sql)  # Execute the SQL statement
        log_to_file(f"Step 1: Duplicates inserted into temporary table for {db_table}.")

        # Step 2: Remove duplicates from the main table
        step2_sql = f"""
        BEGIN;

        DELETE FROM {db_table}  -- Delete duplicates from the main table
        USING {db_table}_duplicates 
        WHERE {db_table}_duplicates.{unique_key} = {db_table}.{unique_key};

        COMMIT;
        """

        cur.execute(step2_sql)  # Execute the SQL statement
        log_to_file(f"Step 2: Duplicates removed from {db_table}.")

        # Step 3: Insert back the non-duplicate rows
        step3_sql = f"""
        BEGIN;

        ALTER TABLE {db_table}_duplicates DROP COLUMN total_duplicates;  -- Remove extra columns
        ALTER TABLE {db_table}_duplicates DROP COLUMN duplicate_rn;
        INSERT INTO {db_table}  -- Insert the remaining non-duplicate rows back into the main table
        SELECT * 
        FROM {db_table}_duplicates;

        COMMIT;
        """

        cur.execute(step3_sql)  # Execute the SQL statement
        log_to_file(f"Step 3: Non-duplicate rows inserted back into {db_table}.")

        # Step 4: Drop the temporary table
        step4_sql = f"""
        BEGIN;

        DROP TABLE {db_table}_duplicates;  -- Remove the temporary table

        COMMIT;
        """

        cur.execute(step4_sql)  # Execute the SQL statement
        log_to_file(f"Step 4: Temporary table {db_table}_duplicates dropped.")

        # Send email notification once duplicates have been removed
        email_subject = f"Duplicate Removal Notification for {db_table}"
        email_body = f"Duplicates have been successfully removed from {db_table}."
        send_email(email_subject, email_body)

    except Exception as e:
        # Log any errors encountered while processing duplicates
        log_to_file(f"Error processing steps for {db_table}: {str(e)}")
        send_email("Error Removing Duplicates", f"Error processing steps for {db_table}: {str(e)}")
        # Optionally roll back the transaction in case of error
        cur.execute("ROLLBACK;")
        log_to_file(f"Transaction rolled back due to error.")

def remove_duplicates():
    """Main function to remove duplicates from Redshift tables."""
    parser = ConfigParser()  # Create a ConfigParser object
    parser.read('r_duplicates.ini')  # Read the configuration file containing table details

    # Get the Redshift connection details from the ini file
    config = server_config('r_duplicates.ini', 'yoda_r_lake')
    conn = None  # Initialize connection variable
    try:
        log_message = f"Connecting to {config['host']} database {config['database']}..."
        log_to_file(log_message)  # Log connection attempt
        print(log_message)  # Print connection message

        # Establish connection to the Redshift database
        conn = pg.connect(**config)
        conn.autocommit = True  # Set autocommit mode
        cur = conn.cursor()  # Create a cursor to execute queries

        # Loop through each section in the config file that starts with 'yoda_hub'
        for section in parser.sections():
            if section.startswith('yoda_hub'):
                table_config = server_config('r_duplicates.ini', section)

                # Extract table-specific parameters
                table_name = table_config['table']
                unique_key = table_config['unique_key']
                db_table = f"{table_config['database']}.{table_name}"  # Full table reference

                log_to_file(f"Checking for duplicates in {db_table}...")
                duplicate_keys = check_for_duplicates(cur, db_table, unique_key)  # Check for duplicates

                if duplicate_keys:  # If duplicates are found
                    print(f"Duplicates found in {db_table}. Processing...")
                    log_to_file(f"Duplicates found in {db_table}. Processing...")
                    remove_duplicates_from_table(cur, db_table, unique_key)  # Remove duplicates
                else:
                    print(f"No duplicates found in {db_table}. Skipping...")
                    log_to_file(f"No duplicates found in {db_table}. Skipping...")

    except Exception as e:
        # Log any errors encountered during the duplicate removal process
        log_to_file(f"Failed to process: {str(e)}")
        send_email("Error in Duplicate Removal Process", f"Failed to process: {str(e)}")
        print(f"Failed to process: {str(e)}")

    finally:
        if conn:  # Close the database connection if it was opened
            conn.close()
        print("Connection closed.")  # Print connection closure message

if __name__ == '__main__':
    remove_duplicates()  # Execute the main function when the script runs
