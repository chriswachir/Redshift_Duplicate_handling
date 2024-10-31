# Redshift_Duplicate_handling
Monitoring but by checking and removing duplicates from redshift, using sql.

# Data Duplicates Checker

## Overview
The Data Duplicates Checker is a Python-based utility that connects to Amazon Redshift databases to identify duplicate records across specified tables. It supports email and Slack notifications to alert users about found duplicates.

## Features
- Connects to Amazon Redshift databases.
- Configurable for multiple tables with specified unique keys.
- Sends email alerts for found duplicates.
- Sends Slack notifications for found duplicates.

## Requirements
- Python 3.7 or higher
- psycopg2
- ConfigParser
- smtplib
- email.mime
- datetime


### README for Duplicate Removal

# Data Duplicates Removal

## Overview
The Data Duplicates Removal is a Python-based utility designed to connect to Amazon Redshift databases to find and remove duplicate records. This tool complements the Data Duplicates Checker by automating the removal of duplicates based on specified unique keys.

## Features
- Connects to Amazon Redshift databases.
- Configurable for multiple tables with specified unique keys.
- Safely removes duplicate records while keeping one instance.
- Sends email alerts for successful removal operations.
- Sends Slack notifications after duplicates are removed.

## Requirements
- Python 3.7 or higher
- psycopg2
- ConfigParser
- smtplib
- email.mime
- datetime


You can schedule these using airflow or cron.


