import psycopg2
import re
import unittest
from unittest.mock import patch, MagicMock
import random
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
import os
load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("scriptpwd") ,
    "host": os.getenv("scripthost"),
    "port": os.getenv("scriptport")
}
# Nginx log file path
# Regex pattern to parse Nginx logs
LOG_FILE_PATH = "nginx_test.log"

# Regex pattern to parse Nginx logs
LOG_PATTERN = re.compile(
    r'\S+ - - \[(?P<timestamp>.+?)\] "\S+ \S+ \S+" '
    r'(?P<status>\d+) (?P<number_of_request>\d+)'
)

STATUS_DESCRIPTIONS = {
    "200": "Success",
    "301": "Moved Permanently",
    "400": "Bad Request",
    "403": "Forbidden",
    "404": "Not Found",
    "500": "Internal Server Error"
}

def connect_db():
    """Establishes a connection to PostgreSQL."""
    return psycopg2.connect(**DB_CONFIG)

def create_table():
    """Creates a table for storing Nginx logs if it does not exist."""
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS logs (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP,
                    status INT,
                    number_of_request INT,
                    status_description TEXT
                )
            ''')
            conn.commit()

def parse_log_line(line):
    """Parses a single log line and returns a dictionary of extracted fields."""
    match = LOG_PATTERN.match(line)
    if match:
        log_data = match.groupdict()
        log_data["status_description"] = STATUS_DESCRIPTIONS.get(log_data["status"], "Unknown")
        return log_data
    return None

def insert_log_entry(log_data):
    """Inserts parsed log data into PostgreSQL."""
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                INSERT INTO logs (timestamp, status, number_of_request, status_description)
                VALUES (%s, %s, %s, %s)
            ''', (
                log_data['timestamp'],
                int(log_data['status']),
                int(log_data['number_of_request']),
                log_data['status_description']
            ))
            conn.commit()

def generate_random_logs(num=100):
    """Generates random Nginx log entries and writes them to a test log file."""
    statuses = ["200", "301", "400", "403", "404", "500"]
    
    with open(LOG_FILE_PATH, "w") as f:
        for _ in range(num):
            log_entry = (
                f"127.0.0.1 - - "
                f"[{(datetime.utcnow() - timedelta(days=random.randint(0, 30))).strftime('%d/%b/%Y:%H:%M:%S +0000')}] "
                f'"GET /index.html HTTP/1.1" {random.choice(statuses)} {random.randint(200, 5000)}'
            )
            f.write(log_entry + "\n")

def process_log():
    """Reads the Nginx log file and inserts parsed data into the database."""
    with open(LOG_FILE_PATH, "r") as file:
        for line in file:
            log_data = parse_log_line(line)
            print("Debug: ", log_data)
            if log_data:
                insert_log_entry(log_data)

class TestNginxLogProcessing(unittest.TestCase):
    
    def test_parse_log_line(self):
        sample_log = '127.0.0.1 - - [05/Mar/2025:12:34:56 +0000] "GET /index.html HTTP/1.1" 200 1234'
        expected_output = {
            'timestamp': '05/Mar/2025:12:34:56 +0000',
            'status': '200',
            'number_of_request': '1234',
            'status_description': 'Success'
        }
        self.assertEqual(parse_log_line(sample_log), expected_output)

    @patch('psycopg2.connect')
    def test_insert_log_entry(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        log_data = {
            'timestamp': '2025-03-05 12:34:56 +0000',
            'status': '200',
            'number_of_request': '1234',
            'status_description': 'Success'
        }
        
        insert_log_entry(log_data)
        mock_conn.cursor().execute.assert_called_once()
        mock_conn.commit.assert_called_once()

# if __name__ == "__main__":
#     generate_random_logs()
#     create_table()
#     process_log()
#     print("Log processing complete.")
#     unittest.main()

if __name__ == "__main__":
    create_table()
    while True:
        generate_random_logs()
        process_log()
        print("Log processing complete. Sleeping for 12 hours...")
        time.sleep(24 * 3600)  # Sleep for 24 hours
