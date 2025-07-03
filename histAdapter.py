import pandas as pd
import psycopg
from datetime import datetime, timezone, timedelta
import time
from loguru import logger
from typing import Tuple, Optional, Dict, Any
import os
import argparse
import requests
from requests.auth import HTTPBasicAuth
from urllib3.exceptions import InsecureRequestWarning
from dotenv import load_dotenv
import sys
import inspect
import traceback

# Load environment variables from .env file
load_dotenv()

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Create the parser
parser = argparse.ArgumentParser(description='Connect to Proficy Historian and Transfer information to PostgreSQL Database.')

# Add arguments
parser.add_argument('--hist_server', type=str,
                    default=os.environ.get('HIST_SERVER', 'localhost'),
                    help='Historian Server (default: localhost)')

parser.add_argument('--hist_secret', type=str,
                    default=os.environ.get('HIST_SECRET', '2Secret2'),
                    help='Historian Client Secret (default: 2Secret2)')

parser.add_argument('--pg_port', type=int,
                    default=int(os.environ.get('PG_PORT', '5432')),
                    help='PostgreSQL Port to connect on (default: 5432)')

parser.add_argument('--pg_user', type=str,
                    default=os.environ.get('PG_USER', 'postgres'),
                    help='PostgreSQL User (default: postgres)')

parser.add_argument('--pg_password', type=str,
                    default=os.environ.get('PG_PASSWORD', 'postgres'),
                    help='PostgreSQL Password (default: postgres)')

parser.add_argument('--pg_server', type=str,
                    default=os.environ.get('PG_SERVER', 'localhost'),
                    help='PostgreSQL Server (default: Localhost)')

parser.add_argument('--debug', action='store_true',
                    default=os.environ.get('DEBUG', '').lower() in ('true', '1', 'yes'),
                    help='Debug enabled (default: False)')

parser.add_argument('--backfill', type=int,
                    default=int(os.environ.get('BACKFILL', '1')),
                    help='The time in days to backfill PostgreSQL with data (default: 10)')

parser.add_argument('--hist_connection_timeout', type=int,
                    default=int(os.environ.get('HIST_CONNECTION_TIMEOUT', '')),
                    help='Historian Connection Timeout (default: 10)')

parser.add_argument('--hist_response_timeout', type=int,
                    default=int(os.environ.get('HIST_RESPONSE_TIMEOUT', '')),
                    help='Historian Response Timeout (default: 3)')

parser.add_argument('--message_support', action='store_true',
                    default=os.environ.get('MESSAGE_SUPPORT', '').lower() in ('true', '1', 'yes'),
                    help='Enable Support Emails (default: False)')


# Parse arguments
args = parser.parse_args()

# Calculate the path to shared_modules and to sys.path
script_dir = os.path.dirname(__file__)
module_path = os.path.abspath(os.path.join(script_dir, '..', 'shared_modules'))
sys.path.insert(0, module_path)
import notification as notification

HIST_ADAPTER_VERSION = '25.04.27'

PG_CONN = {
    "host": args.pg_server,
    "port": args.pg_port,
    "dbname": "guardsman",
    "user": args.pg_user,
    "password": args.pg_password
}


LOG_PATH = './logs/trace.log'
HISTORIAN_SERVER_NAME = args.hist_server
HISTORIAN_PASSWORD = args.hist_secret
DEBUG = args.debug
BACKFILL = args.backfill
HIST_CONNECTION_TIMEOUT = args.hist_connection_timeout
HIST_RESPONSE_TIMEOUT = args.hist_response_timeout
MESSAGE_SUPPORT = args.message_support
MAX_RETRIES = 3  # Maximum number of attempts to get token
RETRY_DELAY = 5  # Seconds to wait between retry attempts


def query_postgres(connection_params: Dict[str, Any], sql_statement: str, params=None) -> Tuple[int, Optional[pd.DataFrame]]:
    """
    Query information from a Postgres database.

    Args:
        connection_params (Dict[str, Any]): The connection parameters to the Postgres database
        sql_statement (str): The SQL statement to execute
        params (list, optional): Parameters to be used with the SQL statement for parameterized queries

    Returns:
        Tuple[int, Optional[pd.DataFrame]]: A tuple containing:
            - status code (0 for success, non-zero for failure)
            - pandas DataFrame with the results (if applicable, None otherwise)
    """
    function_name = 'query_postgres'
    status_code = 0
    result_df = None

    try:
        # Connect to the PostgreSQL database
        conn = psycopg.connect(**connection_params)


        # Create a cursor
        with conn.cursor() as cursor:
            # Execute the SQL statement with parameters if provided
            if params:
                cursor.execute(sql_statement, params)
            else:
                cursor.execute(sql_statement)

            # Check if the query returns data
            if cursor.description:
                # Fetch all rows
                rows = cursor.fetchall()

                # Get column names
                column_names = [desc[0] for desc in cursor.description]

                # Create a pandas DataFrame
                result_df = pd.DataFrame(rows, columns=column_names)

            # Commit the transaction
            conn.commit()

    except Exception as e:
        # Handle other errors
        status_code = 2
        logger.error(f"Error: {e}")
        notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                       'Import Shared Modules', traceback.format_exc() + ' - ' +
                                       inspect.currentframe().f_code.co_name)
        if MESSAGE_SUPPORT:
            notification.email_support()

    finally:
        # Close the connection if it exists
        if 'conn' in locals() and conn is not None:
            conn.close()

    return status_code, result_df

def request_api(token, query_url):
    """
    Sends a GET request to the specified query URL with the provided token.

    Args:
        query_url (str): The URL of the API endpoint to query.
        token (str): The authorization token to include in the request headers.

    Returns:
        tuple: A dictionary containing the response data and the HTTP status code.
    """
    function_name = 'request_api'

    # Define the headers for the request, including the authorization token
    headers = {'Authorization': 'Bearer ' + token}

    try:

        # Send a GET request to the query URL with the specified headers and timeouts
        response = requests.get(query_url, headers=headers, verify=False,
                                timeout=(HIST_CONNECTION_TIMEOUT, HIST_RESPONSE_TIMEOUT))

        if response.status_code != 200:
            return False, None
        else:
            # There was a valid response
            resp_dict = response.json()
            return True, resp_dict

    except:
        return False, None



def read_time_series_data(connection_params: Dict[str, Any], table_name: str, tag_list: str, 
                      start_time, end_time, sample_interval: int) -> pd.DataFrame:
    """
    Query time series data from a TimescaleDB table with sampling, gap filling, and interpolation.

    Args:
        connection_params (Dict[str, Any]): Database connection parameters
        table_name (str): Name of the table to query
        tag_list (str): Comma-delimited string of metrics/tags to retrieve
        start_time: Start time for the query (inclusive)
        end_time: End time for the query (exclusive)
        sample_interval (int): Sampling interval in seconds

    Returns:
        pd.DataFrame: DataFrame with timestamp column and one column per tag, ordered by timestamp
    """
    function_name = 'read_time_series_data'
    try:
        # Check for empty tag_list
        if not tag_list or tag_list.strip() == '':
            logger.warning("Empty tag list provided to read_time_series_data")
            return pd.DataFrame(columns=['timestamp'])

        # Convert comma-delimited string to list
        tags = [tag.strip() for tag in tag_list.split(',')]

        # Validate table_name to prevent SQL injection
        if not table_name or not isinstance(table_name, str) or ';' in table_name:
            logger.error(f"Invalid table name provided: {table_name}")
            notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                           'Import Shared Modules', traceback.format_exc() + ' - ' +
                                           inspect.currentframe().f_code.co_name)
            if MESSAGE_SUPPORT:
                notification.email_support()

            return pd.DataFrame(columns=['timestamp'] + tags)

        # Build the SQL query with time bucketing, interpolation, and gap filling
        # For each tag, we create a subquery that selects the tag's data and pivots it
        subqueries = []
        query_params = []

        for tag in tags:
            # Use parameterized queries for all values including tag names for security
            subquery = """
            SELECT 
                time_bucket(%s, time_bucket_gapfill(%s, "timestamp", %s, %s)) as bucket_time,
                %s as metric_name,
                LOCF(AVG(value)) as value
            FROM """ + table_name + """
            WHERE metric_name = %s
            AND quality = 'good'
            AND "timestamp" >= %s
            AND "timestamp" < %s
            GROUP BY bucket_time, metric_name
            """
            subqueries.append(subquery)
            query_params.extend([
                f'{sample_interval} seconds',
                f'{sample_interval} seconds',
                start_time,
                end_time,
                tag,
                tag,
                start_time,
                end_time
            ])

        # Combine all subqueries with UNION ALL
        combined_query = " UNION ALL ".join(subqueries)

        # Build the case statements for pivoting
        case_statements = []
        for tag in tags:
            case_statements.append(f'MAX(CASE WHEN metric_name = %s THEN value END) as "{tag}"')
            query_params.append(tag)

        # Final query to pivot the data
        final_query = """
        WITH combined_data AS (
            """ + combined_query + """
        )
        SELECT 
            bucket_time as timestamp,
            """ + ', '.join(case_statements) + """
        FROM combined_data
        GROUP BY bucket_time
        ORDER BY bucket_time
        """

        # Execute the query
        status_code, result_df = query_postgres(
            connection_params, 
            final_query, 
            params=query_params
        )

        if status_code != 0 or result_df is None:
            # Return empty DataFrame with correct columns if query fails
            logger.error(f"Query failed with status code {status_code}")
            notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                           'Import Shared Modules', traceback.format_exc() + ' - ' +
                                           inspect.currentframe().f_code.co_name)
            if MESSAGE_SUPPORT:
                notification.email_support()
            return pd.DataFrame(columns=['timestamp'] + tags)

        # Ensure the result is ordered by timestamp (should already be from the query)
        if not result_df.empty and 'timestamp' in result_df.columns:
            result_df = result_df.sort_values('timestamp')

        return result_df

    except Exception as e:
        # Handle any other unexpected errors
        logger.error(f"Unexpected error in read_time_series_data: {e}")
        notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                       'Import Shared Modules', traceback.format_exc() + ' - ' +
                                       inspect.currentframe().f_code.co_name)
        if MESSAGE_SUPPORT:
            notification.email_support()
        return pd.DataFrame(columns=['timestamp'])

def extract_unique_tags(tag_list_df):
    """
    Extract unique tags from a DataFrame containing a tag_list column.

    Args:
        tag_list_df (pd.DataFrame): DataFrame containing a tag_list column with comma-delimited lists of tags

    Returns:
        str: A comma-delimited string of unique tags
    """

    function_name = 'extract_unique_tags'
    # Create a set to store unique tags
    unique_tags = set()

    # Iterate through each row in the DataFrame
    for _, row in tag_list_df.iterrows():
        # Skip rows with empty or NaN tag_list
        if pd.isna(row['tag_list']) or row['tag_list'] == '':
            continue

        # Split the comma-delimited list and add each tag to the set
        tags = [tag.strip() for tag in row['tag_list'].split(',')]
        unique_tags.update(tags)

    # Join the unique tags into a comma-delimited string
    return ','.join(sorted(unique_tags))

def get_token():
    """
    Retrieves the required access token to support the historian REST calls.

    If an error occurs (status code not equal to 0), the function will wait for 
    RETRY_DELAY seconds and try again. After MAX_RETRIES unsuccessful attempts, 
    the program will exit.

    Args:

    Returns:
        tuple: A tuple containing:
            - status code (0 for success, non-zero for failure)
            - access token (if successful, None otherwise)

    Raises:
        SystemExit: If the token retrieval fails after MAX_RETRIES attempts.
    """
    function_name = 'get_token'
    status_code = 0
    # Construct the username by appending '.admin' to the server name
    user = HISTORIAN_SERVER_NAME + ".admin"

    # Construct the query URL for the token request
    query_url = "https://" + HISTORIAN_SERVER_NAME + ":443/uaa/oauth/token?grant_type=client_credentials"

    # Define the parameters for the token request
    params = {"grant_type": "client_credentials"}

    # Initialize retry counter
    retry_count = 0

    while retry_count < MAX_RETRIES:
        try:
            # Send a GET request to the query URL with the parameters and authentication
            response = requests.get(query_url, params, auth=HTTPBasicAuth(user, HISTORIAN_PASSWORD), verify=False,
                                    timeout=(HIST_CONNECTION_TIMEOUT, HIST_RESPONSE_TIMEOUT))

            # Parse the response as JSON
            resp_dict = response.json()

            # Check if the response was successful (200 OK)
            if response.status_code == 200:
                # Extract the access token from the response dictionary
                token = resp_dict.get('access_token', None)
                return status_code, token
            else:
                status_code = response.status_code
                message = f"Error retrieving token: {response.status_code} for Historian {HISTORIAN_SERVER_NAME} - Retrying - Count {retry_count}"
                logger.info(message)
                notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                               function_name, message)

                # Increment retry counter
                retry_count += 1

                if retry_count < MAX_RETRIES:
                    message = f"Retrying in {RETRY_DELAY} seconds (attempt {retry_count}/{MAX_RETRIES})"
                    logger.info(message)
                    notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                                      function_name, message)
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error(f"System stopped after {MAX_RETRIES} failed attempts")
                    notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                                      function_name, message)

                    if MESSAGE_SUPPORT:
                        notification.email_support()
                    exit(-1)
        except Exception as e:
            status_code = -1
            logger.error(f"Exception retrieving token for Historian {HISTORIAN_SERVER_NAME}: {str(e)}")
            notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                           'Import Shared Modules', traceback.format_exc() + ' - ' +
                                           inspect.currentframe().f_code.co_name)
            if MESSAGE_SUPPORT:
                notification.email_support()

            # Increment retry counter
            retry_count += 1

            if retry_count < MAX_RETRIES:
                logger.info(f"Retrying in {RETRY_DELAY} seconds (attempt {retry_count}/{MAX_RETRIES})...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"System stopped after {MAX_RETRIES} failed attempts - Possible invalid client secret")
                notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                               'Import Shared Modules', traceback.format_exc() + ' - ' +
                                               inspect.currentframe().f_code.co_name)
                if MESSAGE_SUPPORT:
                    notification.email_support()
                exit(-2)


def get_raw_data(token, start_utc, end_utc, tag_list):
    """
    Retrieves raw data from the historian API for the specified tags and time range.

    If an error occurs, the function will wait for RETRY_DELAY seconds and try again.
    After MAX_RETRIES unsuccessful attempts, the program will exit.

    Args:
        token (str): The authorization token for the API request
        start_utc (datetime): The start time for the data query in UTC
        end_utc (datetime): The end time for the data query in UTC
        tag_list (str): Comma-separated list of tags to query

    Returns:
        pd.DataFrame: DataFrame containing the retrieved data

    Raises:
        SystemExit: If the data retrieval fails after MAX_RETRIES attempts.
    """
    function_name = 'get_raw_data'
    # Format with the specified format string
    start_utc_str = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc_str = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Create a semi colon delimited string for the tag list
    tag_list_str = tag_list.replace(',',';')

    query_url1 = "https://" + HISTORIAN_SERVER_NAME + ":443/historian-rest-api/v1/datapoints/sampled?tagNames="
    query_url2 = tag_list_str + '&start='+ start_utc_str + '&end=' + end_utc_str + '&samplingMode=4&direction=1'
    query_url = query_url1 + query_url2

    # Initialize retry counter
    retry_count = 0

    # Initialize empty DataFrame for the result
    asset_data_df = pd.DataFrame()

    while retry_count < MAX_RETRIES:
        status, resp_dict = request_api(token, query_url)
        if status:
            data = resp_dict.get('Data')
            if len(data) == 0:
                message = f"No data returned for tags {tag_list_str} between {start_utc_str} and {end_utc_str}"
                logger.info(message)
                notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                               function_name, message)
            else:
                num_tags = len(tag_list_str.split(";"))
                asset_data_df = pd.DataFrame()
                for i in range(num_tags):
                    tag_name = data[i].get('TagName')
                    tag_data = data[i].get('Samples')
                    qualities = [Samples['Quality'] for Samples in tag_data]
                    timestamps = [Samples['TimeStamp'] for Samples in tag_data]
                    values = [Samples['Value'] for Samples in tag_data]
                    # Create DataFrame from the lists
                    temp_df = pd.DataFrame({
                        'TagName': tag_name,
                        'Quality': qualities,
                        'TimeStamp': timestamps,
                        'Value': values
                    })
                    asset_data_df = pd.concat([asset_data_df, temp_df], ignore_index=True)

                # Change quality in Dataframe from Number to Text
                asset_data_df['Quality'] = asset_data_df['Quality'].astype(str)
                asset_data_df.loc[asset_data_df['Quality'] == '3', 'Quality'] = 'Good'
                asset_data_df.loc[asset_data_df['Quality'] != 'Good', 'Quality'] = 'Bad'

            return asset_data_df
        else:
            # Increment retry counter
            retry_count += 1

            if retry_count < MAX_RETRIES:
                message = f"Retrying get_raw_data in {RETRY_DELAY} seconds (attempt {retry_count}/{MAX_RETRIES})"
                logger.info(message)
                notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                               function_name, message)
                import time
                time.sleep(RETRY_DELAY)
            else:
                message = (f"System stopped after {MAX_RETRIES} failed attempts get raw data for tags {tag_list_str} "
                           f"between {start_utc_str} and {end_utc_str}")
                logger.error(message)
                notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                               function_name, message)
                if MESSAGE_SUPPORT:
                    notification.email_support()

                exit(-5)

    return asset_data_df


def historian_data_insert(asset_data_df, asset_id, start_time, end_time):
    """
    Insert data from a Pandas DataFrame into the historian_data PostgreSQL table.
    Uses batch insertion for improved efficiency.

    Args:
        asset_data_df (pd.DataFrame): DataFrame containing columns quality, TagName, TimeStamp, and value

    Returns:
        int: Status code (0 for success, non-zero for failure)
    """
    function = 'historian_data_insert'
    # Check if DataFrame is empty
    if asset_data_df.empty:
        message = (f"No data to insert into historian_data table for asset id {asset_id} between {start_time} "
                   f"and {end_time}")

        notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                          function_name, message)
        return 0

    try:
        # Convert value column from string to float
        asset_data_df['Value'] = asset_data_df['Value'].astype(float)

        # Connect to the PostgreSQL database directly for batch operation
        try:
            conn = psycopg.connect(**PG_CONN)

            # Create a cursor
            with conn.cursor() as cursor:
                # Prepare data for batch insertion
                data_to_insert = []
                for _, row in asset_data_df.iterrows():
                    data_to_insert.append((
                        row['Quality'],
                        row['TimeStamp'],
                        row['TagName'],
                        row['Value']
                    ))

                # Use executemany for batch insertion
                cursor.executemany(
                    "INSERT INTO historian_data (quality, timestamp, metric_name, value) VALUES (%s, %s, %s, %s)",
                    data_to_insert
                )

                # Commit the transaction
                conn.commit()

            if DEBUG:
                message = (f"Successfully inserted {len(data_to_insert)} rows into historian_data table for ")
                logger.info(message)
                notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                                  function_name, message)
            return 0

        finally:
            # Close the connection if it exists
            if 'conn' in locals() and conn is not None:
                conn.close()

    except Exception as e:
        message = f"Error inserting data into historian_data table: {e}"
        logger.error(message)
        notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                       function_name, traceback.format_exc() + ' - ' +
                                       inspect.currentframe().f_code.co_name)
        if MESSAGE_SUPPORT:
            notification.email_support()
        return 1

def process_assets():
    """
    Process assets from the asset table by iterating through each row and extracting tag_list and date_last_data.
    For each asset, create a time loop from date_last_data to current time - 10 seconds in 1-hour increments.
    For each increment, call the test function and update the date_last_data field with the end time.

    Returns:
        None
    """
    # Get all assets from the database
    function_name = 'process_assets'
    status, token = get_token()
    sql_query = 'SELECT asset_id, tag_list, date_last_data FROM asset'
    status_code, assets_df = query_postgres(PG_CONN, sql_query)

    if status_code != 0:
        message = "Error querying assets from database"
        logger.error(message)
        notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                       function_name, message)
        if MESSAGE_SUPPORT:
            notification.email_support()
        return

    if assets_df.empty:
        message = "No assets found in database"
        logger.info(message)
        notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                       function_name, message)
        return
    if DEBUG:
        message = f"Processing {len(assets_df)} assets"
        logger.info(message)
        notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                       function_name, message)


    # Process each asset
    for _, asset in assets_df.iterrows():
        current_time = datetime.now(timezone.utc) - timedelta(seconds=10)
        asset_id = asset['asset_id']
        tag_list = asset['tag_list']
        date_last_data = asset['date_last_data']

        if DEBUG:
            message = f"Processing asset ID: {asset_id}"
            logger.info(message)
            notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                              function_name, message)

        # If date_last_data is null, set it to current datetime - 4 weeks
        if pd.isna(date_last_data):
            date_last_data = current_time - timedelta(days=BACKFILL)

            if DEBUG:
                message = f"Asset {asset_id} has null date_last_data, using {date_last_data}"
                logger.info(message)
                notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                                  function_name, message)

        # Ensure date_last_data is a datetime object with timezone
        if isinstance(date_last_data, str):
            try:
                date_last_data = datetime.fromisoformat(date_last_data.replace('Z', '+00:00'))
            except ValueError:
                date_last_data = datetime.strptime(date_last_data, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

        if date_last_data.tzinfo is None:
            date_last_data = date_last_data.replace(tzinfo=timezone.utc)

        # Create time loop from date_last_data to current time in 1-hour increments
        start_time = date_last_data
        while start_time < current_time:
            # Calculate end time (start time + 1 hour or current time, whichever is earlier)
            end_time = min(start_time + timedelta(hours=1), current_time)

            if DEBUG:
                message = f"Processing time increment for asset {asset_id}: {start_time} to {end_time}"
                logger.info(message)
                notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                                  function_name, message)

            # Call the test function

            asset_data_df = get_raw_data(token, start_time, end_time, tag_list)

            if len(asset_data_df) > 0:

                # Insert data into historian_data table
                insert_status = historian_data_insert(asset_data_df, asset_id, start_time, end_time)
                if insert_status == 0:
                    if DEBUG:
                        message = f"Successfully inserted data for asset {asset_id} from {start_time} to {end_time}"
                        logger.info(message)
                        notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                                          function_name, message)

                        # Update the date_last_data field in the database
                    update_query = f"""
                    UPDATE asset 
                    SET date_last_data = '{end_time}' 
                    WHERE asset_id = {asset_id}
                    """
                    update_status, _ = query_postgres(PG_CONN, update_query)
                    if update_status != 0:
                        message = f"Error updating date_last_data for asset {asset_id}"
                        logger.error(message)
                        notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                                       function_name, message)
                        if MESSAGE_SUPPORT:
                            notification.email_support()

            start_time = end_time

def get_tag_properties(token, tagname):
    """
    Checks whether a historian tag exists by querying its 'Description' property.

    Args:
        token (str): The authentication token.
        tagname (str): The name of the tag to check.

    Returns:
        tuple: A tuple containing a boolean indicating if the API call was successful and a boolean indicating
        if the tag exists.

    Raises:
        None
    """

    # Strip leading and trailing white spaces from the tag name
    tagname = tagname.strip()

    # Construct the URL for the API request
    query_url = "https://" + HISTORIAN_SERVER_NAME + "/historian-rest-api/v1/tags/properties/" + tagname

    # Send the API request and retrieve the response
    status, resp_dict = request_api(token, query_url)
    if not status:
        message = f"System Stopped - Error retrieving tag properties for tagname {tagname}"
        logger.debug(message)
        notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                          function_name, message)
        exit(-3)
    else:
        # Operation is successful
        # If the 'ErrorCode' in the response is -1, the tag does not exist
        if resp_dict.get('ErrorCode') == -1:
            return True, False
        # Otherwise, the tag exists
        else:
            return True, True


def main():

    function_name = 'main'
    message = f"HistAdapter Started - {HIST_ADAPTER_VERSION}"
    logger.info(message)
    notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                      function_name, message)


    # Get taglist
    sql_query = "select asset_id, tag_list, date_last_data from asset where historian = 'hist'"
    status_code, tag_list_df = query_postgres(PG_CONN, sql_query)
    if status_code != 0:
        message = "Error querying tag_list from database"
        logger.error(message)
        notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                          function_name, message)
        if MESSAGE_SUPPORT:
            notification.email_support()
        return
    if tag_list_df.empty:
        message = "No tag_list found in database"
        logger.info(message)
        notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                          function_name, message)


    # Extract unique tags from the tag_list column
    try:
        unique_tags_str = extract_unique_tags(tag_list_df)
        logger.info(f"Extracted {len(unique_tags_str.split(','))} unique tags")
        if DEBUG:
            message = f"Extracted {len(unique_tags_str.split(','))} unique tags"
            logger.info(message)
            notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                              function_name, message)
    except Exception as e:
        message = "System Stopped - Error extracting unique tags {e}"
        logger.error(message)
        notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',
                                          function_name, message)
        if MESSAGE_SUPPORT:
            notification.email_support()
        exit(-1)

    status, token = get_token()
    if DEBUG:
        message = f"Retrieved token: {token}"
        logger.info(message)
        notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                          function_name, message)

    # Does the tag exist
    tag_exists_list=[]
    tag_not_exists_list=[]
    tag_list = [tag.strip() for tag in unique_tags_str.split(',')]

    for tag in tag_list:
        status, tag_exists = get_tag_properties(token, tag)
        if tag_exists:
            tag_exists_list.append(tag)
        else:
            tag_not_exists_list.append(tag)

    if len(tag_not_exists_list) == 0:
        if DEBUG:
            message = f"All tags exist in historian"
            logger.info(message)
            logger.info(notification.write_db_message_log(PG_CONN, 'Info', 'histAdapter',
                                          function_name, message))
    else:
        message = f"System Stopped - The following tags do not exist in historian: {tag_not_exists_list}"
        logger.error(message)
        notification.write_db_message_log(PG_CONN, 'Error', 'histAdapter',function_name, message)
        if MESSAGE_SUPPORT:
            notification.email_support()
        exit(-4)
    if DEBUG:
        logger.info(f"The following tags are present in historian: {tag_exists_list}")

    while True:
        process_assets()
        time.sleep(30)



if __name__ == "__main__":
    """
    Main entry point for the script.

    This block is executed when the script is run directly (i.e., not imported as a module).
    It sets up logging and calls the main function.
    """
    function_name = '__main__'
    # Add a log sink to the logger with the specified format
    logger.remove()
    logger.add(
        LOG_PATH,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
        level="DEBUG",
        rotation="1 MB",  # Rotate the file when it reaches 1MB
        retention="4 week",  # Keep logs for 4 weeks
        backtrace=True,  # Enable detailed traceback
        diagnose=True,  # Enable diagnostic information
        catch=True,  # Catch errors within the logging system
        compression="zip",
        enqueue=False,
        delay=True
    )

    # Call the main function
    main()
