# Importing the required libraries
import pandas as pd
import numpy as np
import requests
import sqlite3
import logging
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(
    filename='../code_log.txt',
    encoding='utf-8',
    level=logging.DEBUG,
    format='%(asctime)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def log_progress(message):
    logger.debug(message)


# Declaring variables
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = {'class': 'wikitable'}
output_path = "./Largest_banks_data.csv"
csv_path = "pipeline/exchange_rate.csv"
table_name = "Largest_banks"
df_ex = pd.read_csv(csv_path)
conn = sqlite3.connect("Banks.db")
queries = [
    "SELECT * FROM Largest_banks",
    "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
    "SELECT Name FROM Largest_banks LIMIT 5"
]

log_progress("Preliminaries complete. Initiating ETL process")


# Functions for ETL processes
def extract(endpoint, table_attributes):
    """Extract data from a webpage and return it as a DataFrame."""
    response = requests.get(endpoint)
    if response.status_code == 200:
        log_progress("Successfully fetched the webpage!")
    else:
        log_progress("Failed to retrieve the webpage.")
        return None

    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find('table', attrs=table_attributes)

    if table:
        data = []
        for row in table.find_all('tr')[1:]:  # Skipping the header
            columns = row.find_all('td')
            if len(columns) >= 3:
                bank_name = columns[1].get_text(strip=True)
                market_cap = columns[2].get_text(strip=True).replace(',', '')
                data.append({"Name": bank_name, "MC_USD_Billion": float(market_cap) if market_cap else np.nan})

        dataframe = pd.DataFrame(data)
        log_progress("Data extraction complete.")
        return dataframe
    else:
        log_progress("No table found on the webpage.")
        return None


def transform(dataframe, ex_dataframe):
    """Transform extracted data, applying exchange rates for multiple currencies."""
    exchange_rate = ex_dataframe.set_index('Currency').to_dict()['Rate']
    dataframe['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in dataframe['MC_USD_Billion']]
    dataframe['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in dataframe['MC_USD_Billion']]
    dataframe['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in dataframe['MC_USD_Billion']]
    log_progress("Data transformation complete.")
    return dataframe


def load_to_csv(dataframe, save_path):
    """Save DataFrame to a CSV file."""
    try:
        dataframe.to_csv(output_path, index=False)
        log_progress(f"Data successfully saved to {save_path}")
    except Exception as e:
        log_progress(f"Error saving data to CSV: {e}")


def load_to_db(dataframe, connection, table):
    """Load DataFrame into an SQLite database table."""
    try:
        dataframe.to_sql(table, connection, if_exists='replace', index=False)
        log_progress(f"Data successfully loaded to table {table_name}")
    except Exception as e:
        log_progress(f"Error loading data to the database: {e}")


def run_query(query_input, connection):
    """Execute and print query results from the database."""
    try:
        cursor = connection.cursor()
        cursor.execute(query_input)
        results = cursor.fetchall()
        log_progress(f"Executed query: {query_input}")
        for row in results:
            print(row)
        cursor.close()
    except Exception as e:
        log_progress(f"Error executing query: {e}")


# Main ETL workflow
df = extract(url, table_attribs)
if df is not None:
    transformed_df = transform(df, df_ex)
    load_to_csv(transformed_df, output_path)
    load_to_db(transformed_df, conn, table_name)

    for query in queries:
        run_query(query, conn)

conn.close()
log_progress("Process Complete. Server Connection closed.")
