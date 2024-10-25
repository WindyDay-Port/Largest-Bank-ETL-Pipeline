# Extract, Transform and Load data from world's largest banks into a database for further processing and querying
This project implements an ETL (Extract, Transform, Load) pipeline that extracts data from a Wikipedia page listing the largest banks in the world, transforms the data using exchange rates, and loads it into a CSV file and an SQLite database.

## Table of Contents

- [Overview](#overview)
- [Technologies Used](#technologies-used)
- [Installation](#installation)
- [Usage](#usage)
- [ETL Process](#etl-process)
- [Queries](#queries)
- [Logging](#logging)
- [Contributing](#contributing)
- [License](#license)

## Overview

This ETL pipeline performs the following operations:

1. **Extract**: Scrapes data from the Wikipedia page listing the largest banks.
2. **Transform**: Converts the extracted data using exchange rates for different currencies.
3. **Load**: Saves the transformed data to a CSV file and loads it into an SQLite database.

## Technologies Used

- Python
- Pandas
- NumPy
- Requests
- BeautifulSoup
- SQLite
- Logging

## Installation

To set up the project, follow these steps:

1. Clone the repository:
   ```
   git clone http://github.com/WindyDay-Port/Simple-ETL-pipeline.git
   ```
2. Change into the project directory:
  ```
  cd pipeline
  ```
3. Install the required packages:
  ```
  pip install pandas numpy requests beautifulsoup4
  ```

## Usage

To run the ETL pipeline, execute the following command in your terminal:
  ```
  python your_script_name.py
  ```
Replace <your_script_name.py> with the name of your Python script containing the ETL code.

## ETL Process
The ETL process is structured into the following functions:

* extract(endpoint, table_attributes): Extracts data from the specified webpage.
* transform(dataframe, ex_dataframe): Transforms the extracted data using exchange rates.
* load_to_csv(dataframe, save_path): Saves the transformed DataFrame to a CSV file.
* load_to_db(dataframe, connection, table): Loads the DataFrame into an SQLite database.
* run_query(query_input, connection): Executes queries on the SQLite database and prints the results.

## Queries
The following SQL queries can be executed on the SQLite database:

1. Select all records from the Largest_banks table.
2. Calculate the average market capitalization in GBP billion.
3. Retrieve the names of the top 5 largest banks.

## Logging
Logging is set up to track the progress and errors throughout the ETL process. Log messages will be saved to code_log.txt.

## Contributing
If you would like to contribute to this project, please fork the repository and create a pull request with your changes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
