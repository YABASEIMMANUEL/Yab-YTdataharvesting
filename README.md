# YouTube Data Harvesting and Warehousing using SQL and Streamlit

This is a Python application that utilizes the Streamlit framework to build a user interface for harvesting and warehousing YouTube data. The application connects to the YouTube Data API and a MySQL database to extract, transform, and load data from YouTube channels and videos.

## Features

- Extract channel details, including name, description, subscriber count, total videos, and views.
- Extract video details, including title, description, views, likes, comments, duration, thumbnail, and caption status.
- Extract comment details, including comment text, author, and posted date.
- Store extracted data in a MySQL database.
- View insights and analytics based on the stored data through a Streamlit interface.

## Requirements

- Python 3.x
- Streamlit
- Pandas
- GoogleAPIs
- MySQL Connector

## Installation

1. Clone the repository or download the source code.
2. Install the required Python packages by running `pip install -r requirements.txt`.
3. Set up a MySQL database and update the database connection details in the code.
4. Obtain a YouTube Data API key and update the `api_key` variable in the code.

## Usage

1. Run the application by executing `python -m streamlit run filename.py` in your terminal or command prompt.
2. The application will open in your default web browser.
3. Navigate through the sidebar options to extract and transform data or view insights.
4. To extract data, enter a valid YouTube channel ID and click the "Extract Data" or "Upload to MySQL" button.
5. To view insights, select a question from the dropdown menu on the "View" page.

## Basic Workflow and Execution

1. **Set up the MySQL Database**: Create a new MySQL database and update the connection details in the code.
2. **Obtain YouTube Data API Key**: Follow the instructions from the Google Cloud Platform to obtain an API key for the YouTube Data API.
3. **Install Dependencies**: Install the required Python packages by running `pip install`. 
4. **Run the Application**: Execute `python -m streamlit run filename.py` in your terminal or command prompt to start the Streamlit application.
5. **Extract Data**: On the "Extract and Transform" page, enter a valid YouTube channel ID and click the "Extract Data" button to fetch the channel details. Then, click the "Upload to MySQL" button to store the extracted data in the MySQL database.
6. **View Insights**: Navigate to the "View" page and select a question from the dropdown menu to view the corresponding insights based on the data stored in the MySQL database.

## Contributing

Contributions are welcome! If you find any issues or have suggestions for improvements, please open an issue or submit a pull request.

