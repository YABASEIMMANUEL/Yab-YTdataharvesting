# YouTube Data Harvesting and Warehousing Application
 
## Introduction
 
This Python application is designed to harvest and warehouse data from YouTube channels and videos. It utilizes the Streamlit framework to create an intuitive user interface and interacts with the YouTube Data API and a MySQL database to extract, transform, and load relevant data.
 
## Summary
 
The application offers the following key features:
 
1. **Channel Data Extraction**: Retrieve comprehensive details about YouTube channels, including channel name, description, subscriber count, total videos, and cumulative views.
 
2. **Video Data Extraction**: Extract granular video-level data, such as titles, descriptions, view counts, like counts, comment counts, duration, thumbnails, and caption availability status.
 
3. **Comment Data Extraction**: Harvest comments associated with videos, capturing details like comment text, author, and posting date.
 
4. **Data Warehousing**: Store the extracted data in a MySQL database for efficient data management and querying.
 
5. **Analytics**: Leverage the Streamlit interface to explore and analyze the stored data through interactive dashboards and predefined queries.
 
## Execution
 
To run the application, follow these steps:
 
1. **Prerequisites**: Ensure that you have Python 3.x installed on your system, along with the required Python packages listed in the `requirements.txt` file.
 
2. **Database Setup**: Set up a MySQL database instance and update the connection details in the application code.
 
3. **API Key Integration**: Obtain a YouTube Data API key from the Google Cloud Platform and integrate it into the application.
 
4. **Dependency Installation**: Install the required Python packages by executing `pip install -r requirements.txt` in your terminal or command prompt.
 
5. **Application Execution**: Launch the Streamlit application by running `streamlit run app.py` in your terminal or command prompt.
 
6. **Data Extraction**: On the "Extract and Transform" page, enter a valid YouTube channel ID and click the "Extract Data" button to fetch the channel details. Then, click the "Upload to MySQL" button to store the extracted data in the MySQL database.
 
7. **Data Exploration**: Navigate to the "Query" page and select a desired question from the dropdown menu to analyze the corresponding insights based on the data stored in the MySQL database.
 
## Conclusion
 
This YouTube Data Harvesting and Warehousing Application provides a comprehensive solution for extracting, transforming, and analyzing data from YouTube channels and videos. With its user-friendly interface and integration with the YouTube Data API and a MySQL database, users can efficiently harvest and warehouse relevant data. The application's querying capabilities enable users to explore and gain valuable insights into various aspects of YouTube channels and videos.
