                                            Youtube-data-harvesting-and-Warehousing project


                                  
Youtube-Data-Harvesting-And-Warehousing YouTube Data Harvesting and Warehousing is a project that intends to provide users with the ability to access and analyse data from numerous YouTube channels. SQL, MongoDB, and Streamlit are used in the project to develop a user-friendly application that allows users to retrieve, save, and query YouTube channel and video data.

TOOLS AND LIBRARIES USED: this project requires the following components:

STREAMLIT: Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis operations.

PYTHON: Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the development of the complete application, including data retrieval, processing, analysis, and visualisation.

GOOGLE API CLIENT: The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.

MONGODB: MongoDB is built on a scale-out architecture that has become popular with developers of all kinds for developing scalable applications with evolving data schemas. As a document database, MongoDB makes it easy for developers to store structured or unstructured data. It uses a JSON-like format to store documents.

MYSQL: MySQL is an open-source relational database management system, widely utilized for efficiently storing and retrieving data in various applications. Renowned for its performance, scalability, and active community support, MySQL remains a popular choice in the development of dynamic and data-driven websites.

REQUIREMENTS

Set up your development environment:

Ensure you have Python installed on your system.
Install the necessary libraries using pip, such as Streamlit, pymongo, SQLAlchemy, and google-api-python-client.
Set up a MongoDB database (you can use MongoDB Atlas for convenience) and obtain the connection string.
Set up a SQL database (e.g., MySQL, PostgreSQL) and create the necessary tables.

Approach:
1. Set up a Streamlit app: Streamlit is a great choice for building data
visualization and analysis tools quickly and easily. You can use Streamlit to
create a simple UI where users can enter a YouTube channel ID, view the
channel details, and select channels to migrate to the data warehouse.
2. Connect to the YouTube API: You'll need to use the YouTube API to retrieve
channel and video data. You can use the Google API client library for Python to
make requests to the API.
3. Store data in a MongoDB data lake: Once you retrieve the data from the
YouTube API, you can store it in a MongoDB data lake. MongoDB is a great
choice for a data lake because it can handle unstructured and semi-structured
data easily.
4. Migrate data to a SQL data warehouse: After you've collected data for
multiple channels, you can migrate it to a SQL data warehouse. You can use a
SQL database such as MySQL or PostgreSQL for this.
5. Query the SQL data warehouse: You can use SQL queries to join the tables
in the SQL data warehouse and retrieve data for specific channels based on
user input. You can use a Python SQL library such as SQLAlchemy to interact
with the SQL database.
6. Display data in the Streamlit app: Finally, you can display the retrieved data
in the Streamlit app. You can use Streamlit's data visualization features to
create charts and graphs to help users analyze the data.

