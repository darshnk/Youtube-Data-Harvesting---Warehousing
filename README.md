# YouTube-Data-Harvesting-and-Warehousing-

The YouTube Data Analysis Application is a user-friendly tool designed to help users explore, analyze, and gain insights on data collected from multiple YouTube channels. Leveraging the capabilities of Streamlit, along with APIs from YouTube, MongoDB, and PostgreSQL, this application enables effortless data retrieval, storage, and analysis.

Key features of the app are :

**Easy Channel Data Retrieval**: The application allows users to input a YouTube channel ID, fetch relevant details such as video count, channel views, playlists, videos, and comments, and store this data securely in both MongoDB and PostgreSQL databases.

**Seamless Data Migration**: Users can seamlessly migrate channel data from MongoDB to a PostgreSQL database, enabling efficient data warehousing and analysis using SQL queries.

**Interactive Data Analysis**: The Data Analysis section offers a range of predefined questions about the channels, presenting users with valuable insights such as the most viewed videos, the channels with the highest number of videos, and more.

**User-Friendly Interface**: With an intuitive Streamlit interface, users can navigate through various functionalities effortlessly. The application guides users on finding YouTube channel IDs and provides a streamlined process for data storage and analysis.

**Developed using:**
------------
1. **Language** - Python

2. **Libraries** - python-youtube, pandas, mysql-connector, PyMySQL, SQLAlchemy

3. **Database** - MongoDB, MySQL

4. **API key**

5. **Front-End/GUI** - Streamlit


**Workflow**
------------
1. Create a dashboard using streamlit.
2. Obtain API credentials by enabling YouTube Data API in Google Developers console.
3. Using Google Api , extract YouTube Channel, Playlist, Video and Comment data.
4. Push the extracted YouTube data into MongoDB.
5. Migrate the data in MongoDB to SQL.
6. Retrieve the youtube information using SQL query.


**User-Interface and Output Display**
------------
1. **About the app and getting Channel ID**
------------
![Intro YT.png](https://github.com/darshnk/Youtube-Data-Harvesting-and-Warehousing/blob/main/assets/Intro%20YT.png)


2. **Enter Channel Id and Pushing the youtube data into MongoDB**
------------
![YT Channel ID.png](https://github.com/darshnk/Youtube-Data-Harvesting-and-Warehousing/blob/main/assets/YT%20Channel%20ID.png)


3. **Selecting a channel to migrate the channel data to SQL**
------------
![YT Data Mig.png](https://github.com/darshnk/Youtube-Data-Harvesting-and-Warehousing/blob/main/assets/YT%20Data%20Mig.png)


4. **Retrieving YouTube information/data from SQL by selecting the Questions in the Dropdown**
------------
![YT Data Analysis.png](https://github.com/darshnk/Youtube-Data-Harvesting-and-Warehousing/blob/main/assets/YT%20Data%20Analysis.png)



