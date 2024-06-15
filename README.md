import pandas as pd
import streamlit as st
import mysql.connector as sql
from googleapiclient.discovery import build
from datetime import datetime

# SETTING PAGE CONFIGURATIONS
st.set_page_config(page_title="YouTube Data Harvesting and Warehousing",
                   layout="wide",
                   initial_sidebar_state="expanded")

# CONNECTING WITH MYSQL DATABASE
mydb = sql.connect(
    host="localhost",
    user="root",
    password="root",
    database="youtube_db"
)
mycursor = mydb.cursor(buffered=True)

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyAa1ikwBNX1Lf3qja94EPIjZIwGbzcVe3M"
youtube = build('youtube', 'v3', developerKey=api_key)

def channel_exists(channel_id):
    query = "SELECT EXISTS(SELECT 1 FROM channel_details WHERE Channel_id = %s)"
    mycursor.execute(query, (channel_id,))
    return mycursor.fetchone()[0] == 1

# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part='snippet,contentDetails,statistics', id=channel_id).execute()
    if 'items' not in response:
        st.error(f"No channel data found for Channel ID: {channel_id}")
        return []
    for i in range(len(response['items'])):
        data = {
            'Channel_id': response['items'][i]['id'],
            'Channel_name': response['items'][i]['snippet']['title'],
            'Playlist_id': response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
            'Description': response['items'][i]['snippet'].get('description', ''),
            'Subscribers': response['items'][i]['statistics'].get('subscriberCount', 0),
            'Total_videos': response['items'][i]['statistics'].get('videoCount', 0),
            'Views': response['items'][i]['statistics'].get('viewCount', 0)
        }
        ch_data.append(data)
    return ch_data

# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids):
    video_stats = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(part="snippet,contentDetails,statistics", id=','.join(v_ids[i:i+50])).execute()
        if 'items' not in response:
            continue
        for video in response['items']:
            video_details = {
                'Video_id': video['id'],
                'Title': video['snippet']['title'],
                'Description': video['snippet'].get('description', ''),
                'Views': video['statistics'].get('viewCount', 0),
                'Likes': video['statistics'].get('likeCount', 0),
                'Favorite_count': video['statistics'].get('favoriteCount', 0),
                'Comments': video['statistics'].get('commentCount', 0),
                'Duration': video['contentDetails']['duration'],
                'Thumbnail': video['snippet']['thumbnails']['default']['url'],
                'Caption_status': video['contentDetails'].get('caption', '')
            }
            video_stats.append(video_details)
    return video_stats

# FUNCTION TO GET COMMENT DETAILS
def get_comment_details(v_id):
    comment_data = []
    next_page_token = None
    while True:
        response = youtube.commentThreads().list(part="snippet,replies", videoId=v_id, maxResults=100, pageToken=next_page_token).execute()
        if 'items' not in response:
            break
        for cmt in response['items']:
            data = {
                'Comment_id': cmt['id'],
                'Comment_text': cmt['snippet']['topLevelComment']['snippet'].get('textDisplay', ''),
                'Comment_author': cmt['snippet']['topLevelComment']['snippet'].get('authorDisplayName', ''),
                'Comment_posted_date': cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                'Video_id': cmt['snippet']['topLevelComment']['snippet']['videoId']
            }
            comment_data.append(data)
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    return comment_data

# FUNCTION TO INSERT DATA INTO MYSQL
def insert_channel_details(ch_details):
    query = """
    INSERT INTO channel_details (Channel_id, Channel_name, Playlist_id, Description, Subscribers, Total_videos, Views)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
    Channel_name = VALUES(Channel_name), Subscribers = VALUES(Subscribers), 
    Views = VALUES(Views), Total_videos = VALUES(Total_videos), Description = VALUES(Description)
    """
    for detail in ch_details:
        mycursor.execute(query, (
            detail['Channel_id'],
            detail['Channel_name'],
            detail['Playlist_id'],
            detail['Description'],
            detail['Subscribers'],
            detail['Total_videos'],
            detail['Views']
        ))
    mydb.commit()

def insert_video_details(vid_details):
    query = """
    INSERT INTO video_details (Video_id, Title, Description, Views, Likes, Favorite_count, Comments, Duration, Thumbnail, Caption_status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
    Title = VALUES(Title), Thumbnail = VALUES(Thumbnail), Description = VALUES(Description), Duration = VALUES(Duration), Views = VALUES(Views), 
    Likes = VALUES(Likes), Comments = VALUES(Comments), Favorite_count = VALUES(Favorite_count), 
    Caption_status = VALUES(Caption_status)
    """
    for detail in vid_details:
        mycursor.execute(query, (
            detail['Video_id'],
            detail['Title'],
            detail['Description'],
            detail['Views'],
            detail['Likes'],
            detail['Favorite_count'],
            detail['Comments'],
            detail['Duration'],
            detail['Thumbnail'],
            detail['Caption_status']
        ))
    mydb.commit()

def insert_comment_details(comm_details):
    query = """
    INSERT INTO comment_details (Comment_id, Comment_text, Comment_author, Video_id)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
    Comment_text = VALUES(Comment_text), Comment_author = VALUES(Comment_author)
    """
    for detail in comm_details:
        mycursor.execute(query, (
            detail['Comment_id'],
            detail['Comment_text'],
            detail['Comment_author'],
            detail['Video_id'],
        ))
    mydb.commit()

# STREAMLIT APP INTERFACE
selected = st.sidebar.selectbox("Navigation", ["Home", "Extract and Transform", "View"])

# HOME PAGE
if selected == "Home":
    st.markdown("## Youtube Harvest")

# EXTRACT AND TRANSFORM
if selected == "Extract and Transform":
    st.markdown("## Extract and Transform YouTube Data")
    st.markdown("### Enter YouTube Channel ID below:")
    ch_id = st.text_input("Hint: Go to channel's home page > Right click > View page source > Find channel_id")

    if ch_id and st.button("Extract Data"):
        ch_details = get_channel_details(ch_id)
        if ch_details:
            st.write(f'#### Extracted data from **{ch_details[0]["Channel_name"]}** channel')
            st.table(ch_details)
    if ch_id and st.button("Upload to MySQL"):
        if ch_id:
            with st.spinner('Please wait...'):
                if channel_exists(ch_id):
                    st.warning("Channel data already exists in the database.")
                else: 
                    ch_details = get_channel_details(ch_id)
                    if ch_details:
                        insert_channel_details(ch_details)

                        v_ids = get_channel_videos(ch_id)
                        vid_details = get_video_details(v_ids)
                        insert_video_details(vid_details)

                        comm_details = []
                        for video_id in v_ids:
                            comm_details.extend(get_comment_details(video_id))
                        insert_comment_details(comm_details) 
                        st.success("Upload to MySQL successful!")
                    else:
                        st.error("No channel data found.")
        else:
            st.warning("Please enter a valid YouTube Channel ID.")

        # VIEW PAGE
if selected == "View":
    
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    [#'Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT Title AS Video_Title FROM video_details""")
        column_names = [desc[0] for desc in mycursor.description]
        df = pd.DataFrame(mycursor.fetchall(),columns=column_names)
        st.write(df)
       
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT Channel_name 
        AS Channel_Name, Total_videos AS Total_Videos
                            FROM channel_details
                            ORDER BY total_videos DESC""")
        column_names = [desc[0] for desc in mycursor.description]
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT Title AS Video_Title, Views AS Views 
                            FROM video_details
                            ORDER BY views DESC
                            LIMIT 10""")
        column_names = [desc[0] for desc in mycursor.description]
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.Video_id AS Video_id, a.Title AS Video_Title, b.Total_Comments
                            FROM video_details AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comment_details GROUP BY Video_id) AS b
                            ON a.Video_id = b.Video_id
                            ORDER BY b.Total_Comments DESC""")
        column_names = [desc[0] for desc in mycursor.description]
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT title AS Title,likes AS Likes 
                            FROM video_details 
                            ORDER BY likes DESC
                            LIMIT 10""")
        column_names = [desc[0] for desc in mycursor.description]
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '6. What is the total number of likes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT title AS Title, likes AS Likes
                            FROM video_details
                            ORDER BY likes DESC""")
        column_names = [desc[0] for desc in mycursor.description]
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_name, views AS Views
                            FROM channel_details
                            ORDER BY views DESC""")
        column_names = [desc[0] for desc in mycursor.description]
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT Video_id AS Video_ID
                            FROM video_details""")
        column_names = [desc[0] for desc in mycursor.description]
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""
            SELECT AVG( CASE WHEN Duration REGEXP '^PT([0-9]+H)?([0-9]+M)?([0-9]+S)?$' THEN IFNULL( TIME_TO_SEC(
                                CONCAT(
                                    IF(LOCATE('H', Duration) > 0, SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'H', 1), 'T', -1), '0'),
                                    ':',
                                    IF(LOCATE('M', Duration) > 0, SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'M', 1), 'H', -1), 'T', -1), '0'),
                                    ':',
                                    IF(LOCATE('S', Duration) > 0, SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'S', 1), 'M', -1), '0'))), 0) ELSE 0 END) AS average_duration_seconds FROM video_details vd JOIN channel_details""")
        column_names = [desc[0] for desc in mycursor.description]
        df = pd.DataFrame(mycursor.fetchall(), columns=column_names)
        st.write(df)
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT video_id AS Video_ID, comments AS Comments
                        FROM video_details
                        ORDER BY comments DESC
                        LIMIT 10""")
        column_names = [desc[0] for desc in mycursor.description]
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)
    mycursor.close()
    mydb.close()
