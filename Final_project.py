import pandas as pd
import streamlit as st
import mysql.connector as sql
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
import plotly.express as pl


# SETTING PAGE CONFIGURATIONS
st.set_page_config(page_title="YouTube Data Harvesting and Warehousing",
                   layout="wide",
                   initial_sidebar_state="expanded")

# CONNECTING WITH MYSQL DATABASE
mydb = sql.connect(
    host="localhost",
    user="root",
    password="root",
    database="youtube_data"
)
mycursor = mydb.cursor(buffered=True)

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyCuRNBLnGy8fazSKEnlgBYAhFFrUt89XAE"
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
            # Format published date
            Published_date = video['snippet']['publishedAt']
            if Published_date:
                Published_date = datetime.strptime(Published_date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
            
            video_details = {
                'Video_id': video['id'],
                'Channel_name': video['snippet']['channelTitle'],
                'Title': video['snippet']['title'],
                'Description': video['snippet'].get('description', ''),
                'Published_Date': Published_date,
                'Views': video['statistics'].get('viewCount', 0),
                'Likes': video['statistics'].get('likeCount', 0),
                'Dislikes': video['statistics'].get('DislikeCount', 0),
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
        try:
            response = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=v_id,
                maxResults=100,
                pageToken=next_page_token
            ).execute()
        except HttpError as e:
            error_content = e.content.decode('utf-8')
            st.error(f"An HTTP error {e.resp.status} occurred: {error_content}")
            if e.resp.status == 400:
                st.error("The API server failed to successfully process the request.")
            elif e.resp.status == 403 and 'commentsDisabled' in error_content:
                st.warning(f"Comments are disabled for video ID: {v_id}")
            else:
                st.error(f"Unexpected error content: {error_content}")
            break
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            break

        if 'items' not in response:
            st.warning(f"No comment items found for video ID: {v_id}")
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
    INSERT INTO video_details (Video_id, Title, Description, Channel_name, Views, Published_Date, Likes, Favorite_count, Comments, Duration, Thumbnail, Caption_status, Dislikes)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
    Title = VALUES(Title), Thumbnail = VALUES(Thumbnail), Description = VALUES(Description), Duration = VALUES(Duration), Views = VALUES(Views), 
    Likes = VALUES(Likes), Comments = VALUES(Comments), Favorite_count = VALUES(Favorite_count), 
    Caption_status = VALUES(Caption_status), Channel_name = VALUES (Channel_name), Published_Date = VALUES(Published_Date), Dislikes = VALUES(Dislikes)
    """
    for detail in vid_details:
        if detail['Published_Date'] and detail['Published_Date'] != '0':
            mycursor.execute(query, (
                detail['Video_id'],
                detail['Title'],
                detail['Description'],
                detail['Channel_name'],
                detail['Views'],
                detail['Published_Date'],
                detail['Likes'],
                detail['Favorite_count'],
                detail['Comments'],
                detail['Duration'],
                detail['Thumbnail'],
                detail['Caption_status'],
                detail['Dislikes']
            ))
        else:
            mycursor.execute(query, (
                detail['Video_id'],
                detail['Title'],
                detail['Description'],
                detail['Channel_name'],
                detail['Views'],
                None,  # Set Published_Date to None if it's invalid
                detail['Likes'],
                detail['Favorite_count'],
                detail['Comments'],
                detail['Duration'],
                detail['Thumbnail'],
                detail['Caption_status'],
                detail['Dislikes']
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
selected = st.sidebar.selectbox("Navigation જ⁀➴", ["☰ Home", "📥 Extract and Transform","🗃️ Data Bank", "📉 Query"])
with st.sidebar:
    st.header(":red[YouTube Data Extraction]")
    st.subheader(":blue[Objective]")
    st.markdown("This project demonstrates harvesting data from YouTube via the YouTube Data API and storing it in a MySQL database. It collects channel, video, and comment information for analysis and retrieval.")  

# HOME PAGE
if selected == "☰ Home":
   
    st.header(':red[YouTube] Data Harvesting', divider='grey')
    st.image('yt log.gif')
    st.toast('Created by Yabase Immanuel', icon='🤖')


# EXTRACT AND TRANSFORM
if selected == "📥 Extract and Transform":
    st.header("Extract and Transform :red[YouTube] Data", divider='grey')
    st.subheader("Enter YouTube Channel ID below:")
    ch_id = st.text_input("Hint: Go to channel's home page > More about this channel > Share channel > Copy Channel ID")

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
 
 
 # Over all data
if selected == "🗃️ Data Bank":
    st.title(":green[Data Warehouse Zone]")
    st.write("The collected data is stored for further analysis.")
    # Display channel details
    st.header("Channel Details")
    mycursor.execute("SELECT * FROM Channel_details")
    ch_data = mycursor.fetchall()
    ch_df = pd.DataFrame(ch_data, columns=[desc[0] for desc in mycursor.description])
    st.write(ch_df)
    
    # Display video details
    st.header("Video Details")
    mycursor.execute("SELECT * FROM Video_details")
    vid_data = mycursor.fetchall()
    vid_df = pd.DataFrame(vid_data, columns=[desc[0] for desc in mycursor.description])
    st.write(vid_df)
    
    # Display comment details
    st.header("Comment Details")
    mycursor.execute("SELECT * FROM Comment_details")
    comm_data = mycursor.fetchall()
    comm_df = pd.DataFrame(comm_data, columns=[desc[0] for desc in mycursor.description])
    st.write(comm_df)
    
# Query Page
if selected == "📉 Query":
    st.header(':orange[YOUTUBE DATA HARVESTING ANALYSIS]', divider='grey')
    questions = st.selectbox('Analysis About :',
    [
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?']
    )
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT Title AS Video_Title, Channel_name as Channel_name FROM video_details""")
        column_names = [desc[0] for desc in mycursor.description]
        df1 = pd.DataFrame(mycursor.fetchall(),columns=column_names)
       
        st.write(df1)
    
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT Channel_name 
        AS Channel_Name, Total_videos AS Total_Videos
                            FROM channel_details
                            ORDER BY total_videos DESC""")
        column_names = [desc[0] for desc in mycursor.description]
        df2 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df2)
        
        yab = pl.bar(df2, x = "Channel_Name", y = "Total_Videos",title = "Channels with the Highest number of Videos")
        #Update layout
        yab.update_layout(
            xaxis_title="Channel",
            yaxis_title="Number of Videos")
        #display the chart using Streamlit
        st.write("Graphical Demonstration")
        st.plotly_chart(yab)

    
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT Channel_name, Title AS Video_Title, Views AS Views 
                            FROM video_details
                            ORDER BY views DESC
                            LIMIT 10""")
        column_names = [desc[0] for desc in mycursor.description]
        df3 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df3)
          
        yab = pl.bar(df3, x = "Video_Title", y = "Views", title = "Top 10 Videos with the Highest Views by Channel")
        yab.update_layout(
        xaxis_title="Video_Title",
        yaxis_title="Views")
        st.write("Graphical Demonstration")
        st.plotly_chart(yab)
    
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.Video_id AS Video_id, a.Title AS Video_Title, b.Total_Comments, Channel_name
                            FROM video_details AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comment_details GROUP BY Video_id) AS b
                            ON a.Video_id = b.Video_id
                            ORDER BY b.Total_Comments DESC""")
        column_names = [desc[0] for desc in mycursor.description]
        df4 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df4)
          
    
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT title AS Title,likes AS Likes, Channel_name as Channel_name
                            FROM video_details 
                            ORDER BY likes DESC
                            LIMIT 10""")
        column_names = [desc[0] for desc in mycursor.description]
        df5 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df5)
        
        yab = pl.bar(df5, x = "Title", y = "Likes",color = "Channel_name", title = "Videos with the Highest Likes by Channel")
        yab.update_layout(
        xaxis_title="Video Title",
        yaxis_title="Like Count", legend_title = "Channel Name")
        st.write("Graphical Demonstration")
        st.plotly_chart(yab)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT sum(Likes) as Likes, count(Dislikes) as Dislikes, Title as Title From video_details group by Title""")
        column_names = [desc[0] for desc in mycursor.description]
        df6 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df6)
         
    
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_name, views AS Views
                            FROM channel_details
                            ORDER BY views DESC""")
        column_names = [desc[0] for desc in mycursor.description]
        df7 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df7)
        
        yab = pl.bar(df7, x = "Channel_name", y = "Views", title = "Total Number of Views for Each Channel")
        yab.update_layout(
        xaxis_title="Channel Name",
        yaxis_title="Total Number of Views")
        st.write("Graphical demonstration") 
        st.plotly_chart(yab)
        
    
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""select title as video_title,Published_Date as video_release,Channel_name as Channel_name from video_details
                where extract(year from Published_date)=2022""")
    
        column_names = [desc[0] for desc in mycursor.description]
        df8 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df8)
        
        yab = pl.histogram(df8, x = "Channel_name", title = "Channels with Published Videos in 2022")
        yab.update_layout(
        xaxis_title="Channel Name",
        yaxis_title="Frequency")
        st.write("Graphical Demonstration")
        st.plotly_chart(yab)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""
            SELECT AVG( CASE WHEN Duration REGEXP '^PT([0-9]+H)?([0-9]+M)?([0-9]+S)?$' THEN IFNULL( TIME_TO_SEC(
                                CONCAT(
                                    IF(LOCATE('H', Duration) > 0, SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'H', 1), 'T', -1), '0'),
                                    ':',
                                    IF(LOCATE('M', Duration) > 0, SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'M', 1), 'H', -1), 'T', -1), '0'),
                                    ':',
                                    IF(LOCATE('S', Duration) > 0, SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'S', 1), 'M', -1), '0'))), 0) ELSE 0 END) AS average_duration_seconds,Channel_name as Channel_name FROM video_details group by Channel_name""")
        column_names = [desc[0] for desc in mycursor.description]
        df9 = pd.DataFrame(mycursor.fetchall(), columns=column_names)
        st.write(df9)
        
        yab = pl.bar(df9, x = "Channel_name", y = "average_duration_seconds", title = "Average Duration of Videos by Channel")
        yab.update_layout(
        xaxis_title = "Channel Name",
        yaxis_title = "Average Duration")
        st.write("Graphical Demonstration")
        st.plotly_chart(yab)
    
    
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT a.Video_id AS Video_id, a.Title AS Video_Title, b.Total_Comments, Channel_name
                            FROM video_details AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comment_details GROUP BY Video_id) AS b
                            ON a.Video_id = b.Video_id
                            ORDER BY b.Total_Comments DESC limit 20""")
        column_names = [desc[0] for desc in mycursor.description]
    df10 = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df10)
    
    mycursor.close()
    mydb.close()