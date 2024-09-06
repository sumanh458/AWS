from googleapiclient.discovery import build 
import pandas as pd 
import seaborn as sns
import matplotlib.pyplot as plt


api_key="AIzaSyDB5njE1N6yoDo8v8NdYFrG7dsfqtp3B4E"

#channel_id="UC0GP1HDhGZTLih7B89z_cTg"

channel_ids =["UC4p_I9eiRewn2KoU-nawrDg", #Strait Times
              "UC0GP1HDhGZTLih7B89z_cTg", #Business Times
              "UCrbQxu0YkoVWu2dw5b1MzNg", #ZaoBao
              "UCs0xZ60FSNxFxHPVFFsXNTA", #Tamil Marusu
              "UC_WgSFSkn7112rmJQcHSUIQ" #Berita Harian
]

youtube =build('youtube','v3',developerKey=api_key)


##Function to get Channel Statistics
def get_channel_stats(youtube,channel_ids):
    all_data =[]
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id=','.join(channel_ids))
    response = request.execute()
    for i in range(len(response['items'])):
        data = dict(Channel_name =response['items'][i]['snippet']['title'],
                    Subscriber =response['items'][i]['statistics']['subscriberCount'],
                    Views =response['items'][i]['statistics']['viewCount'],
                    Videos =response['items'][i]['statistics']['videoCount'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads']
                    )
        all_data.append(data)
    return all_data

channel_statistics = get_channel_stats(youtube,channel_ids)
#print(channel_statistics)

channel_data = pd.DataFrame(channel_statistics)
print(channel_data)
channel_data['Subscriber'] =pd.to_numeric(channel_data['Subscriber'])
channel_data['Views'] =pd.to_numeric(channel_data['Views'])
channel_data['Videos'] =pd.to_numeric(channel_data['Videos'])

# To plot thr bar graph for analysis
#sns.set(rc={'figure.figsize':(10,8)})
'''ax =sns.barplot(x='Channel_name',y='Subscriber',data=channel_data)
plt.show()
ax =sns.barplot(x='Channel_name',y='Views',data=channel_data)
plt.show()
ax =sns.barplot(x='Channel_name',y='Videos',data=channel_data)
plt.show()'''

playlist_id =channel_data.loc[channel_data['Channel_name'] =='Tamil Murasu','Playlist_id'].iloc[0]

##Function to get video ids
def get_video_ids (youtube,playlist_id):
    request =youtube.playlistItems().list(
        part='contentDetails',
        playlistId= playlist_id,
        maxResults =50
    )
    video_ids =[]
    response =request.execute()
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
    next_page_token =response.get('nextPageToken')
    more_pages =True
    while more_pages:
        if next_page_token is None:
            more_pages =False
        else:
            request =youtube.playlistItems().list(
            part='contentDetails',
            playlistId= playlist_id,
            maxResults =50,pageToken =next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            next_page_token = response.get('nextPageToken')
    return video_ids


video_ids = get_video_ids(youtube,playlist_id)
#print(video_ids)

def get_video_details(youtube,video_ids):
    all_video_stats =[]
    for i in range(0,len(video_ids),50):
        request = youtube.videos().list(
            part="snippet,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()
        for video in response['items']:
            video_stats =dict(Title = video['snippet']['title'],
                              Published_date = video['snippet']['publishedAt'],
                              Views = video['statistics']['viewCount'],
                              Likes = video['statistics']['likeCount'],
                              Favorite = video['statistics']['favoriteCount'],
                              Comments = video['statistics']['commentCount']
                              )
            all_video_stats.append(video_stats)
    return all_video_stats

all_video_stats = get_video_details(youtube,video_ids)
#print(all_video_stats)
all_video_stats = pd.DataFrame(all_video_stats)
print(all_video_stats)