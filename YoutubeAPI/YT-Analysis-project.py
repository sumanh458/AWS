from googleapiclient.discovery import build 
import pandas as pd 
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
import os
import base64



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

##Function to get get video ids
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

##Function to get video details
def get_video_details(youtube,video_ids):
    all_video_stats =[]
    for i in range(0,len(video_ids),50):
        request = youtube.videos().list(
            part="snippet,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()
        for video in response['items']:
            video_stats =dict(Video_Id =video['id'],
                              Title = video['snippet']['title'],
                              Published_date = video['snippet']['publishedAt'],
                              Views = video['statistics'].get('viewCount'),
                              Likes = video['statistics'].get('likeCount'),
                              Favorite = video['statistics'].get('favoriteCount'),
                              Comments = video['statistics'].get('commentCount')
                              )
            all_video_stats.append(video_stats)
    return all_video_stats

def main():
    ##Initializing Boto3 client to interact with s3
    current_date = str(datetime.today().strftime('%Y-%m-%d'))

    ##Bucket Name
    target_bucket='youtubeanalysisdata'

    ##Suman google api key
    api_key ="QUl6YVN5REI1bmpFMU42eW9Ebzh2OE5kWUZyRzdkc2ZxdHAzQjRF"
    api_key = base64.b64decode(api_key)
    

    ##Youtube channel id
    channel_ids =["UC4p_I9eiRewn2KoU-nawrDg", #Strait Times
                "UC0GP1HDhGZTLih7B89z_cTg", #Business Times
                "UCrbQxu0YkoVWu2dw5b1MzNg", #ZaoBao
                "UC_WgSFSkn7112rmJQcHSUIQ", #Berita Harian
                "UCs0xZ60FSNxFxHPVFFsXNTA" #Tamil Marusu
    ]

    ##Building youtube api object
    youtube =build('youtube','v3',developerKey=api_key)

    ##Calling function for channel statics for prescribe youtube channel id
    channel_data = get_channel_stats(youtube,channel_ids)
    channel_data = pd.DataFrame(channel_data)

    ##Parsing the dataframe to desire datatype
    channel_data['Subscriber'] =pd.to_numeric(channel_data['Subscriber'])
    channel_data['Views'] =pd.to_numeric(channel_data['Views'])
    channel_data['Videos'] =pd.to_numeric(channel_data['Videos'])

    ##To add new column as inserted date for analysis
    #channel_data['Inserted_date']= datetime.today().strftime('%Y-%m-%d')
    print(channel_data)

    ##To store the channel data in s3 location with partitioned value(i.e fetched date)
    channel_data_file_name = 'Youtube_channel_data_'+current_date+'.csv'
    channel_data_sub_folder ='channel_data/inserted_date='+current_date+'/'+channel_data_file_name
    if os.getlogin() in 'Suman Haldar':
        fullpath= os.path.join(os.path.dirname(__file__), 'channel_data/inserted_date='+current_date)
        os.makedirs(fullpath,exist_ok=True)
        channel_data.to_parquet(f"{fullpath}/{channel_data_file_name}", index=False)
    else:
        channel_data.to_csv('s3://'+target_bucket+'/'+channel_data_sub_folder, index=False)

  

    ## To plot thr bar graph for analysing the channel statistic
    '''sns.set(rc={'figure.figsize':(10,8)})
    ax =sns.barplot(x='Channel_name',y='Subscriber',data=channel_data)
    plt.show()
    ax =sns.barplot(x='Channel_name',y='Views',data=channel_data)
    plt.show()
    ax =sns.barplot(x='Channel_name',y='Videos',data=channel_data)
    plt.show()'''

    ##To fetch the playlist_id for defined youtube channel
    for index, row in channel_data.iterrows():
        print("============== Fetching result for "+row['Channel_name']+" channel ==============")
        playlist_id =row['Playlist_id']
    
        ##Function to get all video ids
        video_ids = get_video_ids(youtube,playlist_id)

        ##Function to fetch videos details of each video ids
        video_details = get_video_details(youtube,video_ids)
        video_details = pd.DataFrame(video_details)

        
        video_details['Published_date'] = pd.to_datetime(video_details['Published_date']).dt.date
        video_details['Views'] = pd.to_numeric(video_details['Views'])
        video_details['Likes'] = pd.to_numeric(video_details['Likes'])
        video_details['Favorite'] = pd.to_numeric(video_details['Favorite'])
        video_details['Comments'] = pd.to_numeric(video_details['Comments'])

        ##To add new column ,inserted date for analysis
        #video_details['Inserted_date']= datetime.today().strftime('%Y-%m-%d')
        print(video_details)

        ##To store the video_details data in s3 location with partitioned value(i.e playlist_id and fetched date)
        video_details_file_name = 'Youtube_video_details_'+current_date+'.csv'
        video_details_sub_folder ='video_details/playlist_id='+str(playlist_id)+'/inserted_date='+current_date+'/'+video_details_file_name
        if os.getlogin() in 'Suman Haldar':
            fullpath= os.path.join(os.path.dirname(__file__), 'video_details/'+str(playlist_id)+'/inserted_date='+current_date)
            os.makedirs(fullpath,exist_ok=True)
            video_details.to_csv(f"{fullpath}/{video_details_file_name}", index=False)
        else:
            video_details.to_csv('s3://'+target_bucket+'/'+video_details_sub_folder, index=False)


        '''
        #Simple analysis
        ##Top 10 videos
        top_10_videos =video_details.sort_values(by='Views',ascending=False).head(10)
        print(top_10_videos)

        #bar plot 
        ax1 =sns.barplot(x='Views',y='Title',data=top_10_videos)
        plt.show()

        #in Which month video is posted
        video_details['Month'] = pd.to_datetime(video_details['Published_date']).dt.strftime('%b')
        print('==============')
        print(video_details)

        #to calculate video posted in each month
        videos_per_month =video_details.groupby('Month',as_index=False).size()
        print(videos_per_month)

        sort_order =['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        videos_per_month.index = pd.CategoricalIndex(videos_per_month['Month'],categories = sort_order,ordered=True)
        videos_per_month = videos_per_month.sort_index()
        print(videos_per_month)'''

if __name__=="__main__":
    main()
    





