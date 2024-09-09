
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

def fetchingDataFromDataCatalog(database,table):
    dynamicDataframe = glueContext.create_dynamic_frame.from_catalog(
        database = database, 
        table_name = table
        )
    return dynamicDataframe.toDF()


dfChannelData = fetchingDataFromDataCatalog('dev','channel_data')
dfChannelData.show(100,False)
dfChannelData.createOrReplaceTempView("channel_data")
spark.sql('select inserted_date,channel_name,subscriber,views,videos from channel_data where playlist_id ="UU0GP1HDhGZTLih7B89z_cTg"').show(10,False)




dfVideoDetails = fetchingDataFromDataCatalog('dev','video_details')
dfVideoDetails.createOrReplaceTempView("video_details")

dfVideoDetails.show(10,False)

spark.sql('select inserted_date,playlist_id,count(1) as videocount,sum(views.long) as viewscount ,\
sum(likes) as likecount,sum(favorite.long) as favoritecount ,sum(comments) as commentcount  \
from video_details group by inserted_date,playlist_id order by inserted_date asc ').show(1000,False)

spark.sql('select a.inserted_date,a.channel_name,a.subscriber,a.views,a.videos,a.playlist_id,\
          b.playlist_id,b.videocount,b.viewscount,b.likecount,b.favoritecount,b.commentcount from channel_data a   inner join (select inserted_date,playlist_id,count(1) as videocount,sum(views.long) as viewscount ,\
sum(likes) as likecount,sum(favorite.long) as favoritecount ,sum(comments) as commentcount  \
from video_details group by inserted_date,playlist_id order by inserted_date asc ) b on a.inserted_date = \
b.inserted_date and a.playlist_id = b.playlist_id ').show(1000,False)





