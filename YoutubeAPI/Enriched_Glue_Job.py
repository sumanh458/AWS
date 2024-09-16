import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime

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

def writeS3Parquet(catalogDatabase,catalogTableName,partitionKeys,sourceDynamicframe):
    writeS3PARQUET = glueContext.getSink(connection_type= 's3', 
                                     path='s3://youtubeanalysisdataenriched/'+catalogTableName+'/', 
                                     updateBehavior = "UPDATE_IN_DATABASE",
                                     enableUpdateCatalog= True,
                                     partitionKeys= partitionKeys,
                                     transformation_ctx="writeS3PARQUET"
                                    )
    writeS3PARQUET.setCatalogInfo(catalogDatabase=catalogDatabase, catalogTableName=catalogTableName)
    writeS3PARQUET.setFormat("glueparquet")
    writeS3PARQUET.writeFrame(sourceDynamicframe)

    

current_date = datetime.today().strftime('%Y-%m-%d')

dfChannelData = fetchingDataFromDataCatalog('dev','channel_data')
dfVideoDetails = fetchingDataFromDataCatalog('dev','video_details')

try:
    maxChannelData = spark.sql("select max(inserted_date) from enriched.channel_data").distinct().rdd.flatMap(lambda x: x).collect()[0]
    maxVideodetails = spark.sql("select max(inserted_date) from enriched.video_details").distinct().rdd.flatMap(lambda x: x).collect()[0]
    maxDate = min(maxChannelData,maxVideodetails)
except:
    maxDate='2020-01-01'

if maxDate != current_date:
    dfChannelData.where("inserted_date > '"+maxDate+"'").createOrReplaceTempView("channel_data")
    dfVideoDetails.where("inserted_date > '"+maxDate+"'").createOrReplaceTempView("video_details")


    ChannelDataDynamicFrame = DynamicFrame.fromDF(dfChannelData, glueContext, "ChannelDataDynamicFrame")
    writeS3Parquet('enriched','channel_data',['inserted_date'],ChannelDataDynamicFrame)


    video_details = spark.sql('select playlist_id,video_id,title,inserted_date,published_date from video_details')
    video_details_DynamicFrame = DynamicFrame.fromDF(video_details, glueContext, "video_details")
    writeS3Parquet('enriched','video_details',['playlist_id','inserted_date'],video_details_DynamicFrame)



    video_details_dly = spark.sql('select views.long as views, likes,favorite.long as favorite,comments,video_id,inserted_date from video_details')
    video_details_dly_DynamicFrame = DynamicFrame.fromDF(video_details_dly, glueContext, "video_details_dly")
    writeS3Parquet('enriched','video_details_dly',['inserted_date'],video_details_dly_DynamicFrame)




    channel_data_mnthly = spark.sql('select c.channel_name,v.playlist_id,sum(v.views.long) as totalviews, \
    sum(v.likes) as totallikes ,sum(v.favorite.long) as \
    totalfavorite,sum(v.comments) as totalcomments , int(date_format(v.published_date, "yyyyMM")) as published_month  from video_details v left join channel_data c \
    on v.playlist_id = c.playlist_id and v.inserted_date = c.inserted_date group by c.channel_name,v.playlist_id,date_format(v.published_date, "yyyyMM")  ')

    channel_data_mnthly_DynamicFrame = DynamicFrame.fromDF(channel_data_mnthly, glueContext, "channel_data_mnthly")
    writeS3Parquet('enriched','channel_data_mnthly',['channel_name','published_month'],channel_data_mnthly_DynamicFrame)
else:
    print("================ Nothing to move to enriched layer ================")
    print("================ All Data is already present in the enriched layer ================")



job.commit()