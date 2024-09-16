## Code to load the data in enriched layer from dev layer

# Import the modules required for the program
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime


# Intitiate the SparkContext,glueContext and SparkSession
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

''' Function to get data from Data Catalog
    Return the Dataframe of the desire table'''
def fetchingDataFromDataCatalog(database,table):
    dynamicDataframe = glueContext.create_dynamic_frame.from_catalog(
        database = database, 
        table_name = table
        )
    return dynamicDataframe.toDF()

##Function to write the data to S3 location and update the Glue catalog table
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


## Calculating current date
current_date = datetime.today().strftime('%Y-%m-%d')

## Fetching data of Channel Data 
dfChannelData = fetchingDataFromDataCatalog('dev','channel_data')

## Fetching data of Video Details 
dfVideoDetails = fetchingDataFromDataCatalog('dev','video_details')

try:
    ## Fetching maximum Inserted date for enriched.channel_data and enriched.video_details table 
    maxChannelData = spark.sql("select max(inserted_date) from enriched.channel_data").distinct().rdd.flatMap(lambda x: x).collect()[0]
    maxVideodetails = spark.sql("select max(inserted_date) from enriched.video_details").distinct().rdd.flatMap(lambda x: x).collect()[0]

    ## Calculating minimun date from enriched.channel_data and enriched.video_details table fro incremental load
    maxDate = min(maxChannelData,maxVideodetails)
except:
    maxDate='2020-01-01'

## Condition for incremental load to occur
if maxDate != current_date:
    ## Filtering data on the basis od inserted_date with incremental inserted data to load to enriched layer
    dfChannelData.where("inserted_date > '"+maxDate+"'").createOrReplaceTempView("channel_data")
    dfVideoDetails.where("inserted_date > '"+maxDate+"'").createOrReplaceTempView("video_details")

    ## Writing the Channel data to Glue catalog table enriched.channel_data
    ChannelDataDynamicFrame = DynamicFrame.fromDF(dfChannelData, glueContext, "ChannelDataDynamicFrame")
    writeS3Parquet('enriched','channel_data',['inserted_date'],ChannelDataDynamicFrame)

    ## Transforming the Video_details to contain playlist_id,video_id,title and inserted_date,published_date columns
    video_details = spark.sql('select playlist_id,video_id,title,inserted_date,published_date from video_details')

    ## Writing the Video_details data to Glue catalog table enriched.video_details
    video_details_DynamicFrame = DynamicFrame.fromDF(video_details, glueContext, "video_details")
    writeS3Parquet('enriched','video_details',['playlist_id','inserted_date'],video_details_DynamicFrame)

    ## Transforming the Video_details to contain views,likes,favorite,comments,video_id and inserted_date columns
    video_details_dly = spark.sql('select views.long as views, likes,favorite.long as favorite,comments,video_id,inserted_date from video_details')

    ## Writing the Video_details_dly data to Glue catalog table enriched.video_details_dly
    video_details_dly_DynamicFrame = DynamicFrame.fromDF(video_details_dly, glueContext, "video_details_dly")
    writeS3Parquet('enriched','video_details_dly',['inserted_date'],video_details_dly_DynamicFrame)

    ## Transforming the Video_details and Channel data to generate enriched.channel_data_mnthly table
    channel_data_mnthly = spark.sql('select c.channel_name,v.playlist_id,sum(v.views.long) as totalviews, \
    sum(v.likes) as totallikes ,sum(v.favorite.long) as \
    totalfavorite,sum(v.comments) as totalcomments , int(date_format(v.published_date, "yyyyMM")) as published_month  from video_details v left join channel_data c \
    on v.playlist_id = c.playlist_id and v.inserted_date = c.inserted_date group by c.channel_name,v.playlist_id,date_format(v.published_date, "yyyyMM")  ')

    ## Writing the channel_data_mnthly data to Glue catalog table enriched.channel_data_mnthly
    channel_data_mnthly_DynamicFrame = DynamicFrame.fromDF(channel_data_mnthly, glueContext, "channel_data_mnthly")
    writeS3Parquet('enriched','channel_data_mnthly',['channel_name','published_month'],channel_data_mnthly_DynamicFrame)

else:
    print("================ Nothing to move to enriched layer ================")
    print("================ All Data is already present in the enriched layer ================")

job.commit()