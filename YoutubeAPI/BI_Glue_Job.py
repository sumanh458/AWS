
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
spark.sql("select from_unixtime(unix_timestamp('20150101' ,'yyyyMMdd'), 'yyyy-MM-dd') ,\
current_date,date_format(from_unixtime(unix_timestamp('20240101' ,'yyyyMMdd'), 'yyyy-MM-dd'),'MMM yyyy') as MMMM_yyyy").show(10,False)
channel_data_mnthly_Top10 = spark.sql("select channel_name,(totalviews/1000) as totalviews,published_month,\
date_format(from_unixtime(unix_timestamp(published_month||'01' ,'yyyyMMdd'), 'yyyy-MM-dd'),'MMM yyyy')  as publishedmonthyear\
 from enriched.channel_data_mnthly where  published_month between \
date_format(current_date -INTERVAL '11' month,'yyyyMM') and date_format(current_date, 'yyyyMM')  order by published_month asc  \
")
channel_data_mnthly_Top10.show(10,False)
import matplotlib.pyplot as plt
x = channel_data_mnthly_Top10.where("channel_name = 'The Straits Times'").select('publishedmonthyear').rdd.flatMap(lambda x: x).collect()
x
y= channel_data_mnthly_Top10.where("channel_name = 'The Straits Times'").select('totalviews').rdd.flatMap(lambda x: x).collect()
y
fig = plt.figure(figsize = (10, 5))

# creating the bar plot
plt.bar(x, y, color ='maroon', 
        width = 0.5)

plt.xlabel(" Year month ")
plt.ylabel("No. of views (in K)")
plt.title("Views in channel 'The Straits Times'")
plt.xticks(rotation=15)
plt.show()
plt.clf()
plt.clf()
x = channel_data_mnthly_Top10.where("channel_name = 'Tamil Murasu'").select('publishedmonthyear').rdd.flatMap(lambda x: x).collect()
y= channel_data_mnthly_Top10.where("channel_name = 'Tamil Murasu'").select('totalviews').rdd.flatMap(lambda x: x).collect()
fig = plt.figure(figsize = (10, 5))

# creating the bar plot
plt.bar(x, y, color ='maroon', 
        width = 0.4)

plt.xlabel(" Year month ")
plt.ylabel("No. of views (in K)")
plt.title("Views in channel 'Tamil Murasu'")
plt.xticks(rotation=15)
plt.show()
plt.clf()

x = channel_data_mnthly_Top10.where("channel_name = 'The Business Times'").select('publishedmonthyear').rdd.flatMap(lambda x: x).collect()
y= channel_data_mnthly_Top10.where("channel_name = 'The Business Times'").select('totalviews').rdd.flatMap(lambda x: x).collect()
fig = plt.figure(figsize = (10, 5))

# creating the bar plot
plt.bar(x, y, color ='maroon', 
        width = 0.4)

plt.xlabel(" Year month ")
plt.ylabel("No. of views (in K)")
plt.title("Views in channel 'The Business Times'")
plt.xticks(rotation=15)
plt.show()
plt.clf()
x = channel_data_mnthly_Top10.where("channel_name = 'Berita Harian Singapura'").select('publishedmonthyear').rdd.flatMap(lambda x: x).collect()
y= channel_data_mnthly_Top10.where("channel_name = 'Berita Harian Singapura'").select('totalviews').rdd.flatMap(lambda x: x).collect()
fig = plt.figure(figsize = (10, 5))

# creating the bar plot
plt.bar(x, y, color ='maroon', 
        width = 0.4)

plt.xlabel(" Year month ")
plt.ylabel("No. of views (in K)")
plt.title("Views in channel 'Berita Harian Singapura'")
plt.xticks(rotation=15)
plt.show()

plt.clf()
x = channel_data_mnthly_Top10.where("channel_name = 'zaobaosg'").select('publishedmonthyear').rdd.flatMap(lambda x: x).collect()
y= channel_data_mnthly_Top10.where("channel_name = 'zaobaosg'").select('totalviews').rdd.flatMap(lambda x: x).collect()
fig = plt.figure(figsize = (10, 5))

# creating the bar plot
plt.bar(x, y, color ='maroon', 
        width = 0.4)

plt.xlabel(" Year month ")
plt.ylabel("No. of views (in K)")
plt.title("Views in channel 'zaobaosg'")
plt.xticks(rotation=15)
plt.show()
###########################Channel subscriber #############
channel_data_sub_view_videos_trnd= spark.sql("select channel_name,(subscriber/100) as subscriber_count,(views/10000) as view_count,videos as videos_count,inserted_date \
from enriched.channel_data  order by inserted_date asc")
spark.sql("select channel_name,(subscriber/100) as subscriber_count,(views/10000) as view_count,videos as videos_count,inserted_date \
from enriched.channel_data  where channel_name = 'zaobaosg' order by inserted_date asc").show(10,False)
plt.clf()
import numpy as np 
x = channel_data_sub_view_videos_trnd.where("channel_name = 'zaobaosg'").select('inserted_date').rdd.flatMap(lambda x: x).collect()
y1 = channel_data_sub_view_videos_trnd.where("channel_name = 'zaobaosg'").select('subscriber_count').rdd.flatMap(lambda x: x).collect()
y2 = channel_data_sub_view_videos_trnd.where("channel_name = 'zaobaosg'").select('view_count').rdd.flatMap(lambda x: x).collect()
y3 = channel_data_sub_view_videos_trnd.where("channel_name = 'zaobaosg'").select('videos_count').rdd.flatMap(lambda x: x).collect()

ind = np.arange(len(x) ) 
width = 0.25

bar1 = plt.bar(ind, y1, width, color = 'r') 
bar2 = plt.bar(ind+width, y2, width, color='g') 
bar3 = plt.bar(ind+width*2, y3, width, color = 'b') 

plt.xlabel("Inserted date")
plt.ylabel("Value")
plt.title('Trend analysis for channel_name zaobaosg')
plt.xticks(ind+width,x) 
plt.legend( (bar1, bar2, bar3), ('Subscriber', 'Views', 'Videos') )

plt.show()
##########How many subscribers are there for each channel?####
spark.sql("select channel_name,subscriber from (select channel_name,subscriber, inserted_date,\
dense_rank( ) over(partition by channel_name order by inserted_date desc) as rank  from enriched.channel_data) temp where temp.rank =1 ").show(10, False)
channel_data_subscriber = spark.sql("select channel_name,subscriber from (select channel_name,subscriber, inserted_date,\
dense_rank( ) over(partition by channel_name order by inserted_date desc) as rank  from enriched.channel_data) temp where temp.rank =1 ")
channel_data_subscriber.show(10,False)
plt.clf()
sub_data = channel_data_subscriber.select('subscriber').rdd.flatMap(lambda x: x).collect()
channel_name_sub = channel_data_subscriber.select('channel_name').rdd.flatMap(lambda x: x).collect()
y = np.array(sub_data)
mylabels = channel_name_sub

plt.pie(y, labels = mylabels)
plt.show() 
#############How many video’s have been published for each channel##########
channel_data_video_count = spark.sql("select c.channel_name, v.playlist_id,count(video_id) as video_count  from (select * from enriched.video_details \
where inserted_date = (select distinct max(inserted_date) from enriched.video_details)) v left join \
(select * from enriched.channel_data where inserted_date = (select distinct max(inserted_date) from enriched.channel_data)) c on v.inserted_Date = \
c.inserted_date and c.playlist_id =\
v.playlist_id group by c.channel_name,v.playlist_id  ")
channel_data_video_count.show(10,False)
plt.clf()
video_count_view = channel_data_video_count.select('video_count').rdd.flatMap(lambda x: x).collect()
channel_name_view = channel_data_video_count.select('channel_name').rdd.flatMap(lambda x: x).collect()
y = np.array(video_count_view)
mylabels = channel_name_view

plt.pie(y, labels = mylabels)
plt.show() 
#############What is the trend for videos published by each channel over the last 12 months?###########
channel_video_trnd = spark.sql("select t.channel_name,t.video_counts,t.published_month, \
date_format(from_unixtime(unix_timestamp(t.published_month||'01' ,'yyyyMMdd'), 'yyyy-MM-dd'),'MMM yyyy')  as publishedmonthyear \
from (select c.channel_name,int(date_format(v.published_date, 'yyyyMM')) as published_month ,count(1) as video_counts from enriched.video_details v left join \
enriched.channel_data c on v.playlist_id = c.playlist_id and v.inserted_date = c.inserted_date where \
v.published_date between current_date -INTERVAL '11' month and current_date \
group by c.channel_name,int(date_format(v.published_date, 'yyyyMM')) order by c.channel_name asc ,int(date_format(v.published_date, 'yyyyMM')) asc) t  ")
channel_video_trnd.show(10000, False)
plt.clf()
x = channel_video_trnd.select('publishedmonthyear').distinct().rdd.flatMap(lambda x: x).collect()
y1 = channel_video_trnd.where('channel_name = "zaobaosg"').select('video_counts').rdd.flatMap(lambda x: x).collect()
y2 = channel_video_trnd.where('channel_name = "The Straits Times"').select('video_counts').rdd.flatMap(lambda x: x).collect()
y3 = channel_video_trnd.where('channel_name = "The Business Times"').select('video_counts').rdd.flatMap(lambda x: x).collect()
y4 = channel_video_trnd.where('channel_name = "Berita Harian Singapura"').select('video_counts').rdd.flatMap(lambda x: x).collect()
y5 = channel_video_trnd.where('channel_name = "Tamil Murasu"').select('video_counts').rdd.flatMap(lambda x: x).collect()

plt.plot(x, y1,label="zaobaosg")
plt.plot(x, y2,'-.',label="The Straits Times")
plt.plot(x, y3,label="The Business Times")
plt.plot(x, y4,'-.',label="Berita Harian Singapura")
plt.plot(x, y5,label="Tamil Murasu")

plt.xlabel("Month Year")
plt.ylabel("Values")
plt.title('Videos count for Channel')
plt.legend()
plt.xticks(rotation=15)
plt.show()
#######Which are the most viewed videos?#############
most_video_details_channel = spark.sql("select c.channel_name,temp.views, temp.video_id from (select v.*,c.playlist_id,dense_rank() over(partition by c.playlist_id \
order by v.views desc) as rank from (select * from enriched.video_details_dly where \
inserted_date =(select max(inserted_date) from enriched.video_details_dly ) ) v left join (select * from enriched.video_details where \
inserted_date =(select max(inserted_date) from enriched.video_details )) c on \
c.video_id = v.video_id and c.inserted_date = v.inserted_date) temp  left join \
(select * from enriched.channel_data where inserted_date =(select max(inserted_date ) from enriched.channel_data \
)) c on temp.playlist_id = c.playlist_id where temp.rank =1  ")
most_video_details_channel.show(10,False)
job.commit()