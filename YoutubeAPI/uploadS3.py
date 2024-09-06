import boto3

client =boto3.client('s3')

target_bucket='youtubeanalysisdata'
sub_folder ='test/Youtube_channel_data_2024-09-03.csv'

client.put_object(Bucket=target_bucket,Key=sub_folder)

