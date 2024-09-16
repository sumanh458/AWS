import json
import boto3


glueClient = boto3.client('glue')
def lambda_handler(event, context):
    folderName = event['Records'][0]['s3']['object']['key'].split('/')[0]
    if folderName =="channel_data":
        response = glueClient.start_crawler(Name="channel_data_crawler")
    elif folderName =="video_details":
        response = glueClient.start_crawler(Name="video_details_crawler")
    print(json.dumps(response,indent=4))
