import boto3
from datetime import datetime, timedelta
import math
import os

# Variables
logGroupName = os.environ['LOG_GROUP'] # Log Group name without first / (eg. aws-glue/crawlers)
taskName = os.environ['TASK_NAME']
bucket = os.environ['BUCKET_NAME']

def lambda_handler(event, context):
    client = boto3.client('logs')

    now = datetime.utcnow() - timedelta(hours=1)

    startOfHour = now.replace(minute=0, second=0, microsecond=0)
    endOfHour = now.replace(minute=59, second=59, microsecond=999999)

    response = client.create_export_task(
        taskName=taskName,
        logGroupName='/{}'.format(logGroupName),
        fromTime=math.floor(startOfHour.timestamp() * 1000),
        to=math.floor(endOfHour.timestamp() * 1000),
        destination=bucket,
        destinationPrefix='{}/{}/{}/{}/{}'.format(logGroupName,now.year,now.month,now.day,now.hour)
    )