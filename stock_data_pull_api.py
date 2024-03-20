from botocore.vendored import requests
import boto3
import json
import time
import urllib.parse
import urllib.request


# Configure AWS credentials and region
aws_access_key = 'AKIAZI2LDD35VP'
aws_secret_key = 'DQV4yB7WO3Ri0AiJZxL4l16v7F/'
aws_region = 'ap-south-1'

# Configure Rapid API key and host
rapid_api_key = 'd6f1608857msh69fd8db33e5972dp1a66abjsnb39c9d8485e7'
rapid_api_host = 'latest-stock-price.p.rapidapi.com'
rapid_api_endpoint = 'https://latest-stock-price.p.rapidapi.com/price'
query_string = {"Indices":"NIFTY 50"}

# Configure SQS queue URL
sqs_queue_url = 'https://sqs.ap-south-1.amazonaws.com/637423263483/project'

# Initialize SQS client
sqs = boto3.client('sqs', region_name=aws_region, aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

def fetch_stock_data():
    try:
        url = rapid_api_endpoint + '?' + urllib.parse.urlencode(query_string)
        req = urllib.request.Request(url)
        req.add_header('X-RapidAPI-Key', rapid_api_key)
        req.add_header('X-RapidAPI-Host', rapid_api_host)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
        return data
    except Exception as e:
        print(f"Error fetching stock data: {e}")
        return None
    
def send_to_sqs(stock_data):
    try:
        if stock_data:
            sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=json.dumps(stock_data))
            print("Data sent to SQS:", stock_data)
            return True
    except Exception as e:
        print(f"Error sending data to SQS: {e}")
        return False

def lambda_handler(event, context):
    for i in range(10):  # while True: will be used for continuous fetch
        # Fetch stock data
        stock_data = fetch_stock_data()

        # Send data to SQS queue
        if stock_data:
            if send_to_sqs(stock_data):
                print("Data transmission successful.")
            else:
                print("Data transmission failed.")

        time.sleep(60)  # Fetch data every 1 minute