import json
import boto3
from datetime import datetime

# Configure AWS credentials and region
aws_access_key = 'AKIAZI2LDD35VP'
aws_secret_key = 'DQV4yB7WO3Ri0AiJZxL4l16v7F/'
aws_region = 'ap-south-1'

# Configure SQS queue URL
sqs_queue_url = 'https://sqs.ap-south-1.amazonaws.com/637423263483/project'

# Initialize SQS client
sqs = boto3.client('sqs', region_name=aws_region,aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb', region_name=aws_region)

# Define the DynamoDB table name
dynamodb_table_name = 'capstone3_stock_data'

def lambda_handler(event, context):
    try:
        # Retrieve messages from SQS queue
        response = sqs.receive_message(QueueUrl=sqs_queue_url, MaxNumberOfMessages=10)
        
        if 'Messages' in response:
            for message in response['Messages']:
                # Parse message body
                message_body = json.loads(message['Body'])
                for data in message_body:
                    # Process data
                    processed_data = {
                        'stock_name': data['symbol'],
                        'high': data['dayHigh'],
                        'low': data['dayLow'],
                        'timestamp': str(datetime.now())  # Add current timestamp
                    }
                    
                    # Insert processed data into DynamoDB
                    response = dynamodb.put_item(
                        TableName=dynamodb_table_name,
                        Item={
                            'stock_name': {'S': processed_data['stock_name']},
                            'high': {'N': str(processed_data['high'])},
                            'low': {'N': str(processed_data['low'])},
                            'timestamp': {'S': processed_data['timestamp']}
                        }
                    )
                    
                    # Check if the insertion was successful
                    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                        print('Data inserted into DynamoDB successfully:', processed_data)
                    else:
                        print('Failed to insert data into DynamoDB:', processed_data)
                
                # Delete the message from the queue after processing
                sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=message['ReceiptHandle'])
    except Exception as e:
        print('Error:', str(e))