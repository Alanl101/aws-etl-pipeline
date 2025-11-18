import json
import boto3
import requests
from datetime import datetime

dynamodb = boto3.client('dynamodb')
kinesis = boto3.client('kinesis')

DYNAMODB_TABLE = 'ChicagoDateState'
KINESIS_STREAM = 'chicago-stream-crime'
CHICAGO_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit=5000"


def lambda_handler(event, context):
    # Get LAST_PROCESSED date for incremental loading from DynamoDB
    try:
        last_processed_response = dynamodb.get_item(
            TableName=DYNAMODB_TABLE,
            Key={
                'DateKey': {'S': 'LAST_PROCESSED'}
            }
        )

        # Check if item exists
        if 'Item' not in last_processed_response:
            print("No last processed date found, processing all records")
            last_processed_dt = datetime.min
        else:
            last_processed_str = last_processed_response['Item']['LastDateTime']['S']
            last_processed_dt = datetime.strptime(last_processed_str, "%Y-%m-%d %H:%M:%S")
            print(f"Last processed date: {last_processed_dt}")
    except Exception as e:
        print(f"Error retrieving last processed date: {e}")
        raise

    # Load Data from CHICAGO_URL
    try:
        resp = requests.get(CHICAGO_URL, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        print(f"Fetched {len(data)} records from Chicago dataset")
    except requests.exceptions.Timeout:
        print("Timeout Occurred when calling Chicago API")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
    except ValueError as e:
        print(f"JSON decoding failed: {e}")
        raise

    # Get data after the last_processed_date
    new_records = []
    latest_date = last_processed_dt

    for record in data:
        if 'updated_on' in record:
            record_dt = datetime.strptime(record['updated_on'], "%Y-%m-%dT%H:%M:%S.%f")
            if record_dt > last_processed_dt:
                new_records.append(record)
                if record_dt > latest_date:
                    latest_date = record_dt

    print(f"Found {len(new_records)} new records to process")

    # Send new records to Kinesis
    for record in new_records:
        try:
            kinesis.put_record(
                StreamName=KINESIS_STREAM,
                Data=json.dumps(record),
                PartitionKey=str(record.get('id', 'default'))
            )
        except Exception as e:
            print(f"Error sending record to Kinesis: {e}")
            raise

    # Update last processed date in DynamoDB
    if new_records:
        try:
            dynamodb.put_item(
                TableName=DYNAMODB_TABLE,
                Item={
                    'DateKey': {'S': 'LAST_PROCESSED'},
                    'LastDateTime': {'S': latest_date.strftime("%Y-%m-%d %H:%M:%S")}
                }
            )
            print(f"Updated last processed date to: {latest_date}")
        except Exception as e:
            print(f"Error updating last processed date: {e}")
            raise

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
