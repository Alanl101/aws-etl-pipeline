import json
import base64
import boto3
from datetime import datetime


def lambda_handler(event, context):

    s3 = boto3.client("s3")

    BUCKET = "nicholas-tc-bucket"
    PREFIX = "bronze/"

    batch = []

    # 1. Decode incoming Kinesis records
    for record in event["Records"]:
        payload = base64.b64decode(record["kinesis"]["data"])
        json_record = json.loads(payload)
        batch.append(json_record)

    if not batch:
        return {"statusCode": 200, "body": "No records"}

    # 2. Build S3 object key (partitioned by date)
    now = datetime.utcnow()
    key = (
        f"{PREFIX}"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"batch_{now.strftime('%H%M%S')}.json"
    )

    # 3. Upload batch to S3
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(batch),
    )

    return {
        "statusCode": 200,
        "body": json.dumps("Success"),
    }
