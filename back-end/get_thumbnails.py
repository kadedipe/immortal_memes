import json
import boto3
import base64
from botocore.exceptions import ClientError
import io
import time

def lambda_handler(event, context):

    # get the S3 service resource
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("adcolaps-quantic-im-memes")

    # get all meme entries from the database
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("im-memes")
    db_memes = table.scan()
    memes = db_memes["Items"]

    time_now = int(time.time())
    thumbnails = []

    for meme in memes:

        # skip if expired
        if time_now > int(meme["timeToDie"]):
            continue

        thumbnail = {
            "timeToLive": int(meme["timeToDie"]) - time_now,
            "timePosted": int(meme["timePosted"]),
            "userName": meme["userName"],
            "id": meme["id"]
        }

        # load image from S3
        with io.BytesIO() as in_mem_file:
            try:
                bucket.download_fileobj(f"thumbnails/{meme['id']}", in_mem_file)

            except ClientError as error:
                if error.response["Error"]["Code"] == "404":
                    continue
                else:
                    raise error

            # convert image to base64 data URL
            thumbnail["imageUrl"] = (
                "data:image/jpeg;base64,"
                + base64.b64encode(in_mem_file.getvalue()).decode("utf-8")
            )

        thumbnails.append(thumbnail)

    # return thumbnails as the body
    return {
        "statusCode": 200,
        "body": json.dumps(thumbnails)
    }