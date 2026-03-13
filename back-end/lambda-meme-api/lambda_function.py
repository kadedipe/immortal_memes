import json
import uuid
import time
import base64
import binascii
import io
from PIL import Image, UnidentifiedImageError
import boto3


def lambda_handler(event, context):

    try:
        meme_data = json.loads(event["body"])
    except Exception:
        return {
            "statusCode": 400,
            "body": json.dumps("Invalid request body")
        }

    # Extract image from data URL
    try:
        header, data = meme_data["image"].split(";base64,")
        extension = header.split("image/")[-1].lower()
    except ValueError:
        return {
            "statusCode": 400,
            "body": json.dumps("Badly formed image data")
        }

    if extension not in ("bmp", "gif", "jpeg", "jpg", "png", "tiff"):
        return {
            "statusCode": 400,
            "body": json.dumps("Unsupported image format")
        }

    # Decode the image
    try:
        image_bytes = base64.decodebytes(bytes(data, "utf-8"))
        image = Image.open(io.BytesIO(image_bytes))
    except (UnidentifiedImageError, binascii.Error):
        return {
            "statusCode": 400,
            "body": json.dumps("Invalid image data")
        }

    # Generate unique ID
    meme_id = uuid.uuid4().hex

    # Connect to S3
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("adcolaps-quantic-im-memes")

    # Upload original image
    with io.BytesIO() as in_mem_file:
        image.save(in_mem_file, format=image.format)
        in_mem_file.seek(0)

        bucket.upload_fileobj(
            in_mem_file,
            f"memes/{meme_id}",
            ExtraArgs={"ContentType": f"image/{extension}"}
        )

    # Create thumbnail
    image.thumbnail((200, 200))

    # JPG doesn't support alpha channel
    if image.mode in ("RGBA", "P"):
        image = image.convert("RGB")

    with io.BytesIO() as in_mem_file:
        image.save(in_mem_file, format="JPEG")
        in_mem_file.seek(0)

        bucket.upload_fileobj(
            in_mem_file,
            f"thumbnails/{meme_id}",
            ExtraArgs={"ContentType": "image/jpeg"}
        )

    # Save metadata to DynamoDB
    posted = int(time.time())
    time_to_die = posted + (24 * 60 * 60)

    db_entry = {
        "id": meme_id,
        "userName": meme_data.get("userName", "anonymous"),
        "timePosted": posted,
        "timeToDie": time_to_die
    }

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("im-memes")

    table.put_item(Item=db_entry)

    # Success response
    return {
        "statusCode": 200,
        "body": json.dumps({
            "id": meme_id
        })
    }
