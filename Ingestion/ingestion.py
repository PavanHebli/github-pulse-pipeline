import os
import time
import json
import logging
import requests
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

# ==============================
# Configuration (ENV VARIABLES)
# ==============================

GITHUB_API_URL = "https://api.github.com/events"
POLL_INTERVAL = 30  # seconds

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
S3_BUCKET = os.getenv("S3_BUCKET")
SERVER_REGION = os.getenv("SERVER_REGION")

# ==============================
# Setup
# ==============================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")


# ==============================
# Fetch GitHub Events
# ==============================

def fetch_github_events():
    headers = {}

    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    try:
        response = requests.get(GITHUB_API_URL, headers=headers, timeout=10)

        if response.status_code == 200:
            remaining = response.headers.get("X-RateLimit-Remaining")
            logger.info(f"GitHub API call successful. Remaining rate limit: {remaining}")
            return response.json()

        elif response.status_code == 403:
            logger.warning("Rate limit exceeded. Sleeping...")
            time.sleep(60)
            return []

        else:
            logger.error(f"Unexpected status code: {response.status_code}")
            return []

    except requests.RequestException as e:
        logger.error(f"Request failed: {str(e)}")
        return []


# ==============================
# Write Raw JSON to S3
# ==============================

def write_to_s3(events):
    if not events:
        return

    now = datetime.now()
    partition_path = now.strftime("year=%Y/month=%m/day=%d")

    file_name = f"github_events_{int(time.time())}.json"
    s3_key = f"raw/github_events/{partition_path}/{file_name}"

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(events),
            ContentType="application/json"
        )
        logger.info(f"Successfully wrote file to s3://{S3_BUCKET}/{s3_key}")

    except ClientError as e:
        logger.error(f"Failed to upload to S3: {str(e)}")


def lambda_handler(event, context):
    events = fetch_github_events()
    write_to_s3(events)

    return {
        "statusCode": 200,
        "body": json.dumps({"events_written": len(events)})
    }

# ==============================
# Main Loop (Runs Forever)
# comment 'lambda_handler' above for and uncomment this code to test locally.
# ==============================

# def run():
#     logger.info("Starting GitHub ingestion service...")

#     while True:
#         start_time = time.time()

#         events = fetch_github_events()
#         write_to_s3(events)

#         elapsed = time.time() - start_time
#         sleep_time = max(0, POLL_INTERVAL - elapsed)

#         time.sleep(sleep_time)


# if __name__ == "__main__":
#     run()

