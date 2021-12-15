import sys
import os
sys.path.append(os.path.abspath("/opt/aqueduct/aqueduct/services"))
from food_supply_chain_service import *
import time
import logging
import boto3

if os.environ.get("ENDPOINT_URL"):
    print("Using endpoint of {}".format(os.environ.get("ENDPOINT_URL")))
    bucket = os.environ.get('S3_BUCKET_NAME')
    session = boto3.session.Session()
    s3 = session.client(
         service_name='s3',
         aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
         aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
         endpoint_url=os.environ.get("ENDPOINT_URL")
    )
    try:
        print("Making bucket {} if it doesn't exist".format(bucket))
        s3.create_bucket(Bucket=bucket)
    except Exception as e:
        print(str(e))

while True:
    try:
        logging.info("Checking for work")
        FoodSupplyChainService.pop_and_do_work()
        time.sleep(5)
    except Exception as e:
        print(str(e))
