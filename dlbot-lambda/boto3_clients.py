import boto3

sns_client = boto3.client("sns", region_name="eu-west-2")
s3_client = boto3.client("s3")
dynamodb_client = boto3.resource("dynamodb")
