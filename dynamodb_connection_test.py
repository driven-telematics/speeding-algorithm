import boto3

# Initialize the DynamoDB client
dynamodb = boto3.client("dynamodb")

# List existing tables (to check connection)
response = dynamodb.list_tables()
print("DynamoDB Tables:", response["TableNames"])
