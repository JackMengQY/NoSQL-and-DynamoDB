import pandas as pd
import boto3

file_path = "C:/Users/10538/OneDrive/桌面/New_database/train.json"
df = pd.read_json(file_path)
print(df.head(5))


# Initialize the DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name='us-east-1',
                         # key hidden)
# Reference the DynamoDB table
table = dynamodb.Table('recipes')


# Create the DynamoDB table
table = dynamodb.create_table(
    TableName='recipes',
    KeySchema=[
        {
            'AttributeName': 'id',
            'KeyType': 'HASH'  # Partition key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'id',
            'AttributeType': 'N'  # Number type
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

# Wait until the table exists
table.wait_until_exists()

print("Table created successfully! Item count:", table.item_count)


####################################################
# Function to insert data into DynamoDB
def insert_data(df, table):
    for index, row in df.iterrows():
        item = {
        # query hidden due to confidentiality
        }
        table.put_item(Item=item)
        print(f"Inserted item with id: {row['id']}")

# Insert data into the table
insert_data(df, table)



#################################
from boto3.dynamodb.conditions import Attr

# Scan the table to find all items where 'cuisine' is 'indian'
response = table.scan(
    FilterExpression=Attr('cuisine').eq('indian')
)

# Extract the items from the response
items = response['Items']

# Print the ingredients of each 'indian' recipe
for item in items:
    # query hidden due to confidentiality
