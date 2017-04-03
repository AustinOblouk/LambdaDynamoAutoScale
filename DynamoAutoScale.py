# Austin Oblouk - Oblouk.com
# https://github.com/AustinOblouk/LambdaDynamoAutoScale
# This is a simple automated DynamoDb provisioning script meant to be run inside of a Python 2.7 Lambda Function
# For setting up this simple script please see https://github.com/AustinOblouk/LambdaDynamoAutoScale

import json
import boto3
import datetime
import math

# If peak throughput is >= 90% of capacity, provisioned read/write capacity will be increased
RAISE_CAPACITY_THRESHOLD = 0.90 

# If provisioned capacity is raised it will be raised to (peak throughput)*this
RAISE_CAPACITY_MULTIPLE = 1.40

# If provisioned capacity is lowered it will be lowered to (peak throughput)*this
LOWER_CAPACITY_MULTIPLE = 1.15

# Maximum times to lower capacity per day (Amazon caps this number at 4)
MAXIMUM_TIMES_TO_LOWER_CAPACITY_PER_DAY = 1

# The minimum provisioned throughput for read and write
MINIMUM_CAPACITY = 3

# The maximum provisioned throughput for read and write
MAXIMUM_CAPACITY = 5000

# The email address VERIFIED with SES to act as an email sender
EMAIL_ADDRESS_OF_SENDER = ""

# The email addresses of the recipients to receive adjustment notifications
# These emails must also be verified with SES if operating in a sandboxed environment
EMAIL_ADDRESSES_OF_RECIPIENTS = []

# How many seconds of data per read/write throughput data point (MUST be a multiple of 60)
SECONDS_PER_DATA_POINT = 300


def lambda_handler(event, context):
    cloudwatch = boto3.client('cloudwatch')
    dynamodbClient = boto3.client('dynamodb')
    for table in dynamodbClient.list_tables()["TableNames"]:
        adjustTableCapacity(str(table))
        adjustTableIndexCapacities(str(table))

def adjustTableCapacity(tableName):
    dynamodb = boto3.resource('dynamodb').Table(tableName)
    cloudwatch = boto3.client('cloudwatch')
    
    currentCapacity = dynamodb.provisioned_throughput
    decreasesSoFarToday = currentCapacity["NumberOfDecreasesToday"]
    writeCapacity = currentCapacity["WriteCapacityUnits"]
    readCapacity = currentCapacity["ReadCapacityUnits"]
    
    now = datetime.datetime.utcnow().isoformat()
    start = ( datetime.datetime.utcnow() -
        datetime.timedelta( minutes=1440 )
        ).isoformat()

    dimensions = [{'Name': 'TableName', 'Value': tableName}]
    
    writeThroughput = cloudwatch.get_metric_statistics(Period=SECONDS_PER_DATA_POINT,
        StartTime=start, EndTime=now,   
        MetricName='ConsumedWriteCapacityUnits', Namespace='AWS/DynamoDB',
        Statistics=['Sum'],Dimensions=dimensions)
    
    readThroughput = cloudwatch.get_metric_statistics(Period=SECONDS_PER_DATA_POINT,
        StartTime=start, EndTime=now,   
        MetricName='ConsumedReadCapacityUnits', Namespace='AWS/DynamoDB',
        Statistics=['Sum'],Dimensions=dimensions)
    
    peakReadThroughputLastDay = 0
    peakWriteThroughputLastDay = 0
    for datapoint in readThroughput["Datapoints"]:
        peakReadThroughputLastDay = max(peakReadThroughputLastDay, datapoint["Sum"]/SECONDS_PER_DATA_POINT)
        
    for datapoint in writeThroughput["Datapoints"]:
        peakWriteThroughputLastDay = max(peakWriteThroughputLastDay, datapoint["Sum"]/SECONDS_PER_DATA_POINT)        
        
    adjustedWriteCapacity = writeCapacity
    adjustedReadCapacity = readCapacity
        
    if decreasesSoFarToday < MAXIMUM_TIMES_TO_LOWER_CAPACITY_PER_DAY:
        adjustedWriteCapacity = math.ceil(peakWriteThroughputLastDay * LOWER_CAPACITY_MULTIPLE)
        adjustedReadCapacity = math.ceil(peakReadThroughputLastDay * LOWER_CAPACITY_MULTIPLE)
    
    if peakWriteThroughputLastDay > (writeCapacity * RAISE_CAPACITY_THRESHOLD):
        adjustedWriteCapacity = math.ceil(peakWriteThroughputLastDay * RAISE_CAPACITY_MULTIPLE)

    if peakReadThroughputLastDay > (readCapacity * RAISE_CAPACITY_THRESHOLD):
        adjustedReadCapacity = math.ceil(peakReadThroughputLastDay * RAISE_CAPACITY_MULTIPLE)
    
    adjustedWriteCapacity = max(adjustedWriteCapacity , MINIMUM_CAPACITY)
    adjustedWriteCapacity = min(adjustedWriteCapacity , MAXIMUM_CAPACITY)
    adjustedReadCapacity = max(adjustedReadCapacity , MINIMUM_CAPACITY)
    adjustedReadCapacity = min(adjustedReadCapacity , MAXIMUM_CAPACITY)
    
    readWasUpdated = (adjustedReadCapacity != readCapacity)
    writeWasUpdated = (adjustedWriteCapacity != writeCapacity)
        
    if readWasUpdated or writeWasUpdated:
        dynamoClient = boto3.client('dynamodb')
        dynamoClient.update_table(TableName=tableName, ProvisionedThroughput={
            "ReadCapacityUnits": int(adjustedReadCapacity),
            "WriteCapacityUnits": int(adjustedWriteCapacity)
            })
    
    returnString =  "Dynamo Table: "+tableName
    returnString += " | Write Capacity: "+str(writeCapacity)
    returnString += " | Read Capacity: "+str(readCapacity)
    returnString += " | Decreases So Far Today: "+str(decreasesSoFarToday)+"/"+str(MAXIMUM_TIMES_TO_LOWER_CAPACITY_PER_DAY)+""
    returnString += " | Peak Read Capacity (24hrs): "+str(peakReadThroughputLastDay)
    returnString += " | Peak Write Capacity (24hrs): "+str(peakWriteThroughputLastDay)
    returnString += " | Updated Read: "+str(readWasUpdated)
    returnString += " | New Read Capacity: "+str(adjustedReadCapacity)
    returnString += " | Updated Write: "+str(writeWasUpdated)
    returnString += " | New Write Capacity: "+str(adjustedWriteCapacity)
    if (readWasUpdated == True or writeWasUpdated == True) and len(EMAIL_ADDRESSES_OF_RECIPIENTS) > 0 and EMAIL_ADDRESS_OF_SENDER != "":
        sendEmailNotification("Dynamo Capacity Updated For Table "+tableName+"","Details: "+returnString)
    print(returnString)
    
def adjustTableIndexCapacities(tableName):
    dynamodb = boto3.resource('dynamodb').Table(tableName)
    if dynamodb.global_secondary_indexes is None:
        print("No secondary indexes in table: "+tableName)
        return

    cloudwatch = boto3.client('cloudwatch')
    
    for secondaryIndex in dynamodb.global_secondary_indexes:
        secondaryIndexName = secondaryIndex["IndexName"]
        currentCapacity = secondaryIndex["ProvisionedThroughput"]
        decreasesSoFarToday = currentCapacity["NumberOfDecreasesToday"]
        writeCapacity = currentCapacity["WriteCapacityUnits"]
        readCapacity = currentCapacity["ReadCapacityUnits"]
        
        now = datetime.datetime.utcnow().isoformat()
        start = ( datetime.datetime.utcnow() -
            datetime.timedelta( minutes=1440 )
            ).isoformat()

        dimensions = [{'Name': 'TableName', 'Value': tableName}, {'Name': 'GlobalSecondaryIndexName', 'Value': secondaryIndexName}]
        
        writeThroughput = cloudwatch.get_metric_statistics(Period=SECONDS_PER_DATA_POINT,
            StartTime=start, EndTime=now,   
            MetricName='ConsumedWriteCapacityUnits', Namespace='AWS/DynamoDB',
            Statistics=['Sum'],Dimensions=dimensions)
        
        readThroughput = cloudwatch.get_metric_statistics(Period=SECONDS_PER_DATA_POINT,
            StartTime=start, EndTime=now,   
            MetricName='ConsumedReadCapacityUnits', Namespace='AWS/DynamoDB',
            Statistics=['Sum'],Dimensions=dimensions)
            
        peakReadThroughputLastDay = 0
        peakWriteThroughputLastDay = 0
        for datapoint in readThroughput["Datapoints"]:
            peakReadThroughputLastDay = max(peakReadThroughputLastDay, datapoint["Sum"]/SECONDS_PER_DATA_POINT)
            
        for datapoint in writeThroughput["Datapoints"]:
            peakWriteThroughputLastDay = max(peakWriteThroughputLastDay, datapoint["Sum"]/SECONDS_PER_DATA_POINT)


        adjustedWriteCapacity = writeCapacity
        adjustedReadCapacity = readCapacity
            
        if decreasesSoFarToday < MAXIMUM_TIMES_TO_LOWER_CAPACITY_PER_DAY:
            adjustedWriteCapacity = math.ceil(peakWriteThroughputLastDay * LOWER_CAPACITY_MULTIPLE)
            adjustedReadCapacity = math.ceil(peakReadThroughputLastDay * LOWER_CAPACITY_MULTIPLE)
        
        if peakWriteThroughputLastDay > (writeCapacity * RAISE_CAPACITY_THRESHOLD):
            adjustedWriteCapacity = math.ceil(peakWriteThroughputLastDay * RAISE_CAPACITY_MULTIPLE)

        if peakReadThroughputLastDay > (readCapacity * RAISE_CAPACITY_THRESHOLD):
            adjustedReadCapacity = math.ceil(peakReadThroughputLastDay * RAISE_CAPACITY_MULTIPLE)
        
        adjustedWriteCapacity = max(adjustedWriteCapacity , MINIMUM_CAPACITY)
        adjustedWriteCapacity = min(adjustedWriteCapacity , MAXIMUM_CAPACITY)
        adjustedReadCapacity = max(adjustedReadCapacity , MINIMUM_CAPACITY)
        adjustedReadCapacity = min(adjustedReadCapacity , MAXIMUM_CAPACITY)
        
        readWasUpdated = (adjustedReadCapacity != readCapacity)
        writeWasUpdated = (adjustedWriteCapacity != writeCapacity)
            
        if readWasUpdated or writeWasUpdated:
            dynamoClient = boto3.client('dynamodb')
            dynamoClient.update_table(TableName=tableName, GlobalSecondaryIndexUpdates=[
                {"Update":
                    {"IndexName": secondaryIndexName,
                        "ProvisionedThroughput":
                        {
                            "ReadCapacityUnits": int(adjustedReadCapacity),
                            "WriteCapacityUnits": int(adjustedWriteCapacity)
                        }
                    }
                }
            ])
        
        returnString = "Dynamo Table ("+tableName+") Index: "+secondaryIndexName
        returnString += " | Write Capacity: "+str(writeCapacity)
        returnString += " | Read Capacity: "+str(readCapacity)
        returnString += " | Decreases So Far Today: "+str(decreasesSoFarToday)+"/"+str(MAXIMUM_TIMES_TO_LOWER_CAPACITY_PER_DAY)+""
        returnString += " | Peak Read Capacity (24hrs): "+str(peakReadThroughputLastDay)
        returnString += " | Peak Write Capacity (24hrs): "+str(peakWriteThroughputLastDay)
        returnString += " | Updated Read: "+str(readWasUpdated)
        returnString += " | New Read Capacity: "+str(adjustedReadCapacity)
        returnString += " | Updated Write: "+str(writeWasUpdated)
        returnString += " | New Write Capacity: "+str(adjustedWriteCapacity)
        if (readWasUpdated == True or writeWasUpdated == True) and len(EMAIL_ADDRESSES_OF_RECIPIENTS) > 0 and EMAIL_ADDRESS_OF_SENDER != "":
            sendEmailNotification("Dynamo Capacity Updated For Index "+secondaryIndexName+" on Table "+tableName+"","Details: "+returnString)
        print(returnString)
    print("Finished Secondary Indexes on Table: "+tableName)

        
def sendEmailNotification(subject, body):
    ses = boto3.client('ses')
    ses.send_email(
        Source=EMAIL_ADDRESS_OF_SENDER,
        Destination={
            'ToAddresses': EMAIL_ADDRESSES_OF_RECIPIENTS
        },
        Message={
            'Subject': {
                'Data': subject,
                'Charset': 'UTF-8'
            },
            'Body': {
                'Html': {
                    'Data': body,
                    'Charset': 'UTF-8'
                }
            }
        }
    )