import time

import boto3
import json

kinesis_stream = boto3.client("kinesis")
#print(help(kinesis_stream))

def put_to_stream(stream_name, partition_key, data):
    response1 = kinesis_stream.put_record(StreamName = stream_name, PartitionKey = partition_key, Data = json.dumps(data))
    return response1

if __name__ == '__main__':
    id = int(input("Enter Id"))
    name = input("Enter Name")
    age = int(input("Enter Age"))

    data = {'id': id, 'name': name, 'age': age}
    response = put_to_stream("test-stream","part", data)
