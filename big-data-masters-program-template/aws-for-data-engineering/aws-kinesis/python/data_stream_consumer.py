import boto3
import json
from datetime import datetime
import time




def get_shard_id(kinesis_client=None):
    # response = kinesis_client.describe_stream(StreamName=stream_name)
    # shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    return "shardId-000000000000"


def run_once(shard_iterator):
    record_response = kinesis_client.get_records(ShardIterator=shard_iterator,
                                                 Limit=2)
    print(record_response)


def run_continous(kinesis_client,shard_iterator):
    while True:
        response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=2)
        shard_iterator = response['NextShardIterator']
        for record in response['Records']:
            if 'Data' in record and len(record['Data']) > 0:
                print(json.loads(record['Data']))
        # wait for 5 seconds
        time.sleep(5)

if __name__ == '__main__':
    kinesis_client = boto3.client('kinesis', region_name='ap-south-1')
    stream_name = 'test-stream'
    shard_id = get_shard_id(kinesis_client)
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=stream_name,
                                                       ShardId=shard_id,
                                                       ShardIteratorType='LATEST')['ShardIterator']
    # run_once(kinesis_client,shard_iterator)

    run_continous(kinesis_client,shard_iterator)
