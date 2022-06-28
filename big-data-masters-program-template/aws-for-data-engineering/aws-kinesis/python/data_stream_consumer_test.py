import time

import boto3
import json


class KinesisDataStreamConsumer:
    def __init__(self, stream_name):
        self.kinesis_client = boto3.client("kinesis")
        self.shard_iterator = \
            self.kinesis_client.get_shard_iterator(StreamName=stream_name, ShardId=self.get_shard_id(),
                                                   ShardIteratorType="LATEST")['ShardIterator']

    def get_shard_id(self):
        return "shardId-000000000000"

    def run_once(self):
        record_response = self.kinesis_client.get_records(ShardIterator=self.shard_iterator)
        print(record_response)

    def run_continous(self):
        while True:
            response = self.kinesis_client.get_records(ShardIterator=self.shard_iterator)
            for record in response['Records']:
                if 'Data' in record and len(record['Data']) > 0:
                    print(json.loads(record['Data']))
            time.sleep(5)


if __name__ == '__main__':
    kinesis_consumer = KinesisDataStreamConsumer("test-stream")
    kinesis_consumer.run_continous()
