import boto3
import json

kinesis_client = boto3.client("kinesis")


def put_stream(stream_name, partition_key, data):
    return kinesis_client.put_record(StreamName=stream_name,
                                     Data=json.dumps(data),
                                     PartitionKey=partition_key)


if __name__ == '__main__':
    id = int(input("Enter id"))
    name = input("Enter name")

    while True:
        data = {'id': id, 'name': name}
        response = put_stream("test-stream", "k1", data)
        print(response)
