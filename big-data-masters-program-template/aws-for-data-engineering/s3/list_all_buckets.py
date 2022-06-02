import boto3
if __name__ == '__main__':
    s3_resource = boto3.resource("s3")
    for bucket in s3_resource.buckets.all():
        print(bucket)