import boto3

if __name__ == '__main__':
    s3_resource = boto3.resource("s3")
    bucket_configuration = {"LocationConstraint": "ap-south-1"}
    s3_resource.create_bucket(Bucket="abc24032022", CreateBucketConfiguration=bucket_configuration)
