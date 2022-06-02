import boto3
if __name__ == '__main__':
    s3_resource = boto3.resource("s3")
    my_bucket = s3_resource.Bucket("npntraining-data-lake")
    for obj in my_bucket.objects.all():
        print(obj.key)