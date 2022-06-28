import json
import boto3

def lambda_handler(event, context):

    client = boto3.client('emr')
    response = client.run_job_flow(
        Name="Boto3 Test Cluster",
        ReleaseLabel='emr-5.35.0',
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.xlarge',
            'InstanceCount': 2,
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-03dfc0a2cde1f6519',
            'Ec2KeyName': 'MyEMRKeyPair',
        },
        Applications=[
            {
                'Name': 'Spark'
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole'
    )
    return {
        'statusCode': 200,
        'body': json.dumps('EMR Cluster Created Successfully.')
    }