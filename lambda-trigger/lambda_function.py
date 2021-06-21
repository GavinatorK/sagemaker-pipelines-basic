import json
import boto3
import urllib.parse
import uuid

sm = boto3.client("sagemaker")

s3 = boto3.resource('s3')


def lambda_handler(event, context):
    # using s3 object that triggered the pipeline
    # read the object, copy the file to default bucket
    # and update the inputData param

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    print("bucket:" + str(bucket))
    print("key:" + str(key))

    print("copying input")
    copy_source = {
        'Bucket': bucket,
        'Key': key
    }
    s3.meta.client.copy(copy_source, 'sagemaker-us-east-1-xxxxxxxxxx', 'input/abalone-dataset.csv')

    print("copying batch")
    copy_source = {
        'Bucket': bucket,
        'Key': 'batch/abalone-dataset-batch'
    }
    s3.meta.client.copy(copy_source, 'sagemaker-us-east-1-xxxxxxxxxxx', 'batch/abalone-dataset-batch')

    # TODO implement
    response = sm.start_pipeline_execution(
        PipelineName='AbalonePipeline',
        PipelineExecutionDisplayName='AbalonePipeline2',
        PipelineParameters=[
            {
                'Name': 'InputData',
                'Value': 's3://sagemaker-us-east-1-xxxxxxxxxx/input/abalone-dataset.csv'
            },

            {'Name': 'BatchData',
             'Value': 's3://sagemaker-us-east-1-xxxxxxxx/batch/abalone-dataset-batch'

             },
        ],
        PipelineExecutionDescription='triggeredbys3',
        ClientRequestToken=str(uuid.uuid4())
    )

    print(response)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

