import json
import boto3
import os


DATABASE_OUTPUT = os.getenv('DATABASE_OUTPUT')
TABLE_OUTPUT = os.getenv('TABLE_OUTPUT')
ENV = os.getenv('ENV')

glue_client = boto3.client('glue')


def lambda_handler(event, context):
    try:
        print(f"Evento recebido: {event}")

        bucket_target = event['Records'][0]['s3']['bucket']['name']
        
        print(f"Bucket target: {bucket_target}")

        key = event['Records'][0]['s3']['object']['key']
        
        print(f"Key do objeto: {key}")

        object_uri = f"s3://{bucket_target}/{key}"

        print(f"URI do objeto: {object_uri}")

        response = glue_client.start_job_run(
            JobName='techchallenge2_data_ingestion_job_prod',
            Arguments={
                '--DT_REF': 'AUTO',
                '--BUCKET_TARGET': bucket_target,
                '--KEY': key,
                '--URI_OBJECT': object_uri,
                '--ENV': ENV,
                '--OUTPUT_DATABASE': DATABASE_OUTPUT,
                '--OUTPUT_TABLE': TABLE_OUTPUT
            })
        
        print(f"Glue job iniciado: {response['JobRunId']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Evento recebido com sucesso')
        }
        
    except Exception as e:
        print(f"Erro ao processar evento: {str(e)}")
        raise e
