import json

def lambda_handler(event, context):
    try:
        print("Evento recebido:")
        print(json.dumps(event, indent=2))
        
        return {
            'statusCode': 200,
            'body': json.dumps('Evento recebido com sucesso')
        }
        
    except Exception as e:
        print(f"Erro ao processar evento: {str(e)}")
        raise e