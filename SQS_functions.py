#importando as bibliotecas
import boto3
import json 
import pandas as pd

#Função para enviar mensagem para a fila 
def send_sqs_message(sqs_queue_url, message_data):
    
    # Create SQS client
    sqs_client = boto3.client('sqs', region_name='sa-east-1')

    # Convert message_data into JSON
    message_data = json.dumps(message_data, ensure_ascii=False)

    # Send message to SQS queue
    send_msg_response = sqs_client.send_message(
        QueueUrl=sqs_queue_url,
        MessageBody=message_data
    )

    print("Mensagem enviada com sucesso")

    return send_msg_response


#Função para pegar todas as mensagens de uma única fila e transformar em um DataFrame
def get_all_sqs(sqs_queue_url):
        
    # Create SQS client
    sqs_client = boto3.client('sqs', region_name='sa-east-1')

    # Receive message from SQS queue
    response = sqs_client.receive_message(
        QueueUrl=sqs_queue_url,
        MaxNumberOfMessages=10000
    )

    messages = response.get('Messages', [])

    data = []
    for message in messages:
        body = json.loads(message['Body'])
        data.append(body)

    df = pd.DataFrame(data)

    print("Mensagem coletadas com sucesso")

    return df
