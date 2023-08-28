#importando as bibliotecas
import boto3
import json 

import os

#Função para enviar mensagem para a fila 
def send_sqs_message(sqs_queue_url, marca, produto, url):
    # Create SQS client
    sqs_client = boto3.client('sqs', region_name='sa-east-1')

    #Criand a mensagem em formato de json 
    message_data = {
        'marca':marca,
        'produto':produto,
        'url':url,
    }

    # Convert message_data into JSON
    message_data = json.dumps(message_data)

    # Send message to SQS queue
    send_msg_response = sqs_client.send_message(
        QueueUrl=sqs_queue_url,
        MessageBody=message_data
    )

    print("Mensagem enviada com sucesso")

    return send_msg_response


def get_sqs_message(queue_url, aws_access_key, aws_secret_key):
    # Create SQS client
    sqs_client = boto3.client('sqs', region_name='sa-east-1', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

    #Pegando a resposta
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1
    )

    #Pegando o corpo da mensagem
    if 'Messages' in response:
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']

        # Delete received message from queue
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

        #Pegando o corpo da mensagem
        message_body = json.loads(message['Body'])
        print(message_body)
        return message_body

