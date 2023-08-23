#importando as bibliotecas
import boto3
import json 

import os

aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

#Função para enviar mensagem para a fila 
def send_sqs_message(sqs_queue_url, marca, produto, url):
    # Create SQS client
    sqs_client = boto3.client('sqs', region_name='sa-east-1', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

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

    return send_msg_response

send_sqs_message('https://sqs.sa-east-1.amazonaws.com/502301607529/FASTSHOP-Creation_Search_Urls', 'GoPro','hero','teste.com')