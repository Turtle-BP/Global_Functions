#importando as bibliotecas
import boto3
import json 

#Criando as credenciais da AWS
AWS_ACCESS_KEY_ID = 'AKIAXJ44CIZUYE4UTUH4'
AWS_SECRET_ACCESS_KEY = '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'

#Função para enviar mensagem para a fila 
def send_sqs_message(sqs_queue_url, marca, produto, url):
    # Create SQS client
    sqs_client = boto3.client('sqs', region_name='sa-east-1', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

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


#Função para receber mensagem da fila