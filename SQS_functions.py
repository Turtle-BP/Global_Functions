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

    # Inicialize o cliente SQS
    sqs = boto3.client('sqs')

    # Obtenha a URL da fila com base no nome
    response = sqs.get_queue_url(QueueName=sqs_queue_url)
    queue_url = response['QueueUrl']

    # Inicialize uma lista para armazenar todas as mensagens
    all_messages = []

    while True:
        # Receba mensagens da fila SQS
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'All'
            ],
            MaxNumberOfMessages=10  # Defina o número máximo de mensagens a serem recebidas de uma vez
        )

        messages = response.get('Messages', [])

        if not messages:
            break  # Saia do loop quando não houver mais mensagens na fila

        # Transforme as mensagens em um DataFrame
        data = []
        for message in messages:
            body = json.loads(message['Body'])
            data.append(body)

        all_messages.extend(data)

        # Exclua as mensagens processadas da fila
        entries = [{'Id': message['MessageId'], 'ReceiptHandle': message['ReceiptHandle']} for message in messages]
        sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)

    df = pd.DataFrame(all_messages)

    return df