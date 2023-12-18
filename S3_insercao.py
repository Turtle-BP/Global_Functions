#Bibliotecas 
import pandas as pd
import boto3

#Funções

# Função que irá inserir o arquivo no S3
def inserir_s3(brand, ExcelName, S3Name, S3Date, Ecommerce=False):
    #Abrindo o cliente s3 
    s3_client = boto3.client('s3')

    #Nome do bucket 
    bucket_name = 'turtle-brand-protection'

    #Fazendo o if para saber se é ecommerce ou não
    if Ecommerce == True:

        #Criando o caminho do arquivo 
        filepath = "GOLD/" + brand + "/" + S3Date + "/Ecommerce/" + S3Name

        #Fazendo o upload do arquivo
        s3_client.upload_file(ExcelName, bucket_name, filepath)

        #print
        print("Arquivo inserido no S3 com sucesso!")

    else:
            
        #Criando o caminho do arquivo 
        filepath = "GOLD/" + brand + "/" + S3Date +"/Marketplace/" + S3Name

        #Fazendo o upload do arquivo
        s3_client.upload_file(ExcelName, bucket_name, filepath)

        #print
        print("Arquivo inserido no S3 com sucesso!")


# Enviando catalogo para o s3
def send_dataframe_s3(dataframe, brand, file_name):

    print(dataframe)

    # Verifique se existem registros a serem salvos no S3
    if not dataframe.empty:
        
        # Salve esse DataFrame no Amazon S3
        s3_client = boto3.client('s3')
        bucket_name = 'turtle-brand-protection'
        s3_file_key = f'Catalog_MercadoLivre/{brand}/{file_name}'

        # Converta o DataFrame para um arquivo CSV
        file_csv = dataframe.to_csv(index=False)

        # Envie o arquivo CSV para o S3
        s3_client.put_object(Bucket=bucket_name, Key=s3_file_key, Body=file_csv)

        print(f'DataFrame com registros CATALOG salvo no S3: s3://{bucket_name}/{s3_file_key}')


# Enviando catalogo para o s3
def send_dataframe_s3_latest(marketplace, dataframe, brand, file_name):

    print(dataframe)

    # Verifique se existem registros a serem salvos no S3
    if not dataframe.empty:
        
        # Salve esse DataFrame no Amazon S3
        s3_client = boto3.client('s3')
        bucket_name = 'turtle-brand-protection'
        s3_file_key = f'marketplace/{brand}/{file_name}'

        # Converta o DataFrame para um arquivo CSV
        file_csv = dataframe.to_csv(index=False)

        # Envie o arquivo CSV para o S3
        s3_client.put_object(Bucket=bucket_name, Key=s3_file_key, Body=file_csv)

        print(f'DataFrame com registros salvo no S3: s3://{bucket_name}/{s3_file_key}')

