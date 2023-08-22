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

