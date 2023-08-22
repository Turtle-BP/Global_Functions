################################ BIBLIOTECAS ################################
import pandas as pd
from datetime import date, datetime
import os

#Adicionando a função do S3 
from Global_Scripts.S3_insercao import inserir_s3


def Download_Fuction(dataset,marketplace,brand):
    # Criando o caminho onde o arquivo vai ser salvo
    current_path = os.getcwd()
    parent_dir = os.path.dirname(current_path)
    parent_dir = os.path.dirname(parent_dir)
    brand_path = parent_dir + "\Data\\" + brand + "\\"
    
    # Chamando função que irá dizer qual periodo do dia estamos
    periodo_day = Period_Day()[0]
    s3_date = Period_Day()[0]

    # Criando variavel com o nome da brand + Marketplace + periodo e dia atual
    # Ficando desse mosso ("GoPro_Amazon_26_1_afternoon.xlsx")
    download_path = brand_path + "\\" + brand + "_" + marketplace + periodo_day + ".xlsx"

    # S3 nome 
    s3_name = brand + "_" + marketplace + periodo_day + ".xlsx"

    # Fazendo a condicional para criar o dir caso não tenha ainda da marca 
    if not os.path.exists(brand_path):
        os.makedirs(brand_path)

    # Salvando o arquivo em excel
    dataset.to_excel(download_path, index=False)

    # Chamando função que irá inserir o arquivo no S3
    #inserir_s3(brand, download_path, s3_name, s3_date, Ecommerce=Ecommerce_value)




# Função que irá dizer qual o periodo do dia
def Period_Day():
    Hour = datetime.now().hour
    data = date.today()

    # Morning: entre 5h da manhã e 11h da manhã 
    if 5 <= Hour < 11:
        Period = "Morning"
    # Afternoon: entre 11h da manhã e 22h da noite
    elif 11 <= Hour < 22:
        Period = "Afternoon"
    # Night: entre 22h da noite e da 5h manhã
    else:
        Period = "Night"

    #Salvando o formato da data de dia_mes
    date_s3 = f'{data.day}_{data.month}'

    #Salvando o formato certo 
    period_complete = f'_{data.day}_{data.month}_{Period}'
    
    # Retornando com medo correto
    return period_complete,date_s3

