#Importando as bibliotecas 
import json
import pandas as pd
from datetime import datetime
import pymysql



## FUNÇÕES 
#Função para pegar qual é o dia da semana e período é hoje 
def get_day_period(fulldate=False):
    #Converte a string para data 
    data_atual = datetime.now()

    #Extraindo o dia da semana 
    week_day_number = data_atual.weekday()

    #Lista com o nome do dia da semana
    dias_da_semana = ["SEGUNDA", "TERÇA", "QUARTA", "QUINTA", "SEXTA", "SÁBADO", "DOMINGO"]

    #Salvando
    dia_semana = dias_da_semana[week_day_number]

    #Verificando a hora 
    Hour = data_atual.hour

    # Morning: entre 5h da manhã e 11h da manhã 
    if 5 <= Hour < 11:
        Period = "Morning"
    # Afternoon: entre 11h da manhã e 22h da noite
    elif 11 <= Hour < 22:
        Period = "Afternoon"
    # Night: entre 22h da noite e da 5h manhã
    else:
        Period = "Night"

    #Checando se o strdate foi preenchido, se for então passa o dia no formato YY-MM-DD
    if fulldate == True:
        #Pegando a data no formato YY-MM-DD
        data_atual = data_atual.strftime('%Y-%m-%d')

        return dia_semana, Period, data_atual


    else:
        #Fazendo o return 
        return dia_semana, Period

#Função para pegar quais são as marcas que foram monitoradas 
def get_brands():
    #Pegando o dia da semana e o período 
    dia_semana, periodo = get_day_period()

    #Criando a conexão com o banco
    connection = pymysql.connect(host='serverturtle.csheuezawnml.sa-east-1.rds.amazonaws.com',
                            user='admin',
                            password='Sand316712',
                            database='WB_Details',
                            cursorclass=pymysql.cursors.DictCursor)
    
    #Criando o cursor
    cursor = connection.cursor()

    #Executando o comando
    cursor.execute("SELECT MARCAS FROM Scheduling WHERE DIA_SEMANA = '%s' and PERIODO = '%s'" % (dia_semana, periodo))

    #Executando 
    brands = cursor.fetchall()

    #Fechando a conexão
    connection.close()

    #Criando a lista com as marcas  
    lista_de_marcas = brands[0]['MARCAS'].split(',')

    #Retornando a lista de marcas
    return lista_de_marcas