###################### BIBLIOTECAS ################################
import pandas as pd 
from datetime import datetime
import pymysql


def Log_registration(marketplace,tempo_corrido,brand,tag,urls_found,urls_collected,status_code,error_description):
    #Criando a conexão 
    connection = pymysql.connect(host='serverturtle.csheuezawnml.sa-east-1.rds.amazonaws.com',
                                user='admin',
                                password='Sand316712',
                                database='WB_Details',
                                cursorclass=pymysql.cursors.DictCursor)

    #Criando cursor 
    C = connection.cursor()

    #Pegando a data de hoje
    today = pd.to_datetime('today', errors='ignore').date()

    #Criando o período 
    Hour = datetime.now().hour
    if Hour > 22:
        Period = "Night"
    elif Hour > 11:
        Period = "Afternoon"
    elif Hour > 4:
        Period = "Morning"

    #Criando a query 
    SQL_Query = """INSERT INTO WB_Logs (DATE_START, PERIOD ,MARKETPLACE, TIME_ELAPSED, BRAND, TAG, URLS_FOUND, URLS_COLLECTED, STATUS_CODE, ERROR_DESCRIPTION) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    #Executando 
    C.execute(SQL_Query,(today,Period,marketplace,tempo_corrido,brand,tag,urls_found,urls_collected,status_code,error_description))
    
    #Fazendo o commit 
    connection.commit()
    connection.close()
    C.close()



#! JSON DE TESTE
# json = {
#     "Marketplace" : [
#         "ViaVarejo"
#     ],
#     "Brands" : [
#         "GoPro"
#     ],
#     "User": "01",
#     "Origem":"Teste",
# }

#Função para registro do Log de Triggers da DfProducts_All
def LogTrigger_Registration(json):

    #Fazendo a conexão com o banco de dados 
    connection = pymysql.connect(host='serverturtle.csheuezawnml.sa-east-1.rds.amazonaws.com',
                                user='admin',
                                password='Sand316712',
                                database='Logs',
                                cursorclass=pymysql.cursors.DictCursor)

    #Criando cursor 
    C = connection.cursor()

    #Pegando a data de hoje
    json['Date'] = pd.to_datetime('today', errors='ignore').date()

    marketplaces_str = ', '.join(json['Marketplace'])

    #Inserindo os dados
    SQL_Query = """INSERT INTO Trigger_Process (DATE, BRANDS, MARKETPLACE, USER, ORIGEM) VALUES (%s,%s,%s,%s,%s)"""

    #Executando
    C.execute(SQL_Query,(json['Date'],json['Brands'],marketplaces_str,json['User'],json['Origem']))

    #Pegando o ID do registro
    SQL_Query = """SELECT idTrigger_Process FROM Trigger_Process ORDER BY idTrigger_Process DESC LIMIT 1"""

    #Salvando o ID para fazer o return depois 
    C.execute(SQL_Query)    

    #Pegando o ID
    ID = C.fetchone()['idTrigger_Process']
    
    #Fazendo o commit
    connection.commit()
    connection.close()

    #Fechando o cursor
    C.close()

    return ID


#Função para registro do Log de Link 
def LogLink_Registration(json):
    #Fazendo a conexão com o banco de dados 
    connection = pymysql.connect(host='serverturtle.csheuezawnml.sa-east-1.rds.amazonaws.com',
                                user='admin',
                                password='Sand316712',
                                database='Logs',
                                cursorclass=pymysql.cursors.DictCursor)

    #Criando cursor 
    C = connection.cursor()

    #Pegando a data de hoje
    json['Date'] = pd.to_datetime('today', errors='ignore').date()

    #Criando o query para colocar
    SQL_Query = """INSERT INTO Trigger_Process (Trigger_ID, DATE, MARKETPLACE, BRAND, URL, ATUAL_STEP, STATUS) VALUES (%s,%s,%s,%s,%s,%s,%s)"""

    #Salvando o ID para fazer o return depois 
    C.execute(SQL_Query, (json['Trigger_ID'],json['Date'],json['Marketplace'],json['Marca'],json['URL'],json['Atual_Step'],json['Status'])) 

    #Fazendo o commit
    connection.commit()
    connection.close()

    #Fechando o cursor
    C.close()

    