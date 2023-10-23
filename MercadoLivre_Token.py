############################################## BIBLIOTECAS ##################################################
import pandas as pd 
import pymysql
import requests

from datetime import datetime, timedelta


################################################ FUNÇÕES #####################################################

# Inseri o Token no banco 
def Insert_Token(token):
    # Criando a conexão 
    connection = pymysql.connect(host='serverturtle.csheuezawnml.sa-east-1.rds.amazonaws.com',
                                user='admin',
                                password='Sand316712',
                                database='WB_Details',
                                cursorclass=pymysql.cursors.DictCursor)

    # Criando cursor 
    C = connection.cursor()

    # Criando a query que atualiza o Token ja presente no banco
    SQL_Query = """
    UPDATE MercadoLivre_Token
    SET MARKETPLACE = 'MERCADO LIVRE', token = %s, DATETIME = %s
    WHERE MARKETPLACE = 'MERCADO LIVRE'
    """

    # Pega a hora atual 
    Hora_Atual = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    # Executando 
    C.execute(SQL_Query,(token,Hora_Atual))
    
    # Fazendo o commit 
    connection.commit()
    connection.close()
    C.close()


# Pega o Token do banco
def Get_Token():
    # Criando a conexão
    connection = pymysql.connect(host='serverturtle.csheuezawnml.sa-east-1.rds.amazonaws.com',
                                user='admin',
                                password='Sand316712',
                                database='WB_Details',
                                cursorclass=pymysql.cursors.DictCursor)

    # Criando cursor 
    C = connection.cursor()

    # Pegando o ultimo token inserido como garantia
    Query_Token = "SELECT * FROM MercadoLivre_Token ORDER BY DATETIME DESC LIMIT 1"

    # Conectando com o banco de dados
    C.execute(Query_Token)
    result = C.fetchall()

    # Criando o dataframe
    Dataframe_Token = pd.DataFrame()

    Dataframe_Token['MARKETPLACE'] = [item['MARKETPLACE'] for item in result]
    Dataframe_Token['TOKEN'] = [item['TOKEN'] for item in result]
    Dataframe_Token['DATETIME'] = [item['DATETIME'] for item in result]

    # Retornando o dataframe
    return Dataframe_Token


# Gera um Token
def Generate_Token():
    # endpoint onde dá o refresh no Token 
    url = 'https://api.mercadolibre.com/oauth/token'
    
    # Header necessario para dar o refresh no Token
    headers = {
        'accept': 'application/json',
        'content-type': 'application/x-www-form-urlencoded'
    }

    # Dados necessarios para dar o refresh no Token
    data = {
        'grant_type': 'refresh_token',
        'client_id': '16039950189108',
        'client_secret': '42MXWXpDK6AL2Fwm4nSHQuPgVAl4O0mO',
        'refresh_token': 'TG-647f84334619f800013cd8d2-454552276'
    }

    # Dando um get no endpoint passando o header e os dados requiridos 
    response = requests.post(url, headers=headers, data=data)

    # Transformando o response em json
    json = response.json()

    # Pegando apenas o Token do response
    Token = json['access_token']

    print(f'\nGerando um novo Token...\nSeu Token de acesso é {Token}\nContinuando...\n')

    # Retornando o Token
    return Token


# Confere o tempo, se o token for expirar, cria um novo e insere no banco, se nao, pega o que esta no banco
def Checking_Token_Time():

    # Pegando o Token que esta no banco 
    Banco_Token = Get_Token()
    
    # Token e Hora atuais 
    Token_Atual = Banco_Token['TOKEN'][0]
    Data_Hora_Banco = str(Banco_Token['DATETIME'][0])

    # Converte a string de data/hora do banco em um objeto datetime
    datetime_banco = datetime.strptime(Data_Hora_Banco, '%d-%m-%Y %H:%M:%S')

    # Calcula a diferença entre a data/hora atual e a do banco
    diferenca = datetime.now() - datetime_banco

    # Verifica se a diferença é maior que 5 horas
    if diferenca > timedelta(hours=5):
        print(f"O Token {Token_Atual} já está para expirar, vamos gerar um novo Token.")
        
        # Gera um novo Token
        Token_Gerado = Generate_Token()

        # Atualiza o Token no banco com a hora atual
        Insert_Token(Token_Gerado)

        # Retorna o novo Token
        return Token_Gerado        

    else:
        # Se o Token foi gerado há menos de 5 horas
        print(f"\nO Token {Token_Atual} ainda é válido.\nContinuando...\n")
        
        # Retorne o Token Atual
        return Token_Atual    

