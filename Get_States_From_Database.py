########################### BIBLIOTECAS ###############################
import pymysql
import pandas as pd
from datetime import datetime


############################## FUNÇÕES #################################

# Quando o seller nao for encontrado, inserir em Verification_Sellers
def Insert_Seller(seller,marketplace,brand):
    
    # Criando a conexão com o banco
    connection = pymysql.connect(host='serverturtle.csheuezawnml.sa-east-1.rds.amazonaws.com',
                            user='admin',
                            password='Sand316712',
                            database='WB_Details',
                            cursorclass=pymysql.cursors.DictCursor)

    # Criando o caminho do Databae
    c = connection.cursor()

    # Criando a query que insere os dados 
    SQL_Query = """INSERT INTO Verification_Sellers (MARKETPLACE, SELLER, BRAND, DATETIME)
                 VALUES (%s, %s, %s, %s)"""

    # Pega a hora atual 
    Hora_Atual = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    # Executando 
    c.execute(SQL_Query,(marketplace,seller,brand,Hora_Atual))
    
    # Fazendo o commit 
    connection.commit()
    connection.close()
    c.close()


# Pegando o estado do banco passando o seller 
def Get_States_From_Database(seller,marketplace,brand):
    
    # Criando a conexão com o banco
    connection = pymysql.connect(host='serverturtle.csheuezawnml.sa-east-1.rds.amazonaws.com',
                            user='admin',
                            password='Sand316712',
                            database='WB_Details',
                            cursorclass=pymysql.cursors.DictCursor)

    # Criando o caminho do Databae
    c = connection.cursor()

    # Criando a Query para pegar o UF daquele seller
    Sql_query = "SELECT ESTADO FROM Cnpjs_Sellers WHERE SELLER = '%s' " % (seller.strip())

    # Conectando com o banco de dados
    c.execute(Sql_query)
    result = c.fetchall()

    # Fechando a conexão do banco
    c.close()
    connection.close()

    # Se o seller não for encontrado
    if not result:
        print("O Seller não foi passado escrito corretamente ou não está na nossa base de dados.")
        print("O valor retornado será ERRO e o Seller será inserido em Verification_Sellers.")

        # Insere esse seller em outro banco 
        Insert_Seller(seller,marketplace,brand)
        
        return 'ERRO'
    
    # Colocando os dados em um dataframe
    Dataset_States = pd.DataFrame()
    Dataset_States['ESTADO'] = [item['ESTADO'] for item in result]

    # Convertendo os estados em uma única string separada por vírgula
    State = ', '.join(Dataset_States['ESTADO'].tolist())

    return State


