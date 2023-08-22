########################### BIBLIOTECAS ###############################
import pymysql
import pandas as pd



############################## FUNÇÕES #################################
def Get_States():
    # Criando a conexão com o banco
    connection = pymysql.connect(host='serverturtle.csheuezawnml.sa-east-1.rds.amazonaws.com',
                            user='admin',
                            password='Sand316712',
                            database='WB_Details',
                            cursorclass=pymysql.cursors.DictCursor)

    # Criando o caminho do Databae
    c = connection.cursor()

    # Criando a Query para pegar todos os estados e siglas do banco
    Sql_query = "SELECT * FROM Estados"

    # Conectando com o banco de dados
    c.execute(Sql_query)
    result = c.fetchall()

    # Fechando a conexão do banco
    c.close()
    connection.close()

    # Colocando os dados em um dataframe
    Dataset_States = pd.DataFrame()
    Dataset_States['ESTADO'] = [item['ESTADO'] for item in result]
    Dataset_States['SIGLA'] = [item['SIGLA'] for item in result]

    # Criar o dicionário
    UFs = {}

    # Percorrer o dataframe e adicionar cada par estado-sigla ao dicionário
    for index, row in Dataset_States.iterrows():
        estado = row['ESTADO']
        sigla = row['SIGLA']
        UFs[estado] = sigla

    return UFs


