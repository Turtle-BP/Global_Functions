############################### BIBLIOTECAS ##################################
import pymysql 
import pandas as pd 


################################ FUNÇÕES ######################################

# Função para pegar os ids de exclusão da tabela dinâmica da AWS 
def IDsTable_Connection(brand,marketplace):
    connection = pymysql.connect(host='serverturtle.csheuezawnml.sa-east-1.rds.amazonaws.com',
                                user='admin',
                                password='Sand316712',
                                database='Products',
                                cursorclass=pymysql.cursors.DictCursor)

    # Criando cursor 
    c = connection.cursor()

    # Criando a Query
    Sql_query = "SELECT * FROM Exclusion_ID WHERE BRAND = '%s' AND MARKETPLACE = '%s'" % (brand.strip(),marketplace.strip())

    # Conectando com o banco de dados
    c.execute(Sql_query)
    result = c.fetchall()

    # Passando todos o dataframe para Lowercase
    Dataset_IDs = pd.DataFrame()
    Dataset_IDs['MARCA'] = [item['BRAND'] for item in result]
    Dataset_IDs['ID'] = [item['ID'] for item in result]
    Dataset_IDs['ID'] = Dataset_IDs['ID'].str.lower().str.strip()

    #Retornando o valor
    return Dataset_IDs


# Função pra limpar os marketplaces através do ID 
def Cleaning_IDs(Dataframe,brand,marketplace):

    # Pegando o dataframe de exclusao
    IDsTable = IDsTable_Connection(brand,marketplace)

    # Criando o dataframe com os ids 
    Dataframe['ID'] = Dataframe['ID'].str.lower().str.strip()

    for ids in IDsTable['ID']: 
        Dataframe = Dataframe[Dataframe['ID'].str.contains(ids) == False]

    return Dataframe



