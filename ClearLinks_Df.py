############################### BIBLIOTECAS ##################################
import pymysql 
import pandas as pd 


################################# FUNÇÕES #####################################

# Função para pegar as palavras chaves de exclusão da tabela dinâmica da AWS 
def WordsTable_Connection(brand,marketplace):
    connection = pymysql.connect(host='serverturtle.csheuezawnml.sa-east-1.rds.amazonaws.com',
                                user='admin',
                                password='Sand316712',
                                database='Products',
                                cursorclass=pymysql.cursors.DictCursor)

    # Criando cursor 
    c = connection.cursor()

    # Criando a Query
    Sql_query = "SELECT * FROM Exclusion_Words WHERE BRAND = '%s' AND MARKETPLACE = '%s'" % (brand.strip(),marketplace.strip())

    # Conectando com o banco de dados
    c.execute(Sql_query)
    result = c.fetchall()

    # Passando todos o dataframe para Lowercase
    Dataset_Products = pd.DataFrame()
    Dataset_Products['MARCA'] = [item['BRAND'] for item in result]
    Dataset_Products['WORDS'] = [item['WORDS'] for item in result]
    Dataset_Products['WORDS'] = Dataset_Products['WORDS'].str.lower().str.strip()

    # Retornando o valor
    return Dataset_Products


# Função para limpar quando estiver com APIS
def Cleaning_Links_API(Dataframe,brand,marketplace):

    # Pegando o dataframe de exclusao
    WordsTable = WordsTable_Connection(brand,marketplace)

    # Criando o dataframe com as urls e os títulos 
    Dataframe['TITLE'] = Dataframe['TITLE'].str.lower().str.strip()

    for words in WordsTable['WORDS']: 
        Dataframe = Dataframe[Dataframe['TITLE'].str.contains(words) == False]

    return Dataframe


