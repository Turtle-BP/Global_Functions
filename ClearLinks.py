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

    # Inicialize os DataFrames fora do loop
    Dataframe_Corretos = pd.DataFrame()
    Dataframe_Errados = pd.DataFrame()

    words_found = []

    for word in WordsTable['WORDS']:
        # Criar uma série booleana para verificar se a palavra está contida em 'TITLE'
        condition = Dataframe['TITLE'].str.contains(word)
        
        # Atualizar os DataFrames cumulativamente
        Dataframe_Corretos = pd.concat([Dataframe_Corretos, Dataframe[~condition]])
        Dataframe_Errados = pd.concat([Dataframe_Errados, Dataframe[condition]])

        # Caso uma palavra esteja no DataFrame errado então armazenar a palavra com a quantidade de linhas que foram atualizadas cumulativamente
        for i in range(Dataframe[condition].shape[0]):
            words_found.append(word)


    Dataframe_Errados['WORDS_FOUND'] = words_found

    return Dataframe_Corretos, Dataframe_Errados


# Função para limpar por título 
def Cleaning_Links(urls,title,brand,marketplace):

    # Pegando o dataframe de exclusao
    WordsTable = WordsTable_Connection(brand,marketplace)

    # Criando o dataframe com as urls e os títulos 
    SimpleDataframe = pd.DataFrame()
    SimpleDataframe['URL'] = urls
    SimpleDataframe['TITLE'] = title
    SimpleDataframe['TITLE'] = SimpleDataframe['TITLE'].str.lower().str.strip()

    # Inicialize os DataFrames fora do loop
    Dataframe_Corretos = pd.DataFrame()
    Dataframe_Errados = pd.DataFrame()

    for ids in WordsTable['WORDS']:
        # Atualize os DataFrames cumulativamente
        Dataframe_Corretos = pd.concat([Dataframe_Corretos, SimpleDataframe[SimpleDataframe['ID'].str.contains(ids) == False]])
        Dataframe_Errados = pd.concat([Dataframe_Errados, SimpleDataframe[SimpleDataframe['ID'].str.contains(ids) == True]])

    return Dataframe_Corretos, Dataframe_Errados




