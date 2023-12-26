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

    #Criando a lista de palavras
    lista_palavras = WordsTable['WORDS'].tolist()

    # Criando o dataframe com as urls e os títulos 
    Dataframe['TITLE'] = Dataframe['TITLE'].str.lower().str.strip()

    # Filtra as linhas em que a coluna 'TITLE' não contém nenhuma palavra da lista
    df_limpo = Dataframe[~Dataframe['TITLE'].str.contains('|'.join(lista_palavras), case=False, na=False)]
    df_removidos = Dataframe[Dataframe['TITLE'].str.contains('|'.join(lista_palavras), case=False, na=False)]

    # Adiciona uma coluna 'PALAVRA_FILTRADA' ao df_removidos para mostrar a palavra responsável
    try:
        df_removidos['REF'] = df_removidos.apply(lambda row: [p for p in lista_palavras if p.lower() in row['TITLE']], axis=1)
        df_removidos['MOTIVO'] = 'PALAVRAS ENCONTRADAS'
    except:
        pass

    return df_limpo, df_removidos
        


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




