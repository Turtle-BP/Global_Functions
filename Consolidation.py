import os
import pandas as pd
import pymysql


############################## FUNÇÕES ##################################

# Pegando todos os Marketplaces
def GetMarketplaces():
    global Dataset_Marketplaces
    
    connection = pymysql.connect(host='serverturtle.csheuezawnml.sa-east-1.rds.amazonaws.com',
                                user='admin',
                                password='Sand316712',
                                database='Marketplaces',
                                cursorclass=pymysql.cursors.DictCursor)

    # Criando o caminho do Databae
    c = connection.cursor()

    # Criando a Query para pegar todos os Marketplaces
    Sql_query = "SELECT MARKETPLACE FROM Marketplaces"

    # Conectando com o banco de dados
    c.execute(Sql_query)
    result = c.fetchall()

    # Fechando conexao
    c.close()
    connection.close()

    # Criando o dataframe
    Dataset_Marketplaces = pd.DataFrame()
    Dataset_Marketplaces['MARKETPLACES'] = [item['MARKETPLACE'] for item in result]

    # Retornando o valor
    return Dataset_Marketplaces


# Função de Consolidação
def Consolidation_files(marketplaces):

    # Definir o caminho do diretório atual
    path = os.getcwd()

    # Os arquivos do diretorio atual
    files_here = os.listdir(path)

    # Criar uma lista vazia para armazenar os dados
    data = []

    # Checando quais marketplaces estao presentes e quais não estão 
    marketplaces_found = [m for m in marketplaces if any(m in f for f in files_here)]
    marketplaces_not_found = [m for m in marketplaces if not any(m in f for f in files_here)]

    for file in files_here:

        # Verificar se o arquivo é um arquivo do Excel
        if file.endswith('.xlsx'):

            if file == 'Consolidado.xlsx':
                pass
        
            else:
                # Leia o arquivo Excel e armazene os dados em um DataFrame, a coluna ID é interpretada como str
                dataframe = pd.read_excel(file, dtype={'ID': str})

                # Adicionar os dados do DataFrame à lista de dados
                data.append(dataframe)

        # Se não for um arquivo .xlsx passe
        else:
            pass

    print('\n')

    # Mostrando os marketplaces que não foram encontrados 
    for not_found in marketplaces_not_found:
        print(f'{not_found} não foi encontrado.')

    print('\n')

    # Mostrando os marketplaces que foram encontrados 
    for found in marketplaces_found:
        print(f'{found} foi encontrado! :D')

    try:
        # Concatenar os dados de todos os arquivos em um único DataFrame
        if data:
            consolidado = pd.concat(data)
            
            # Salvar o DataFrame combinado em um novo arquivo Excel
            consolidado.to_excel('Consolidado.xlsx', index=False)

            print('\n')

            # Printando aonde foi salvo os dados 
            print(f'Os dados foram consolidados em um arquivo chamado "Consolidado.xlsx"\n')
        
        else:
            # Caso dê erro, avise que nenhum arquivo foi consolidado
            print('\nNenhum arquivo foi consolidado! :(\n')
    except:
        print('\nNenhum arquivo foi consolidado! :(\n')



GetMarketplaces()
Consolidation_files(Dataset_Marketplaces['MARKETPLACES'])