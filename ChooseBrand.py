import pandas as pd
import time
import pymysql

import sys 
sys.path.append("../../")


def DfBrands():
    global List_Brands
    
    connection = pymysql.connect(host='serverturtle.csheuezawnml.sa-east-1.rds.amazonaws.com',
                                user='admin',
                                password='Sand316712',
                                database='Brands',
                                cursorclass=pymysql.cursors.DictCursor)

    # Criando o caminho do Database
    c = connection.cursor()

    # Criando a Query
    Sql_query = "SHOW TABLES from Brands"

    # Conectando com o banco de dados
    c.execute(Sql_query)
    result = c.fetchall()
    c.close()
    connection.close()

    df = pd.DataFrame()
    df['Brand'] = result

    df = df.drop_duplicates()
    df = df.reset_index()

    List_Brands = df

    return List_Brands

#Função que monstra quais Brands podemos selecionar
def Choose():

    #Chamando a função que coleta no banco de dados as Brands
    brands = DfBrands()
    brands = brands['Brand']
    print('\n')

    index = 1
    #Percorrendo cada Brand
    for value in brands:
        brand = value.get('Tables_in_Brands')
        string = str(index) + ". " + brand
        print(string)
        index += 1
    
    #Informando para o usúario as Marcas que tem disponivel
    option = int(input(f'\nSelecione a marca desejada: '))
    option = int(option) - 1
    choice = List_Brands['Brand'][option]
    choice = choice.get('Tables_in_Brands')
    print(f'\n{choice} foi escolhida.\n')
        
    return choice


