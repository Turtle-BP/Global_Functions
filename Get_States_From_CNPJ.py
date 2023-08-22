########################### BIBLIOTECAS ###############################
import pandas as pd
import requests
import time


############################## FUNÇÕES ################################

# Remove . - e / dos CNPJs
def Remove_Characters(cnpj):

    # Passando o cnpj para string
    cnpj = str(cnpj)
    
    # Remove os pontos
    cnpj = cnpj.replace(".", "")
    
    # Remove os traços
    cnpj = cnpj.replace("-", "")

    # Remove as barras
    cnpj = cnpj.replace("/", "")
    
    return cnpj


# Pega o estado através da API passando o CNPJ 
def Get_State_CNPJ(cnpj):

    formatted_cnpj = Remove_Characters(cnpj)

    response = requests.get(f'https://receitaws.com.br/v1/cnpj/{formatted_cnpj}')
    time.sleep(21)
    text_json = response.json()

    try:
        state = text_json['uf']
    except:
        print('O CNPJ não existe ou não foi passado corretamente, o estado vai ser retornado como ERRO.')
        state = 'ERRO'

    return state


