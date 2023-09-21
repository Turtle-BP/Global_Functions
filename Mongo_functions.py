#bibliotecas 
import pymongo

##Endereço de IPS da marca
GoPro_IP = '54.207.87.136'
Wacom_IP = '52.67.113.63'
Sample_IP = '18.228.141.40'

#função para inserir dados no mongo
def insert_data(database_name, collection, data_dict, brand):

    #Fazendo as condicionais para a marca
    if brand == 'GoPro':
        IP = GoPro_IP
    elif brand == 'Wacom':
        IP = Wacom_IP
    elif brand == 'Sample':
        IP = Sample_IP

    #conectando ao mongo    
    client = pymongo.MongoClient(f"mongodb://mongodbadmin:admin@{IP}:27017/?authMechanism=DEFAULT")
    
    #Acessando o database
    db = client[database_name]

    #Acessando a collection
    collection = db[collection]

    #Inserindo o dict
    try:
        result = collection.insert_one(data_dict)
    except:
        print("É duplicado | erro")

    #Fechando o client
    client.close()
 

#Função para pegar todos os itens dentro de uma collection
def get_all_items(database, collection_user, brand):
    #Fazendo as condicionais para a marca
    if brand == 'GoPro':
        IP = GoPro_IP
    elif brand == 'Wacom':
        IP = Wacom_IP
    elif brand == 'Sample':
        IP = Sample_IP

    #conectando ao mongo    
    client = pymongo.MongoClient(f"mongodb://mongodbadmin:admin@{IP}:27017/?authMechanism=DEFAULT")
    
    #Acessando o database
    db = client[database]

    #Acessando a collection
    collection = db[collection_user]

    #Pegando todos os itens
    all_items = collection.find({})

    #Passando para uma lista
    items = list(all_items)

    #Fechando o client
    client.close()

    #Retornando a lista de itens
    return items











