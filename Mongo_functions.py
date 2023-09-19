#bibliotecas 
import pymongo

##Endereço de IPS da marca
GoPro_IP = '54.207.87.136'
Wacom_IP = '52.67.113.63'

#função para inserir dados no mongo
def insert_data(database_name, collection, data_dict, brand):

    #Fazendo as condicionais para a marca
    if brand == 'GoPro':
        IP = GoPro_IP
    elif brand == 'Wacom':
        IP = Wacom_IP

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

    print(result.inserted_id)

    #Fechando o client
    client.close()