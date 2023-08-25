#bibliotecas 
import pymongo

#função para inserir dados no mongo
def insert_data(database_name, collection, data_dict):

    #conectando ao mongo    
    client = pymongo.MongoClient("mongodb://mongodbadmin:admin@54.207.87.136:27017/?authMechanism=DEFAULT")
    
    #Acessando o database
    db = client[database_name]

    #Acessando a collection
    collection = db[collection]

    #Inserindo o dict
    result = collection.insert_one(data_dict)

    print(result.inserted_id)

    #Fechando o client
    client.close()