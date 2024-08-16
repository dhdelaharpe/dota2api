import dbHandler
import apiHandler
import pandas as pd
import urls 
import json
import os

dbName=os.getenv('DB_NAME')
def parseHeroes(json):
    '''parse hero response 
    params---
    json:dict: response from steamAPI
    
    ''' 
    heroes = json['result']['heroes']
    df = pd.DataFrame.from_dict(heroes)
    df['image']= df.apply(createHeroPortraitUrl,axis=1)
    
    db = dbHandler.dbHandler(os.getenv('MONGO_CONNECTION_STR'),dbName,'heroes')
    db.connect()
    db.insertData(df.to_dict(orient='records'),many=True)
    
    db.endSession()

#helper function for parseHeroes
def createHeroPortraitUrl(row):
    return "{}{}_full.png".format(urls.BASE_HERO_IMAGES_URL,row['name'].replace('npc_dota_hero_',''))



'''testing '''
#use file for testing 
#f=open('dumpHeroes.json','r')
#jsondump = json.load(f)
#f.close()
#end file
#parseHeroes(jsondump)
'''end testing'''