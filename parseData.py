import dbHandler
import apiHandler
import pandas as pd
import urls 
import json
import os
from time import strftime, localtime 

class parseData:

    def __init__(self, logging=None):
        if(logging):
            import logging
            logging.basicConfig(level=logging.NOTSET)
            self.logger=logging.getLogger(__name__)
        else:
            self.logger=None
        
    def parseHeroesSteam(self,jsonDump):
        '''parse hero response from Steam API
        params---
        json:dict: response from steamAPI
        returns---
        parsed heroes w/ image url added:dict
        ''' 
        heroes = jsonDump['result']['heroes']
        df = pd.DataFrame.from_dict(heroes)
        df['image_full']= df.apply(self.__createHeroPortraitUrlFull,axis=1) #add image column with corresponding url in steam cdn
        df['image_small']=df.apply(self.__createHeroPortraitUrlSmall,axis=1)
        if(self.logger):
            self.logger.info('Portrait URLS added to heroes')
        return df.to_dict(orient='records')
        

    #helper function for parseHeroes
    def __createHeroPortraitUrlFull(self,row):
        return "{}{}_full.png".format(urls.BASE_HERO_IMAGES_URL,row['name'].replace('npc_dota_hero_',''))
    def __createHeroPortraitUrlSmall(self,row):
        return "https://cdn.cloudflare.steamstatic.com//apps/dota2/images/dota_react/heroes/icons/{}.png".format(row['name'].replace('npc_dota_hero_',''))
    def parseMatchesSteam(self,jsonDump):
        '''parse match response from SteamAPI 
        params---
        json:dict:response from api
        returns---
        parsed matches:dict
    '''
        return jsonDump['result']['matches']
        

    def parsePublicMatchesOpenDota(self,jsonDump):
        '''parse match response from Open Dota /publicMatches endpoint
        remove non ranked games: https://github.com/odota/dotaconstants/blob/master/json/lobby_type.json shows types
        remove games less than 15minutes: study https://cosx.org/2017/05/rdota2-seattle-prediction/
        params--
        json: api response 
        return: parsed data: dict
        ''' 
        matches = pd.DataFrame.from_dict(jsonDump)
        matches = matches.loc[matches['game_mode']==22] #only keep ranked matches
        if(self.logger):
            self.logger.info('Filtered for game mode')
        matches = matches.loc[(matches['lobby_type']==7)| (matches['lobby_type']==6) ] #only keep ranked lobbies 6=forced solo mm 7=normal ranked lobby
        if(self.logger):
            self.logger.info('Filtered for lobby type')
        matches = matches.loc[matches['duration']>900] #only keep games >15minutes
        if(self.logger):
            self.logger.info('Filtered for duration>15min')
        #helper function for parseMatches ## convert unix epoch time to datetime 
        
        matches['start_time']=matches.apply(self.__convertUnixTime,axis=1) #convert time 
        if(self.logger):
            self.logger.info('Match start time converted to datetime')
        return matches.to_dict(orient='records')

    def __convertUnixTime(self,row):
        return strftime('%Y-%m-%d %H:%M:%S',localtime(row['start_time']))


'''Testing inserting heroes 
db= dbHandler.dbHandler(os.getenv("MONGO_CONNECTION_STR"))
db.connect(dbName="dota2",collectionName="heroes")
f=open("dumpHeroes.json","r")
data =json.load(f)
pars = parseData()
data = pars.parseHeroesSteam(data)
print(data)
db.insertData(data,many=True)
'''