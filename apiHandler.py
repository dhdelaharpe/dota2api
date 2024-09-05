#imports
# standard libraries 
import os 
import csv
import json
from dotenv import load_dotenv, find_dotenv
from pathlib import Path 
import logging
import numpy as np 
import pandas as pd 
import requests
from urllib.parse import urlencode
import urls

#retry library
from tenacity import retry,stop_after_attempt, wait_exponential, retry_if_exception_type
#load environment to access API key 
load_dotenv(".env")

class ApiHandler(object):
    def __init__(self,api_key=None,language=None,request_exec=None,logging=None):
        '''params---
            api_key = steam web api key ->required to be provided or exist in environment variable 
            language = localization to call in steamapi 
            request_exec: what to use to send requests: default python requests
            logging: enable logs
        ''' 
        request_exec=requests.get
        if(api_key):
            self.api_key=api_key
        else:
            self.api_key= os.getenv('DOTA2_API_KEY')
        
        if(language):
            self.language=language
        else:
            self.language='en_us'
        self.__format="json"
        if(logging):
            import logging
            logging.basicConfig(level=logging.NOTSET)
            self.logger=logging.getLogger(__name__)
        else:
            self.logger=None
    
    @retry(
        stop=stop_after_attempt(3), #retry limit
        wait=wait_exponential(multiplier=1,min=4,max=10), #delay before retries
        retry = retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout))
    )
    def sendRequest(self,call,header=None):
        '''
        Return json response from get request
        Splitting various common errors to allow handling separately (e.g. adding wait time to resend to timeout etc)
        params---
        call: str
        :header: (dict) headers to be added to call
        returns---
        json data
        '''

        try:
            response  = requests.get(call,headers=header)            
            response.raise_for_status()
            if(self.logger):
                self.logger.info('Request sent:{} Response code: {} Additional headers: {} '.format(call,response.status_code,header))
            #check if json parsable
            if('application/json' in response.headers.get('Content-Type','')):
                return response.json()
            else:
                return response.text        
        except requests.exceptions.HTTPError as http_err:
            print('HTTP error: {}'.format(http_err))
        
        except requests.exceptions.ConnectionError as con_err:
            print('Connection error: {}'.format(con_err))

        except requests.exceptions.Timeout as time_err:
            print('Timeout error: {}'.format(time_err))

        except requests.exceptions.RequestException as req_err:
            print('Request error: {}'.format(req_err))

        except requests.exceptions.SSLError as ssl_err:
            print('SSL error: {}'.format(ssl_err))

        except requests.exceptions.APIAuthenticationError as auth_err:
            print('API Auth error:{}'.format(auth_err))

    def __buildReq(self,call,**kwargs):
        '''constructs API query
        params---
        call: str
        kwargs: extra params: json
        return---
        encoded url to be sent
        '''
        kwargs['key']= self.api_key #access key 
        if('language' not in kwargs):#return lang
            kwargs['language']=self.language
        if('format' not in kwargs):#return type
            kwargs['format']=self.__format
        query = urlencode(kwargs) #fit provided params to url query 
        return "{}{}?{}".format(urls.BASE,call,query) #build into acceptable api call and return 

    def fetchHeroes(self,**kwargs):
        '''
        Structure API call to fetch heroes
        
        params---
        Any acceptable key word args 
        
        returns---
        encoded call: url 
        '''
        url = self.__buildReq(urls.GET_HEROES,language=self.language,**kwargs) #build url 
        if(self.logger):
            self.logger.info('URL built: {}'.format(url))
        return url  
    
    def fetchHeroesDetailed(self,**kwargs):
        ''' Replacement for steam call to provide higher detailed data'''
        url = urls.DOTABUFF_HEROES_DETAILED
        return url

    def fetchMatchHistoryBySeqNum(self,**kwargs):
        ''' 
        Structure API call to fetch match history by sequence number

        params---
        Any acceptable key word args
        API route specific args: start_at_match_seq_num: int
                                  matches_requested: int: amount of games to fetch
        returns---
        encoded call: url
        '''
        url=self.__buildReq(urls.GET_MATCH_HISTORY_BY_SEQ_NUM,language=self.language,**kwargs)
        if(self.logger):
            self.logger.info('URL built: {}'.format(url))
        return url

    def fetchItems(self,**kwargs):
        '''***ENDPOINT DOWN*** fetch from https://github.com/odota/dotaconstants/blob/master/build/items.json temporarily 
        structure API call to fetch all items (to localize ids)
        params---
        Any acceptable key word args

        returns---
        encoded call:url
        '''
        url= "{}{}contents/{}?ref=master".format(urls.GIT_BASE,urls.DOTA2_CONSTANTS_REPO, urls.DOTA2_CONSTANTS_ITEMS)
        if(self.logger):
            self.logger.info('URL built: {}'.format(url))
        return url
        #return self.__buildReq(urls.GET_GAME_ITEMS,language=self.language,**kwargs) removed while endpoint is down
    
    def fetchMatchDetails(self,**kwargs):
        '''Builds steam api call to match details endpoint
        :params match_id (string)
        :return encoded url (string)
        ''' 
        url = self.__buildReq(urls.GET_MATCH_DETAILS, **kwargs)
        if(self.logger):
            self.logger.info('URL built: {}'.format(url))
        return url 

    def fetchPublicMatches(self,**kwargs):
        '''OpenDota public matches call
        kwargs allowed---
        less_than_match_id: int : fetch matches with id lower than matches
        min_rank: int 10-85: set min rank for matches: herald 10-15 g 20-25 etc till 85 
        max_rank: int 10-85: max rank for matches
        mmr_ascending: int: order by avg rank
        mmr_descending: int: order by avg rank 
        returns---
        endcoded call: url

        note: does not use __buildReq as no key required for opendota
        ''' 
        url="{}{}?{}".format(urls.OPEN_DOTA_BASE,urls.GET_PUBLIC_MATCHES,urlencode(kwargs))
        if (self.logger):
            self.logger.info('URL built: {}'.format(url))
        return url
