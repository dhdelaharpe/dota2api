#imports
# standard libraries 
import os 
import csv
import json
from dotenv import load_dotenv, find_dotenv
from pathlib import Path 


import numpy as np 
import pandas as pd 
import requests
from urllib.parse import urlencode
import urls
#load environment to access API key 
load_dotenv(".env")
#os.getenv("DOTA2_API_KEY") -> to access 
class ApiHandler(object):
    def __init__(self,api_key=None,language=None,request_exec=None):
        '''params---
            api_key = steam web api key ->required to be provided or exist in environment variable 
            language = localization to call in steamapi 
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

    def sendRequest(self,call):
        '''
        Return json response from get request
        Splitting various common errors to allow handling separately (e.g. adding wait time to resend to timeout etc)
        params---
        call: str
        returns---
        json data
        '''

        try:
            response  = requests.get(call)            
            response.raise_for_status()
            return response.json()
        
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

    def fetch_heroes(self,**kwargs):
        '''
        Structure API call to fetch heroes
        
        params---
        Any acceptable key word args 
        
        returns---
        encoded call: url 
        '''
        return self.__buildReq(urls.GET_HEROES,language=self.language,**kwargs) #build url and return it
        
        



run = ApiHandler()
url = run.fetch_heroes()
res = run.sendRequest(url)
dump_df = pd.DataFrame.from_dict(res, orient='index')
dump_df.to_csv('dump.csv')