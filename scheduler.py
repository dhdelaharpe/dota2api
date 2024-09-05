import schedule 
import time 
import os
import argparse
import apiHandler
import dbHandler
import parseData 
import pymongo

def cyclePopulateMatches(logging=None, source="OpenDota",seqNum=None):
    '''populate dataset work loop - opendota public matches endpoint 
    ''params--
        logging: bool: enable logging
        source:str: options[OpenDota, Steam] default OpenDota
        seqNum: int: sequence number to use for steam call'''
    logger=None 
    if(logging==True):
        import logging
        logging.basicConfig(level=logging.NOTSET)
        logger=logging.getLogger(__name__)
    try:
    #setup 
        api = apiHandler.ApiHandler()
        db = dbHandler.dbHandler(os.getenv('MONGO_CONNECTION_STR'))
        parse = parseData.parseData()
        if(source=='Steam'):
            collectionName= 'matches_steam'
        else:
            collectionName='matches'
        db.connect(id='match_id',dbName='dota2',collectionName=collectionName)
        if(logger):
            logger.info('Connected to db: {} collection: {}'.format('dota2', collectionName))
        
        #request+parse
        if(source=="OpenDota"):
            data = api.sendRequest(api.fetchPublicMatches())
            if(logger):
                logger.info('Sent API request')
            parsed = parse.parsePublicMatchesOpenDota(data)
        elif(source=="Steam"):
            data = api.sendRequest(api.fetchMatchHistoryBySeqNum(**{"seqNum":seqNum, "matches_requested":200}))
            if(logger):
                logger.info('Sent Steam API request')
            parsed = parse.parseMatchesSteam(data)
        if(logger):
            logger.info('Parsed data')
        
        #insert + close 
        db.insertData(parsed,True)
        if(logger):
            logger.info('New data inserted to db')
        db.endSession()
        if(logger):
            logger.info('db connection closed')
        print("Task completed successfully")
    except Exception as err:
        print('Error occurred: {}'.format(err))

def getLatestSequenceNumber():
    '''check db for last steam sequence number added
    returns---
        seqNum: int''' 
    db = dbHandler.dbHandler(os.getenv("MONGO_CONNECTION_STR"))
    db.connect(id="match_id",dbName="dota2",collectionName="matches_steam")
    filt = {"match_seq_num":1}
    sort = [('match_seq_num',pymongo.DESCENDING)]
    return db.findOne(filt,sort=sort)['match_seq_num']

def fetchDetails(api,db,matchId=None, matchSeqNum=None, logging=None):
    '''get details from steam endpoint of matchid ** endpoint down making workaround by using sequence number
    :params: api (apiHandler obj) instance to use
    :paraps: db (dbHandler obj) instance to use
    :params (int) matchid 
    :return (dict) response''' 
    #api = apiHandler.ApiHandler()
    #db = dbHandler.dbHandler(os.getenv("MONGO_CONNECTION_STR"))
    #db.connect(id="match_id", collectionName="matches2", dbName="dota2")
    #handle logging 
    logger=None 
    if(logging==True):
        import logging
        logging.basicConfig(level=logging.NOTSET)
        logger=logging.getLogger(__name__)
    try:
        if(matchSeqNum is not None):
            url = api.fetchMatchHistoryBySeqNum(**{"start_at_match_seq_num":matchSeqNum, "matches_requested":1}) #use sequence num endpoint but just request 1 game
            data = api.sendRequest(url)
            data=data['result']['matches'][0] #endpoint returns array of matches but we just want 1
            if(logger):
                logger.info("Successfully fetched match details using sequence number: {}".format(matchSeqNum))
        else:
            url = api.fetchMatchDetails(**{"match_id":matchId})
            data=api.sendRequest(url)
            if(logger):
                logger.info("Successfully fetched match details using match id: {}".format(matchId))
        return data 
    except KeyError as err:
        print("Key error: {}".format(err))
    except Exception as e:
        print("Error occured fetching details {}".format(e))
    return None 


def updateDetails(match,db):
    ''' handle preparing to merge with db
    :params: match (dict) 
    '''
    try:
        #first fetch corresponding match from db
        seq_num = match["match_seq_num"]
        #we want the old duration as that has been parsed already so delete that entry
        del match["start_time"]
        match["detailed"]= True         #adding this to track which matches have been expanded to include full details 
        db.updateData(match,many=False, query={"match_seq_num":seq_num})
    except errors.PyMongoError as err:
        print("DB error occured: {}".format(err))
    except Exception as e:
        print("Exception : {}".format(e))


def mergeMatches(logging=None):
    '''check db for entries without details, fetch those details from steam api, merge into db '''
    logger=None 
    if(logging==True):
        import logging
        logging.basicConfig(level=logging.NOTSET)
        logger=logging.getLogger(__name__)
    try:
        #get matches to update
        db = dbHandler.dbHandler(os.getenv("MONGO_CONNECTION_STR"))
        db.connect(dbName="dota2", collectionName="matches", id="match_id")
        matchesToExpand = db.findAll(filt={"detailed": {"$exists":False}}) # could also just filter for this value being true but may as well use mongo feature to make it slightly faster
        if(logger):
            logger.info("Found {} matches to update".format(db.collection.count_documents({"detailed":{"$exists":False}})))
        print("Found {} matches to update".format(db.collection.count_documents({"detailed":{"$exists":False}})))
        #make 1 API instance 
        api = apiHandler.ApiHandler()
        for match in matchesToExpand: #loop through and update each 
            seq_num = match.get("match_seq_num")
            if not seq_num:
                #perhaps add some indication through logger or print? 
                continue
            detailed = fetchDetails(api,db,matchSeqNum=seq_num) #make call
            updateDetails(detailed,db) #update entry 
            if(logger):
                logger.info("Updated match {}".format(match.get("match_id")))
        db.endSession()
        if(logger):
            logger.info("DB session closed")
        print("Detailed update task completed")
    except Exception as e:
        print("Exception occured : {}".format(e))

#handle input options to set scheduler
parser= argparse.ArgumentParser('E.g.')
parser.add_argument("--source",help="Source API to use: valid options:[Steam, OpenDota (default if none given)]",type=str,default="OpenDota",required=False, dest="source")
#parser.add_argument("--seqNum", help="Required for steamAPI, sequenceNumber to start from",type=int,default=None,required=False,dest="seqNum")
parser.add_argument("--logging",help="Enable logging, True/False",type=bool,default=False, required=False,dest="logging")
args = parser.parse_args()
source = args.source 
#seqNum = args.seqNum
logging = args.logging 

'''
api=apiHandler.ApiHandler()
db = dbHandler.dbHandler(os.getenv("MONGO_CONNECTION_STR"))
data = api.sendRequest(api.fetchHeroes())
parse = parseData.parseData()
parsed = parse.parseHeroesSteam(data)
f = open("hero.json","w")
import json 
json.dump(parsed,f)
f.close()

'''
#schedule tasks
if(source=='OpenDota'):
    schedule.every(10).minutes.do(cyclePopulateMatches,[logging,source])
    schedule.every(30).minutes.do(mergeMatches,logging)
    while True:
        schedule.run_pending()
        time.sleep(300)
        print('sleeping for 5 minutes')
if(source=='Steam'):  ### TODO ::::this should match above
    while True:
        time.sleep(5)
        print('sleeping for 5 seconds')
        seqNum=getLatestSequenceNumber()
        print("current seqNum:{}".format(seqNum))
        cyclePopulateMatches(logging,source,seqNum)

