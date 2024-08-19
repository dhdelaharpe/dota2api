import schedule 
import time 
import os

import apiHandler
import dbHandler
import parseData 

def cyclePopulateMatches(logging=None):
    '''populate dataset work loop - opendota public matches endpoint '''
    logger=None 
    if(logging):
        import logging
        logging.basicConfig(level=logging.NOTSET)
        logger=logging.getLogger(__name__)
    try:
    #setup 
        api = apiHandler.ApiHandler()
        db = dbHandler.dbHandler(os.getenv('MONGO_CONNECTION_STR'))
        parse = parseData.parseData()
        db.connect(id='match_id',dbName='dota2',collectionName='matches')
        if(logger):
            logger.info('Connected to db: {} collection: {}'.format('dota2', 'matches'))
        #work loop  
        data = api.sendRequest(api.fetchPublicMatches())
        if(logger):
            logger.info('Sent API request')
        parsed = parse.parsePublicMatchesOpenDota(data)
        if(logger):
            logger.info('Parsed data')
        db.insertData(parsed,True)
        if(logger):
            logger.info('New data inserted to db')
        db.endSession()
        if(logger):
            logger.info('db connection closed')
        print("Task completed successfully")
    except Exception as err:
        print('Error occurred: {}'.format(err))


schedule.every(15).minutes.do(cyclePopulateMatches)
while True:
    schedule.run_pending()
    time.sleep(300)
    print('sleeping for 5 minutes')