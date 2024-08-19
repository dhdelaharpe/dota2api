from pymongo import MongoClient, errors
#retry library
from tenacity import retry,stop_after_attempt, wait_exponential, retry_if_exception_type
class dbHandler(object):
    def __init__(self,conStr,logging=None):
        '''params---
            conStr: mongodb connection string/uri: str 
            dbName: name of db to connect to: str
            collectionName: name of collection within db to connect to: str

            attr- client db and collection to be assigned once connection established
            '''
        self.conStr=conStr
        self.dbName=None
        self.collectionName=None
        self.client=None 
        self.db=None
        self.collection=None 
        if(logging):
            import logging
            logging.basicConfig(level=logging.NOTSET)
            self.logger=logging.getLogger(__name__)
        else:
            self.logger=None
    @retry(
        stop=stop_after_attempt(3), #retry limit
        wait=wait_exponential(multiplier=1,min=4,max=10), #delay before retries
        retry = retry_if_exception_type(errors.ConnectionError, errors.ServerSelectionTimeoutError,errors.PyMongoError)
    )
    def connect(self, dbName=None, collectionName=None, id=None):
        '''create connection to mongodb server, assigns client db and collection of dbHandler class
        params---
        id: str: name of field to be used if unique index required
        dbName: str: name of db
        collectionName:str name of collection to use
        '''
        try:
            
            self.client=MongoClient(self.conStr)
            if(dbName is not None):
                self.dbName=dbName 
            self.db=self.client[self.dbName]
            if(collectionName is not None):
                self.collectionName=collectionName
            self.collection=self.db[self.collectionName]
            if(id is not None):
                self.collection.create_index(id,unique=True)
            print('connection established')
            if(self.logger):
                self.logger.info('DB connection established: db={} col={}'.format(self.collectionName,self.dbName))
        except errors.ConnectionError as con_err:
            print('Connection Error: {}'.format(con_err))
        except errors.ServerSelectionTimeoutError as time_err:
            print('Timeout Error: {}'.format(time_err))
        except errors.PyMongoError as err:
            print('Error: {}'.format(err))
    @retry(
        stop=stop_after_attempt(3), #retry limit
        wait=wait_exponential(multiplier=1,min=4,max=10), #delay before retries
        retry = retry_if_exception_type(errors.ConnectionError, errors.ServerSelectionTimeoutError)
    )
    def insertData(self,data,many):
        '''add single data entry to collection
        params-- 
        data: json 
        many: bool: multiple or single entry
        
        potentially added success/fail return for later application
        ''' 
        if self.collection is not None:
            try:
                if(many):
                    res = self.collection.insert_many(data)
                    print('{} entries inserted'.format(res.inserted_ids))
                else:
                    res = self.collection.insert_one(data)
                    print('data inserted')
            except errors.PyMongoError as err:
                print('error occured while inserting data: {}'.format(err))
        else:
            print('Collection not found')
    @retry(
        stop=stop_after_attempt(3), #retry limit
        wait=wait_exponential(multiplier=1,min=4,max=10), #delay before retries
        retry = retry_if_exception_type(errors.ConnectionError, errors.ServerSelectionTimeoutError)
    )        
    def deleteData(self,query,many):
        '''runs query to delete data from connected db
        params---
        query: dict: to match against entries to delete
        many: bool: remove first matching entry if False or all if True
        ''' 
        if self.collection is not None:
            try:
                if(many):
                    res= self.collection.delete_many(query)
                    print('deleted {} entries'.format(res.deleted_count))
                else:
                    res = self.collection.delete_one(query)
                    print('deleted 1 entry')
            except errors.PyMongoError as err:
                print('Error occured: {}'.format(err))
        else:
            print('connect to db first')

    @retry(
        stop=stop_after_attempt(3), #retry limit
        wait=wait_exponential(multiplier=1,min=4,max=10), #delay before retries
        retry = retry_if_exception_type(errors.ConnectionError, errors.ServerSelectionTimeoutError)
    )
    def searchData(self, query=None):
        ''' runs a query using pymongo find method
        params---
        query object: {'key':'value'} allows regex and some operators: empty=SELECT * 
        filter object: {'key':'value'}: optional: allows filtering of fields. e.g.{'id':0, 'name':1} returns just names no ids
        https://www.mongodb.com/docs/manual/reference/method/db.collection.find/
        
        returns---
        cursor: {'key':value} set of matching results
        '''
        if self.collection is not None:
            try:
                return self.collection.find(query)
            except errors.PyMongoError as err:
                print('Error occured: {}'.format(err))
        else:
            print('Error: connect to db first')

    def endSession(self):
        '''close connection to mongodb service'''
        if self.client:
            self.client.close()
            print('session ended')

    