from pymongo import MongoClient, errors

class dbHanlder(object):
    def __init__(self,conStr, dbName,collectionName):
        '''params---
            conStr: mongodb connection string/uri: str 
            dbName: name of db to connect to: str
            collectionName: name of collection within db to connect to: str

            attr- client db and collection to be assigned once connection established
            '''
        self.conStr=conStr
        self.dbName=dbName 
        self.collectionName=collectionName
        self.client=None 
        self.db=None
        self.collection=None 
    
    def connect(self):
        '''create connection to mongodb server, assigns client db and collection of dbHandler class'''
        try:

            self.client=MongoClient(self.conStr)
            self.db=self.client[self.dbName]
            self.collection=self.db[self.collectionName]

            print('connection established')
        except errors.ConnectionError as con_err:
            print('Connection Error: {}'.format(con_err))
        except errors.ServerSelectionTimeoutError as time_err:
            print('Timeout Error: {}'.format(time_err))
        except errors.PyMongoError as err:
            print('Error: {}'.format(err))

    def insertData(self,data):
        '''add single data entry to collection
        params-- 
        data: json 
        
        potentially added success/fail return for later application
        ''' 
        if self.collection:
            try:
                res = self.collection.insert_one(data)
                print('data inserted')
            except errors.PyMongoError as err:
                print('error occured while inserting data: {}'.format(err))
        else:
            print('Collection not found')
            
