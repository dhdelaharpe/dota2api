from pymongo import MongoClient, errors

class dbHandler(object):
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

    