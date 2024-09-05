from pymongo import MongoClient, errors

from bson.son import SON
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
        retry = retry_if_exception_type((errors.ConnectionFailure, errors.ServerSelectionTimeoutError,errors.PyMongoError))
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
        except errors.ConnectionFailure as con_err:
            print('Connection Error: {}'.format(con_err))
        except errors.ServerSelectionTimeoutError as time_err:
            print('Timeout Error: {}'.format(time_err))
        except errors.PyMongoError as err:
            print('Error: {}'.format(err))
    @retry(
        stop=stop_after_attempt(3), #retry limit
        wait=wait_exponential(multiplier=1,min=4,max=10), #delay before retries
        retry = retry_if_exception_type((errors.ConnectionFailure, errors.ServerSelectionTimeoutError))
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
        retry = retry_if_exception_type((errors.ConnectionFailure, errors.ServerSelectionTimeoutError))
    )
    def updateData(self,data,many,query):
        '''add single data entry to collection
        params-- 
        data: json (dict)
        many: bool: multiple or single entry
        query: (dict) query to match to update against
        
        potentially added success/fail return for later application
        ''' 
        if self.collection is not None:
            try:
                op = {"$set":data} #use set to update -- is this even necessary?
                if(many):
                    res = self.collection.update_many(query, op)
                    print('{} entries updated'.format(res.modified_count))
                else:
                    res = self.collection.update_one(query,op)
                    if(self.logger):        #printing for each would slow down the application too much if used in a loop
                        self.logger.info('1 entry updated' if res.modified_count>0 else 'No entries matched')
            except errors.PyMongoError as err:
                print('error occured while updating data: {}'.format(err))
        else:
            print('Collection not found')

    @retry(
        stop=stop_after_attempt(3), #retry limit
        wait=wait_exponential(multiplier=1,min=4,max=10), #delay before retries
        retry = retry_if_exception_type((errors.ConnectionFailure, errors.ServerSelectionTimeoutError))
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
        retry = retry_if_exception_type((errors.ConnectionFailure, errors.ServerSelectionTimeoutError))
    )
    def searchData(self, query=None, filt=None,sort=None):
        ''' runs a query using pymongo find method
        params---
        query object: {'key':'value'} allows regex and some operators: empty=SELECT * 
        filter object: {'key':'value'}: optional: allows filtering of fields. e.g.{'id':0, 'name':1} returns just names no ids
        https://www.mongodb.com/docs/manual/reference/method/db.collection.find/
        
        returns---
        list(cursor): {'key':value} set of matching results
        '''
        if self.collection is not None:
            try:
                return list(self.collection.find(query,filt).sort(sort[0],sort[1]))
            except errors.PyMongoError as err:
                print('Error occured: {}'.format(err))
        else:
            print('Error: connect to db first')
    
    @retry(
        stop=stop_after_attempt(3), #retry limit
        wait=wait_exponential(multiplier=1,min=4,max=10), #delay before retries
        retry = retry_if_exception_type((errors.ConnectionFailure, errors.ServerSelectionTimeoutError))
    )   
    def findOne(self,query=None, filt=None, sort=None):
        '''runs query using pymongo findOne
        params---
        query object: {'key':'value'}
        filter object: {'key':'value'}
        sort object: ['key':value]
        returns---
        dict: {'key':value} result''' 
        if self.collection is not None:
            try:
                return (self.collection.find_one(filt,sort=sort))
            except errors.PyMongoError as err:
                print('Error occured: {}'.format(err))
        else:
            print('Error: connect to db first')

    def findAll(self, query=None,filt=None,sort=None):
        '''runs query using pymongo find'''
        if self.collection is not None:
            try:
                return (self.collection.find(filt,sort=sort))
            except errors.PyMongoError as err:
                print("Error occured: {}".format(err))
        else:
            print("Error: connect to collection first")
    def endSession(self):
        '''close connection to mongodb service'''
        if self.client:
            self.client.close()
            print('session ended')

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1,min=4,max=10),
        retry= retry_if_exception_type((errors.ConnectionFailure,errors.ServerSelectionTimeoutError))
    )
    def getHeroWinRateOverTime(self,interval='day', heroId=1):
        '''aggregates win rate of specific hero over time period
        params--
        interval: str: day/week/other=monthly
        hero: int: id of hero
        returns res:list
        ''' 
        if(self.collection is not None):
            try:
                if(interval=='day'):
                    dateFormat= '%Y-%m-%d'
                elif(interval=='week'):
                    dateFormat='%Y-%U'
                else:
                    dateFormat='%Y-%m'
                if(self.logger):
                    self.logger.info('date format selected {}'.format(dateFormat))                               
                aggrQuery=[#
                    {
                        '$match': {
                            '$or': [
                                {
                                    'radiant_team': heroId
                                }, {
                                    'dire_team': heroId
                                }
                            ]
                        }
                    }, {
                        '$addFields': {
                            'numeric_start_time': {
                                '$toDate': '$start_time'
                            }, 
                            'numeric_radiant_win': {
                                '$cond': {
                                    'if': {
                                        '$and': [
                                            {
                                                '$in': [
                                                    heroId, '$radiant_team'
                                                ]
                                            }, '$radiant_win'
                                        ]
                                    }, 
                                    'then': 1, 
                                    'else': 0
                                }
                            }, 
                            'numeric_dire_win': {
                                '$cond': {
                                    'if': {
                                        '$and': [
                                            {
                                                '$in': [
                                                    heroId, '$dire_team'
                                                ]
                                            }, {
                                                '$eq': [
                                                    '$radiant_win', False
                                                ]
                                            }
                                        ]
                                    }, 
                                    'then': 1, 
                                    'else': 0
                                }
                            }, 
                            'hero_in_radiant_team': {
                                '$in': [
                                    heroId, '$radiant_team'
                                ]
                            }, 
                            'hero_in_dire_team': {
                                '$in': [
                                    heroId, '$dire_team'
                                ]
                            }
                        }
                    }, {
                        '$group': {
                            '_id': {
                                '$dateToString': {
                                    'format': dateFormat, 
                                    'date': '$numeric_start_time'
                                }
                            }, 
                            'total_matches': {
                                '$sum': 1
                            }, 
                            'radiant_won': {
                                '$sum': '$numeric_radiant_win'
                            }, 
                            'dire_won': {
                                '$sum': '$numeric_dire_win'
                            }, 
                            'total_radiant_matches': {
                                '$sum': {
                                    '$cond': [
                                        '$hero_in_radiant_team', 1, 0
                                    ]
                                }
                            }, 
                            'total_dire_matches': {
                                '$sum': {
                                    '$cond': [
                                        '$hero_in_dire_team', 1, 0
                                    ]
                                }
                            }
                        }
                    },     {
                        '$project': {
                            '_id': 0,
                            'date': '$_id',
                            'overall_win_rate': {
                                '$multiply': [
                                    {
                                        '$cond': {
                                            'if': {'$gt': [{'$add': ['$total_radiant_matches', '$total_dire_matches']}, 0]},
                                            'then': {
                                                '$divide': [
                                                    {'$add': ['$radiant_won', '$dire_won']},
                                                    {'$add': ['$total_radiant_matches', '$total_dire_matches']}
                                                ]
                                            },
                                            'else': 0
                                        }
                                    },
                                    100
                                ]
                            },
                            'radiant_win_rate': {
                                '$multiply': [
                                    {
                                        '$cond': {
                                            'if': {'$gt': ['$total_radiant_matches', 0]},
                                            'then': {'$divide': ['$radiant_won', '$total_radiant_matches']},
                                            'else': 0
                                        }
                                    },
                                    100
                                ]
                            },
                            'dire_win_rate': {
                                '$multiply': [
                                    {
                                        '$cond': {
                                            'if': {'$gt': ['$total_dire_matches', 0]},
                                            'then': {'$divide': ['$dire_won', '$total_dire_matches']},
                                            'else': 0
                                        }
                                    },
                                    100
                                ]
                            },
                            'total_radiant_matches': '$total_radiant_matches',
                            'total_dire_matches': '$total_dire_matches'
                        }
                    },
                    {
                        '$sort': {
                            'date': 1
                        }
                    }
                ]
                return list(self.collection.aggregate(aggrQuery))
            except errors.PyMongoError as err:
                print('Error occured {}'.format(err))
   
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1,min=4,max=10),
        retry= retry_if_exception_type((errors.ConnectionFailure,errors.ServerSelectionTimeoutError))
    )
    def getWinRateOverTime(self, interval='day'):
        ''' aggregates data by time and calculates win rate
        params--- 
        interval: str: day/week/other=monthly
        return: res: list
        '''
        if(self.collection is not None):
            try:
                if(interval=='day'):
                    dateFormat= '%Y-%m-%d'
                elif(interval=='week'):
                    dateFormat='%Y-%U'
                else:
                    dateFormat='%Y-%m'
                if(self.logger):
                    self.logger.info('date format selected {}'.format(dateFormat))
                
                aggrQuery=[#convert date from str to date format => order by date => calc total matches=> calc percent winrate =>sort by date
                    {
                        "$addFields":{
                            "numeric_start_time":{"$toDate":"$start_time"},
                            "numeric_radiant_win":{"$cond":{"if":"$radiant_win","then":1,"else":0}}
                        }
                    },
                    {
                        "$group":{ 
                            "_id":{
                                "$dateToString": {"format": dateFormat,"date":"$numeric_start_time"}
                            },
                            "total_matches":{"$sum":1}, 
                            "radiant_won":{"$sum":"$numeric_radiant_win"}
                        }
                    },
                    {
                        "$project":{
                            "date": "$_id",
                            "win_rate":{"$multiply":[{"$divide": ["$radiant_won","$total_matches"]},100]},
                            "_id": 0,
                            "total_matches":"$total_matches"
                        }
                    },
                    {
                        "$sort": SON([("date",1)])
                    }
                ]
                return list(self.collection.aggregate(aggrQuery)) 
            except errors.PyMongoError as err:
                print("Error occured  {}".format(err))
        else:
            print("Connect to db first")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1,min=4,max=10),
        retry= retry_if_exception_type((errors.ConnectionFailure,errors.ServerSelectionTimeoutError))
    )
    def getAggregate(self,query):
        '''run general aggregate queries
        params---
        query: list: pipeline to aggregate
        returns---
        result:list: pymongo response in after list conversion
        '''
        if(self.collection is not None):
            try:
                return list(self.collection.aggregate(query))
            except errors.PyMongoError as err:
                print('Error occured {}'.format(err))
        else:
            print("Connect to db first")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1,min=4,max=10),
        retry= retry_if_exception_type((errors.ConnectionFailure,errors.ServerSelectionTimeoutError))
    )
    def countEntries(self,query={}):
        '''return count documents on query
        params---
        query:dict: filter to use for count: default empty
        returns: int: result'''
        if(self.collection is not None):
            try:
                return self.collection.count_documents(query)
            except errors.PyMongoError as err:
                print('Error occured {}'.format(err))
        else:
            print('Connect to a collection first')