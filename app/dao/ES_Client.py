import sys
from elasticsearch import Elasticsearch, ElasticsearchException


class ES_Client:
    es = ""
    resultSet = []
    queries = {}
    idx = ""
    q = ""
    scroll = '1m'
    scrollSize = 10  # default scroll size to 10
    return_size = "all"

    # q = {"query":{"match_all": {}},"size": 0,"aggs": {"max_date": {"max": {"field": "date"}}}}
    # q = {"query": { "range": {"date": {"gt": 20200328}}}}

    def __init__(self, idx):
        self.idx = idx
        self.getClient()
        self.queries = {
            "getMaxDate": '{"query": {"match_all": {}}, "sort": [{"date": {"order": "desc"}}], "_source": "date"}',

            "getMinDate": '{"query": {"match_all": {}}, "sort": [{"date": {"order": "asc"}}], "_source": "date"}',

            "atHocQuery": ''
        }

    def getQueries(self):
        return self.queries

    def setKwargs(self, **kwargs):

        self.q = kwargs['q']
        try:
            var = kwargs['return_size']
            self.return_size = int(kwargs['return_size'])
        except:
            pass  # use default value

        try:
            var = kwargs['scroll']
            self.scroll = kwargs['scroll']
        except:
            pass  # use default value

        try:
            var = kwargs['scrollSize']
            self.scrollSize = kwargs['scrollSize']
        except:
            pass  # use default value

    def getClient(self):
        self.es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])

    def checkIndex(self, index):
        return self.es.indices.exists(index=index)

    def deleteIndex(self):
        msg = self.idx + " not found"
        if self.checkIndex(self.idx):
            self.es.indices.delete(index=self.idx)
            msg = "The " + self.idx + " index has been deleted"
        print(msg)

    def insert(self, doc_type, dataset):
        imported = 0
        failed = 0
        for doc in dataset:
            try:
                self.es.index(index=self.idx, ignore=400, doc_type=doc_type, id=doc['hash'], body=doc)
                imported += 1
                i = "\rimporting: %s" % imported
                sys.stdout.write(i)

            except ElasticsearchException as esx:
                failed += 1
                print("ElasticsearchException: ", esx)
        print("", end='\r')
        print("records attempted: " + str(len(dataset)) + "\n records imported: " + str(imported) + "\n   records failed: " + str(failed))

    def insertDoc(self, doc):
        try:
            self.es.index(index=self.idx, doc_type='_doc', id=doc['hash'], body=doc)
        except ElasticsearchException as esx:
            print("ElasticsearchException: ", esx)

    def deleteDoc(self, doc_id):
        try:
            self.es.delete(index=self.idx, id=doc_id)
        except ElasticsearchException as esx:
            print("ElasticsearchException: ", esx)

    # query function receives
    def query(self, **kwargs):  # kwargs: q='Required: query name found in self.queries', scroll='Optional: time allotted for incremented search/response: defaults to 1m' scroll_size='Optional: size of each scroll increment: defaults to 10', return_size='Optional: numeric size of records to return: defaults to all'
        self.setKwargs(**kwargs)  # analyze kwargs and set as appropriate

        self.resultSet.clear()  # clear global result array for this query
        qr = self.es.search(index=self.idx, scroll=self.scroll, size=self.scrollSize, body=self.queries[self.q])  # execute initial search and retrieve scroll_id and set size
        sid = qr['_scroll_id']
        scroll_size = qr['hits']['total']['value']
        self.getInnerHits(qr)  # get the _source data and place in global array resultSet

        while int(scroll_size) > 0:  # iterate/scroll the remainder of the query based on scroll_id and scroll size
            qr = self.es.scroll(scroll_id=sid, scroll=self.scroll)
            sid = qr['_scroll_id']  # Update the scroll ID
            scroll_size = len(qr['hits']['hits'])  # Get the number of results that we returned in the last scroll
            self.getInnerHits(qr)  # get the _source data and place in global array resultSet

        if self.return_size == "all":  # return all records in resultSet
            return self.resultSet  # return an array of dictionaries containing _source data
        else:  # return the size requested from the resultSet
            return self.resultSet[0:self.return_size]  # return an array of dictionaries containing _source data

    def getInnerHits(self,qr):  # get the source ('_source') data from an elasticsearch query and place in global array resultSet
        for hit in qr['hits']['hits']:
            self.resultSet.append(hit['_source'])
