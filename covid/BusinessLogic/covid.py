import os
import json
import csv
import requests
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from DataLayer.ElasticSearch.ES_Client import ES_Client


# Function: client interface to local elasticsearch instance
#   Specific Functions:
#         On __init__
#           Retrieve covid data via URL request and place in a global array as json documents
#           Instantiate elasticsearch client with index configuration
#         On call
#           setKwargs()
#               called when doData() is called
#               pass variable number of keyword arguments depending on required action
#               analyze keyword arguments and respond to user if incorrect
#           setStartDate()
#               called when action == insertLatest
#               queries and retrieves the latest date found in covid data
#                   only data after this date is inserted into elasticsearch instance
#           Retrieve latest data from URL and insert 'differential' into local index
#           Delete an index
#           Delete a specific document by id
#           Delete a range of documents (from-to dates)
#           Execute scroll queries
#               configure return size (optional)
#               only need to pass a query name (name looked up in queries dictionary on ES_Client
#           Export data formatted as:
#               Kibana.json (can be imported by Kibana import tool)
#               Elasticsearch.json (standard elasticsearch format)
#               CSV (with headers)
#
#   Input Params:
#           action (required)
#               insertLatest
#                   Params: action='insertLatest'
#                   insert most recent data NOT currently in elasticsearch instance
#               deleteIndex
#                   Params: action='deleteIndex'
#                   deletes the default index defined in the global index variable
#               deleteDoc
#                   Params: action='deleteDoc', doc_id='a document id'
#                   doc_id required
#                   deletes a single document
#               deleteDocs
#                   Params: action='deleteDocs', frm='20200301', to='20200401'
#                   frm-to date range optional
#                   delete a set/range of documents matching frm-to date range
#                   deletes all documents if no frm-to date range provided
#               query
#                   Params: action='query', q='name of query', return_size='1'
#                   return_size optional; defaults to 'all'
#                   set return_size to modulate the number of records to return
#                   scroll based query returning a record set structured as an array of dictionaries
#               export
#                   Params: action='export', target='typeOfExport', fqp='fully qualified path'
#                       target (required)
#                           options: 'CSV', 'KI' (kibana importable), 'ES' (standard elasticsearch format)
#                       fqp (optional)
#                           default: DataLayer/Data/export/YYYYmmdd.fileExtension (file extension set based on target selection)
#                           fqp='directory/filename'
#                       fromDate (optional)
#                           specify the starting point in time to export (e.g. 20200301)
#                           defaults to 20190101 if blank

class Covid:
    esClient = ""  # elasticsearch client
    data = ""  # temp holder for data retrieved from covid site
    jsonAry = []  # temp array for json data (used in export as a differential holder for covidAry)
    covidAry = []  # temp array for json data
    dt = datetime.datetime(2019, 1, 1)  # set default date well prior to covid event
    startDate = dt.strftime('%Y') + dt.strftime('%m') + dt.strftime('%d')  # default to 20190101

    # global params
    idx = ''  # elasticsearch index
    action = ""  # action to be performed
    doc_id = ""  # id of a specific document (used in deleteDoc)
    doc_type = "_doc"
    q = ""  # the body of a query
    return_size = "all"
    target = ""  # a target data format (ES, KI, CSV) used in export
    fqp = ""  # a fully qualified path (used in export)
    fromDate = ""  # from date (used in export)
    frm = ""  # from date (used in deleteDocs date range)
    to = ""  # to date (used in deleteDocs date range)
    msg = ""  # message printed to user
    # global params

    def __init__(self, idx):
        self.idx = idx  # elasticsearch index
        self.esClient = ES_Client(self.idx)  # elasticsearch client (configured with index)
        self.data = requests.get('https://covidtracking.com/api/v1/states/daily.json').content  # retrieve the latest data
        self.covidAry = json.loads(self.data)  # place in array of JSON objects
    #   https://covidtracking.com/api/states/daily

    def setKwargs(self, **kwargs):  # unpack and analyze keyword arguments and set passed == True/False accordingly; if False, stop execution and print message (self.msg) to user
        passed = True
        try:
            kwargs['action']
            self.action = kwargs['action']
            if self.action == 'query':
                try:
                    kwargs['q']
                    self.q = kwargs['q']
                except:
                    self.msg += "query param cannot be blank\n"
                    passed = False

                try:
                    kwargs['return_size']
                    self.return_size = kwargs['return_size']
                except:
                    pass

            elif self.action == 'deleteDoc':
                try:
                    kwargs['doc_id']
                    self.doc_id = kwargs['doc_id']
                except:
                    self.msg += "doc_id param cannot be blank\n"
                    passed = False

            elif self.action == 'insertLatest':
                try:
                    kwargs['doc_type']
                    self.doc_type = kwargs['doc_type']
                except:
                    self.msg += "you must pass a doc_type\n"
                    passed = False

            elif self.action == 'deleteDocs':
                try:
                    kwargs['frm']
                    self.frm = kwargs['frm']
                    kwargs['to']
                    self.to = kwargs['to']
                except:
                    self.msg += "cannot determine date range between " + self.frm + " and " + self.to + "\n"
                    passed = False

            elif self.action == 'export':
                try:
                    kwargs['target']
                    self.target = kwargs['target']
                except:
                    self.msg += "target param cannot be blank\n"
                    passed = False
                try:
                    kwargs['fqp']
                    self.fqp = kwargs['fqp']
                except:
                    self.msg += "fqp param cannot be blank\n"
                    passed = False
                try:
                    kwargs['frm']
                    self.fromDate = kwargs['frm']
                except:  # fromDate optional
                    pass


        except:
            self.msg += "action must be set to one of the following [insertLatest, deleteIndex, deleteDoc, deleteDocs, query, export ]\n"
            passed = False

        return passed

    def doData(self, **kwargs):  # execute requested action
        self.msg = ""
        if not self.setKwargs(**kwargs):
            print(self.msg)  # something wrong with arguments; passed = False; end execution with a print out of error
            return

        dataset = []
        if self.action == "insertLatest":

            if self.esClient.checkIndex(self.idx):  # check if index exists prior to setting start date; if no index, no need to set start date
                self.setStartDate()  # get and set the highest date found in the covid-19 dataset

            for doc in self.covidAry:  # iterate over array of dictionaries
                if int(doc['date']) > int(self.startDate):  # only write documents with a date greater than startDate (startDate defaulted to 20190101 if not set)
                    dataset.append(doc)
            self.esClient.insert('_doc', dataset)  # send to ES_Client for insert

        elif self.action == "deleteIndex":
            self.esClient.deleteIndex()

        elif self.action == "deleteDoc":
            self.esClient.deleteDoc(self.doc_id)

        elif self.action == "deleteDocs":
            for doc in self.covidAry:
                if int(self.frm) <= int(doc['date']) <= int(self.to):
                    self.esClient.deleteDoc(doc['hash'])

        elif self.action == "query":
            # query = {"query": {"range": { "date": {"gt": 20200101}}},"size":10000}
            # ***NOTE: all queries are defined in a dictionary on ES_Client.py, set 'q' equal to the name of the query
            results = self.esClient.query(q=self.q, return_size=self.return_size)
            return results

        elif self.action == 'export':
            self.export()

        print(self.msg)

    def setStartDate(self):  # find and set the maximum data found in covid-19 ES instance
        results = self.esClient.query(q='getMaxDate', return_size=1)
        if len(results) > 0:  # found data in covid-19 instance; if none found go with default startDate of one year ago (to capture all data)
            self.startDate = results[0]['date']
        print("startDate: ", self.startDate)

    def export(self):
        if self.fromDate == '':
            self.fromDate = self.startDate
        else:
            if not self.fromDate.isnumeric() or len(self.fromDate) != 8:  # date must be 8 digit long integer
                print("Incorrect Date Format: Date must be in YYYYmmdd format (e.g. 20200301)")
                return

        self.jsonAry.clear()

        defaultDir = 'DataLayer/Data/export/'

        fileType = '.json'
        if self.target == 'CSV':
            fileType = '.csv'

        if self.fqp == '':  # just set to default directory and file name
            set.fqp = 'DataLayer/Data/export/' + self.fromDate + fileType
        else:  # fix it up if needed
            if self.fqp.find("/") < 0:  # no directory specified, set to default directory
                self.fqp = defaultDir + self.fqp

            if self.fqp.find(".") < 0:  # file extension missing, tack it on the end
                self.fqp += fileType

        if os.path.exists(self.fqp):  # if the export file exists remove it
            os.remove(self.fqp)

        try:
            exportFile = open(self.fqp, 'a')  # create an appendable export file
        except Exception as fileEx:  # directory doesn't exist; stop processing
            print("File Exception: ", fileEx)
            return
        numRecords = 0
        # file directory, name, and extension all configured; build the export contents
        if self.target == "KI":  # create a KI happy json file (everything in quotes, dictionaries separated by new lines, fix the dateChecked field)
            for doc in self.covidAry:
                if int(doc['date']) >= int(self.fromDate):
                    self.jsonAry.append(str(doc).replace("'", '"'))  # create an array hosting each JSON document from original file (and get rid of all ' marks)

            newLine = ""
            for d in self.jsonAry:  # iterate and fix/reconstruct each JSON document
                line = d.replace("{", "").replace("}", "")  # remove the {} brackets (we'll put them back in at the right time)
                line = line.split(",")  # get each key/value pair
                for el in line:  # iterate key/value pairs
                    elData = el.split(":")  # separate key from value and place in an array (so we can 'work' on each value)
                    if elData[1].find('\"') < 0:  # look for quotes in each value, if quotes not found, add them (and remove white-space)
                        elData[1] = '"' + elData[1].strip() + '"'

                    if elData[0].find("dateChecked") > 0:  # the dateChecked field has ':' in it so the previous split(':') method truncated this value -- fix it
                        elData[1] += ":00:00Z\""  # during key/value split on ":", :00:00Z was truncated off each dateChecked value, put it back on (with a trailing quote)
                    newLine += elData[0] + ":" + elData[1] + ","  # rebuild a new line by adding each key/value pair
                newLine = "{" + newLine[0:len(newLine) - 1] + "}\n"  # encase the line in {}, remove the trailing comma, and add a new line '\n'
                exportFile.write(newLine)  # write this line into the new covid file
                newLine = ""  # clear newLine for the next iteration

            numRecords = len(self.jsonAry)

        elif self.target == "ES":
            data = ""
            for doc in self.covidAry:
                if int(doc['date']) >= int(self.fromDate):
                    data += str(doc) + ","
            exportFile.write("[" + data.replace("'", '"').replace("None", "\"None\"").rstrip(",") + "]")

            numRecords = len(self.covidAry)

        elif self.target == "CSV":
            for doc in self.covidAry:
                if int(doc["date"]) >= int(self.fromDate):
                    self.jsonAry.append(doc)

            headers = self.jsonAry[0].keys()
            writer = csv.DictWriter(exportFile, headers)
            writer.writeheader()
            writer.writerows(self.jsonAry)

            numRecords = len(self.jsonAry)

        self.msg += "Exported " + str(numRecords) + " records to: " + os.path.abspath(os.path.dirname(__file__)) + "/" + self.fqp

    def curate(self):
        tmpAry = []
        covidNewAry = []
        with open('DataLayer/Data/source/populationByState_2019.csv') as statePop:
            reader = csv.DictReader(statePop)
            for rows in reader:
                for doc in self.covidAry:
                    if rows['digraph'] == doc['state']:
                        dateSrt = str(doc['date'])
                        y = dateSrt[0:4]
                        m = dateSrt[4:6]
                        d = dateSrt[6:8]
                        doc['dateTrack'] = y + '-' + m + '-' + d + 'T12:00:00Z'

                        try:
                            doc['deathIncrease']
                            if int(doc['deathIncrease']) < 0:  # at times states will publish a negative death increase to adjust for reporting errors; kibana throws a wobbly when this value is < 0
                                doc['deathIncrease'] = 0  # set values < 0 to 0
                        except:
                            doc['deathIncrease'] = 0  # deathIncrease value did not exist or was NoneType, set it to 0

                        try:
                            doc['death'] > 0
                            death = int(doc['death'])
                        except:
                            death = 0
                        try:
                            doc['deathPerCapita'] = death / int(rows['population']) * 10000
                        except:
                            doc['deathPerCapita'] = 0

                        try:
                            doc['hospitalizedCumulative']
                            doc['hospitalizedPerCapita'] = int(doc['hospitalizedCumulative']) / int(rows['population']) * 10000
                        except:
                            doc['hospitalizedPerCapita'] = 0

                        try:
                            doc['inIcuCumulative']
                            doc['icuPerCapita'] = int(doc['inIcuCumulative']) / int(rows['population']) * 10000
                        except:
                            doc['icuPerCapita'] = 0

                        try:
                            doc['positive']
                            try:
                                death > 0
                                int(doc['positive']) > 0
                                doc['mortalityRate'] = round(death / int(doc['positive']), 3) * 100
                                doc['survivalRate'] = round(100 - int(doc['mortalityRate']), 3)
                            except:
                                doc['mortalityRate'] = 0
                                doc['survivalRate'] = 0
                        except Exception as mex:
                            doc['mortalityRate'] = 0
                            doc['survivalRate'] = 0
                            print("mortalityRate Exception: ", mex)

                        tmpAry.append(doc)  # capture newly constructed and current values

                for tmpDoc in tmpAry:  # store newly constructed and current values in a new covidAry (for later rewrite of self.covidAry)
                    covidNewAry.append(tmpDoc)
                tmpAry.clear()

        self.covidAry.clear()  # now that all the data in covidAry has been iterated, clear it for rewrite with newly constructed and current values
        self.covidAry = covidNewAry  # rewrite self.covidAry with newly constructed and current values
        print("length self.covidAry: ", len(self.covidAry))
        self.doData(action='export', target='ES', fqp='currentCovid', frm='20200101')

    def doLR(self, state):
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        covidDF = pd.read_json('DataLayer/Data/export/currentCovid.json')
        stateDF = covidDF.loc[covidDF['state'] == state]
        print(stateDF)
        xVals = pd.Series([num+1 for num in range(len(stateDF))])  # create enumerated Series representing each day for the x axis (1...number of days); add 1 to num so enumeration starts at 1
        print("xVals.mean(): ", xVals.mean())

        yVals = pd.Series(stateDF['deathIncrease'].values)  # transform yVals array to a Series so it can be added to lrDF dataframe (and so Series calculations can be done)
        print("yVals.mean(): ", yVals.mean())

        # create the linear reqression data frame (lrDF) and add the following columns
        lrDF = pd.DataFrame()
        lrDF['x'] = xVals
        lrDF['y'] = yVals
        lrDF['xDev'] = lrDF['x'] - xVals.mean()  # deviation of a point x from x mean value; x - x mean
        lrDF['yDev'] = lrDF['y'] - yVals.mean()  # deviation of a point y from y mean value; y - y mean
        lrDF['xDev2'] = lrDF['xDev'] * lrDF['xDev']  # deviation of x squared
        lrDF['xDevXyDev'] = lrDF['xDev'] * lrDF['yDev']  # deviation of x times deviation of y (xDev * yDev)

        slope = round(lrDF['xDevXyDev'].sum() / lrDF['xDev2'].sum(), 2)  # calculate the slope
        print("slope: ", slope)
        yIntercept = round(yVals.mean() - (slope * xVals.mean()), 2)  # calculate the  y intercept
        print("yIntercept: ", yIntercept)

        plt.plot(xVals, yVals, '.', label='Deaths Per Day')
        plt.legend(loc="upper left")
        plt.xlabel('Daily')
        plt.ylabel('Death Count')
        y_vals = yIntercept + slope * xVals
        plt.plot(xVals, y_vals, '--')
        plt.show()

    def getDFData(self, df, rows, cols):
        if not df:
            df = pd.read_json('DataLayer/Data/export/currentCovid.json')
        print(df.iloc[self.getRowIndices(df, rows), self.getColIndices(df, cols)])
        return df.iloc[self.getRowIndices(df, rows), self.getColIndices(df, cols)]

    def getColIndices(self, df, cols):
        dfCols = list(df.columns)
        colIndices = []

        if len(cols) == 0:  # no columns passed, get all column names
            cols = list(df.columns)

        for c in cols:  # find the valide numeric index of each column and add it to colIndices
            try:
                int(c)  # column passed as an integer, just add it to the list of indices (if integer in bounds)
                if 0 <= c <= len(dfCols):  # ensure column number is not out of bounds
                    colIndices.append(c)
            except:
                if c.find(":") > 0:
                    rng = c.split(":")
                    rngStart = int(rng[0])
                    if rngStart < 0:
                        rngStart = 0
                    rngEnd = int(rng[1]) + 1
                    if rngEnd > len(dfCols):
                        rngEnd = len(dfCols)
                    for col in range(rngStart, rngEnd):
                        colIndices.append(col)
                else:
                    for col in dfCols:  # find the index number for these column names (column names passed as strings)
                        if c == col:  # column names match, add this index to the list of indices
                            colIndices.append(df.columns.get_loc(col))
        return colIndices  # list of indexes

    def getRowIndices(self, df, rows):
        rowIndices = []

        if len(rows) == 0:  # no rows passed, return all row indices
            for row in df.iterrows():
                rowIndices.append(row[0])
        else:
            for row in rows:
                try:
                    int(row)
                    if 0 <= row <= len(df)-1:
                        rowIndices.append(row)
                except:
                    if row.find(":") > 0:
                        rng = row.split(":")
                        rngStart = int(rng[0])
                        if rngStart < 0:
                            rngStart = 0
                        rngEnd = int(rng[1])+1
                        if rngEnd > len(df):
                            rngEnd = len(df)
                        for r in range(rngStart, rngEnd):
                            rowIndices.append(r)
                    else:
                        rowIndices.append(int(row))
        return rowIndices

    def search(self):
        self.esClient.queries['atHocQuery'] = "set me up!"
        print(self.esClient.queries['atHocQuery'])