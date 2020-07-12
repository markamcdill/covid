from BusinessLogic import covid


def main():
    covid19 = covid.Covid('covid-19')

    # covid19.doLR('NY')

    covid19.getDFData('', ['0:50'], [])

    # covid19.doData(action='deleteIndex')  # delete the default index (covid-19)
    # covid19.curate()
    # covid19.doData(action='insertLatest', doc_type='_doc')  # retrieve and insert latest covid data
    # covid19.doData(action='deleteDocs', frm='20200320',to='20200407')  # delete this range of docs; if no frm/to delete all docs
    # covid19.doData(action='deleteDoc', doc_id='5cc91902e24fad7f218a89c4d57c03ceaf0546ed')  # delete a single document; doc_id required
    # results = covid19.doData(action='query', q='getMaxDate', return_size=1)  # all queries are in the queries dictionary found on ES_Client, call them by name; return_size is optional, defaults to all records
    # covid19.doData(action='export', target='CSV', fqp='currentCovid.csv', frm='20200101')  # target types: CSV, KI, ES; fqp can be any fully qualified path (dir/filename), if blank, goes to default directory and file name; frm can be any 8 digit date (e.g. 20200102)
    # covid19.search()


if __name__ == '__main__':
    main()
