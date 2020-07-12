# covid

Application Entry Point: main.py

 **Function**: client interface to local elasticsearch instance
 
   **Specific Functions:**
   
         On __init__
         
           Retrieve covid data via URL request and place in a global array as json documents
           
           Instantiate elasticsearch client with index configuration
           
         On call
         
           doData()
                primary method responds to action parameter
         
           setKwargs()
           
               called when doData() is called
               
               analyze keyword arguments and respond to user if incorrect
               
           setStartDate()
           
               called when action == insertLatest
               
               queries and retrieves the latest date found in covid data
               
                   only data after this date is inserted into elasticsearch instance
                   
           Retrieve latest data from URL and insert 'differential' into local index
           
           Delete an index
           
           Delete a specific document by id
           
           Delete a range of documents (from-to dates)
           
           Execute scroll queries
           
               configure return size (optional)
               
               only need to pass a query name (name looked up in queries dictionary on ES_Client
               
           Export data formatted as:
           
               Kibana.json (can be imported by Kibana import tool)
               
               Elasticsearch.json (standard elasticsearch format)
               
               CSV (with headers)
               

   **Input Params:**
           action (required)
           
            valid actions
            
               insertLatest: retrieve and insert the latest covid data
               
               deleteIndex:  delete the default index defined by the global index variable
               
               deleteDoc: delete a single document (doc_id required)
               
               deleteDocs: delete multiple documents defined by date range 'frm - to' (will delete all documents if no 'frm-to' date range provided)
               
               query: scroll based query, set return_size to modulate the number of records to returned
               
               export: CSV, KI happy json, or elasticsearch standard json
               
    Use Cases:
    
        covid19 = covid.Covid('covid-19')  # instantiate the covid class and configure the elasticsearch client for the covid-19 instance
    
        covid19.doData(action='insertLatest')  # retrieve and insert latest covid data
        
        covid19.doData(action='deleteIndex')  # delete the default index (covid-19)
        
        covid19.doData(action='deleteDocs', frm='20200320',to='20200407')  # delete this range of docs; if no frm/to delete all docs
        
        covid19.doData(action='deleteDoc', doc_id='5cc91902e24fad7f218a89c4d57c03ceaf0546ed')  # delete a single document; doc_id required
        
        results = covid19.doData(action='query', q='getMaxDate', return_size=1)  # all queries are in the queries dictionary found on ES_Client, call them by name; return_size is optional, defaults to all records
        
        covid19.doData(action='export', target='CSV', fqp='myCSV')  # target types: CSV, KI, ES; fqp can be any fully qualified path (dir/filename), if blank, goes to default directory and file name

