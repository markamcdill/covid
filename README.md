# covid
Simple python data science project to track covid stats

# Dependencies
  - ElasticSearch
  - Kibana
  
# Platform
  - Anaconda 3 (or latest); comes with Python 3.7 (or latest)
  - Pycharm (install latest version)
  
# Install and Run
ElasticSearch
  1. Download ElasticSearch: https://www.elastic.co/downloads/elasticsearch
  2. Save/Copy the compressed file to home (~) directory and uncompress
  3. Open a terminal and navigate to ~/elasticsearch-7.6.1/bin  NOTE: the latest version may be greater than 7.6.1; the folder name will reflect that version
  4. Start elasticsearch from terminal by typing: ./elasticsearch
  5. View elasticsearch from web browser: open a browser and go to: http://localhost:9200 (you should see a JSON output with values for 'name', 'cluster_name' etc.)

Kibana
  1. Download Kibana: https://www.elastic.co/downloads/kibana
  2. Save/Copy the compressed file to home (~) directory and uncompress
  3. Open a terminal and navigate to ~/kibana-7.6.1-darwin-x86_64/bin  NOTE: the latest version may be greater than 7.6.1; the folder name will reflect that version
  4. Start kibana from terminal by typing: ./kibana
  5. View kibana from web browser: open a browser and go to: http://localhost:5601 (you should see the kibana dashboard)

Anaconda
  1. Download Anaconda 3 (individual edition): https://www.anaconda.com/products/individual
  2. Find graphical installer on download page (it's at the bottom of the page)
  3. Follow graphical installer instructions; NOTE: sometimes older versions of Python (v2) conflict with Anaconda 3 install -- remove Python v2 from system)
  
# Code Base (the app)
  - Clone into ~/PycharmProjects folder (folder created when Anaconda installed)

# App Execution
  - open main.py
  - Comment out all lines (# == comment) then uncomment (remove #) the following three lines
    --  covid19 = covid.Covid('covid-19')
        covid19.curate()
        covid19.doData(action='insertLatest', doc_type='_doc')  # retrieve and insert latest covid data
        
    -- run main.py
  
  
