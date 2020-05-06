# -*- coding: utf-8 -*-
"""
Last edited April 2020

@author: Drew DiPrinzio
"""

"""
Import libraries
"""

import os
import json
import pandas as pd
import re
import urllib
# from datetime import datetime
import pickle

"""
Before beginning, we will want to prepare a few things:
1. Create 'curl' and 'export' folder path in directory to save results from curl GET requests.
2. You will need to set up a token on the USDA website:
   https://usda.library.cornell.edu/apidoc/index.html
   Save this as `my_token` in your main directory.
"""

def create_dir(path): 
    try:
        # Create target Directory
        os.mkdir(path)
        print("Directory " , path ,  " Created ") 
    except FileExistsError:
        print("Directory " , path ,  " already exists")

create_dir('curl')
create_dir('export')

with open('my_token.json', 'r') as f:
    data = f.read()

my_token = json.loads(data)['jwt']


"""
Create first API query function, publication search:

1. Pull all publications matching search "Price" using the publication search query
2. We have to pass Authorization Bearer: {my_token}
3. We will set an {identifier} for our request. This saves the curl response to two files: 
    dataFile_{identifier}.json        -- contains results from request in json format
    informationFile_{identifier}.txt -- contains short review of request status

This syntax is for windows. May be different on other OS.
Note that \u007b \u007d are being used in this to avoid errors with "{" and "}" 
characters in f string.
"""

def curl_publication_search(search_query,identifier):
    
    # This query takes the form:
    # "https://usda.library.cornell.edu/api/v1/publication/search?q={search_query}"
    
    # Curl request to 
    command = f'curl -X GET "https://usda.library.cornell.edu/api/v1/publication/search?q={search_query}" -H "Accept: applicatio/json" -H "Authorization: Bearer {my_token}" > curl/dataFile_{identifier}.json 2> curl/informationFile_{identifier}.txt'

    os.system(command)

    # Read in search results
    with open(f'curl/dataFile_{identifier}.json', 'r') as f:
        data = f.read()

    # Load results, create df, export as CSV
    results = json.loads(data)
    
    results_df = pd.DataFrame(results)    
    
    results_df.to_csv(f'export/results_{identifier}.csv')


# Run function
search_query = 'Price' #String to query
identifier = 'price_search'

curl_publication_search(search_query,identifier)

view = pd.read_csv('export/results_price_search.csv')

"""
Create second function: Request all releases by report identifier

Given a report identifier which can be found in the publication search,
pull the links to all releases over time. Depending on the publication,
the releases might be daily, weekly, etc. and also may come as txt, pdf, or
zip files.
"""

def curl_report_by_identifier(start_date,end_date,identifier,save_name):

    # This query takes the form:
    # https://usda.library.cornell.edu/api/v1/release/findByIdentifier/{identifier}?latest=false&start_date={start_date}&end_date={end_date}
        
    command = f'curl -X GET "https://usda.library.cornell.edu/api/v1/release/findByIdentifier/{identifier}?latest=false&start_date={start_date}&end_date={end_date}" -H "accept: application/json" -H "Authorization: Bearer {my_token}" > curl/dataFile_{save_name}.json 2> curl/informationFile_{save_name}.txt'
    
    os.system(command)
    
    # Read in search results
    with open(f'curl/dataFile_{save_name}.json', 'r') as f:
        data = f.read()

    # Load results, create df, export as CSV
    results = json.loads(data)
    
    results_df = pd.DataFrame(results) 
    
    results_df.to_csv(f'export/results_{save_name}.csv')



"""
Run the releases request function for the peanut price report:
    
We can query for the same peanut price files that we pulled in the 01_scrape_reports.py file.
We will use the 'identifier' column from the results_price_search.csv, and use the findByIdentifier api query

"""

#Input vars to curl request
start_date = '2006-01-01'
end_date   = '2020-04-15'
identifier = 'PeanPrice' #found in results_price_search.csv
save_name  = 'peanut_price' #for files to be saved

curl_report_by_identifier(start_date, end_date, identifier, save_name)

peanut_results = pd.read_csv(f'export/results_peanut_price.csv')
# We'll have to reformat a bit, but this is basically the same table we made
# from downloading directly from the website.



"""
Another option for using this function is to filter down the results_price_search.csv,
based on a few parameters. 

Let's filter to:
status    = 'Active'
frequency = 'Daily'
resource_type = 'Report'

We will then find an identifier for our use.
"""

results_df = pd.read_csv('export/results_price_search.csv')
 
results_df['is_active'] = results_df['status'].str.contains('Active')
results_df['is_daily'] = results_df['frequency'].str.contains('Daily')
results_df['is_report'] = results_df['resource_type'].str.contains('Report')

results_df = results_df[results_df['is_active']==True]
results_df = results_df[results_df['is_daily']==True]
results_df = results_df[results_df['is_report']==True]

#View results_df
results_df

"""
From viewing the results_df, one common report seems to be the 'Onion & Potato Report'
"""

results_df['onion_report'] = results_df['title'].str.contains('Onion (&|and) Potato')

results_df = results_df[results_df['onion_report']==True]

results_df.identifier = results_df.identifier.str[2:10]

results_df.reset_index(inplace=True)

"""
There are 17 different geographic locations listed, and two slightly different reports,
the shipping point and the wholesale market reports.

First we will run function to pull all releases for a single identifier.
We will use Philadelphia (index 5).
"""

identifier = results_df['identifier'][5]

#Input vars to curl request
start_date = '2006-01-01'
end_date   = '2020-04-15'
save_name  = 'onion_price_phl' #for files to be saved

curl_report_by_identifier(start_date, end_date, identifier, save_name)

"""
Running identifier on additional files
"""
start_date = '2006-01-01'
end_date   = '2018-12-15'
save_name  = 'prod2' #for files to be saved
identifier = 'AJ_PY046'
curl_report_by_identifier(start_date, end_date, identifier, save_name)


"""
Bulk download Philadelphia report txt files
"""

phl_reports = pd.read_csv('export/results_onion_price_phl.csv')

create_dir('export/onion_price_phl_files')

# View the first txt file
def clean_filename(file):
    file = file[2:len(file)-2]
    return file
    
phl_reports['clean_files'] = pd.Series(map(clean_filename,phl_reports['files']))

def save_files(export_folder,dataframe,files_col,date_col,name_col):
    
    create_dir('export')
    create_dir(f'export/{export_folder}')
    
    for i in range(0,len(dataframe)):
    
        if (i%50==0):
            print(f'file {i} of {len(dataframe)}')
        
        file_path = list(dataframe[files_col])[i]
        date      = list(dataframe[date_col])[i][0:10]
        name      = list(dataframe[name_col])[i]
        
        file = urllib.request.urlopen(file_path)
        
        content = []
        
        for line in file:
        	decoded_line = line.decode("unicode_escape")
        	content.append(decoded_line)
            
        with open(f'export/{export_folder}/{i:04}_{date}_{name}.txt', 'wb') as fp:
            pickle.dump(content, fp)
        
save_files('onion_price_phl_files',phl_reports,'clean_files','release_datetime','id')