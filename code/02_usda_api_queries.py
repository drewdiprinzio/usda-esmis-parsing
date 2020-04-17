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
    command = f'curl -X GET "https://usda.library.cornell.edu/api/v1/publication/search?q={search_query}" -H "Accept: applicatio/json" -H "Authorization: Bearer {my_token}" -w "%\u007bresponse_code\u007d;%\u007btime_total\u007d"> curl/dataFile_{identifier}.json 2> curl/informationFile_{identifier}.txt'

    os.system(command)

    # Read in search results
    with open(f'curl/dataFile_{identifier}.json', 'r') as f:
        data = f.read()

    # The curl request has some extra numbers at the end which probably relate to the 
    # specific request, (such as time taken to download).  These cause errors when using 
    # json.loads, so remove them with regex substitute.
    to_parse = re.sub(r"[0-9]{3};[0-9]{1,3}\.[0-9]{5,6}$", "", data)

    # Load results, create df, export as CSV
    results = json.loads(to_parse)
    
    results_df = pd.DataFrame(results)    
    
    results_df.to_csv(f'export/results_{identifier}.csv')


# Run function
search_query = 'Price' #String to query
identifier = 'price_search'

curl_publication_search(search_query,identifier)


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
        
    command = f'curl -X GET "https://usda.library.cornell.edu/api/v1/release/findByIdentifier/{identifier}?latest=false&start_date={start_date}&end_date={end_date}" -H "accept: application/json" -H "Authorization: Bearer {my_token}" -w "%\u007bresponse_code\u007d;%\u007btime_total\u007d"> curl/dataFile_{save_name}.json 2> curl/informationFile_{save_name}.txt'
    
    os.system(command)
    
    # Read in search results
    with open(f'curl/dataFile_{save_name}.json', 'r') as f:
        data = f.read()

    to_parse = re.sub(r"[0-9]{3};[0-9]{1,3}\.[0-9]{5,6}$", "", data)
     
    # Load results, create df, export as CSV
    results = json.loads(to_parse)
    
    results_peanutprices = pd.DataFrame(results) 
    
    results_peanutprices.to_csv(f'export/results_{save_name}.csv')



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
One common report seems to be the 'Onion & Potato Recap Report'
"""

results_df['onion_report'] = results_df['title'].str.contains('Onion (&|and) Potato')

results_df = results_df[results_df['onion_report']==True]

# There are 17 different shipping points listed.
# We will use Philadelphia (index 7)
results_df.reset_index(inplace=True)

identifier = results_df['identifier'][5]

# We need to clean the identifier:
identifier = identifier[2:10]

"""
Run function to pull all releases for this identifier
"""

#Input vars to curl request
start_date = '2006-01-01'
end_date   = '2020-04-15'
save_name  = 'onion_price' #for files to be saved

curl_report_by_identifier(start_date, end_date, identifier, save_name)

onion_prices_df = pd.read_csv('export/results_onion_price.csv')