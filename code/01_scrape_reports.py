# -*- coding: utf-8 -*-
"""
Last edited April 2020

@author: Drew DiPrinzio
"""

import urllib
import pandas as pd
from bs4 import BeautifulSoup
import os

#Function for creating directory
def create_dir(path): 
    try:
        # Create target Directory
        os.mkdir(path)
        print("Directory " , path ,  " Created ") 
    except FileExistsError:
        print("Directory " , path ,  " already exists")

"""
Get links to all txt, pdf, and zip files.  

This uses the Peanut Price report as an example. There are 71 pages on the USDA 
website for this price report, and each contains several weekly reports. 

First, we will pull the html for these 71 pages in an 'html' folder.
Next, we will identify the report date and web links to all reports and save to CSV.
"""

webpages = []
webpages.append('https://usda.library.cornell.edu/concern/publications/5t34sj58c?locale=en#release-items')
for i in range(2,72):
    webpages.append(f'https://usda.library.cornell.edu/concern/publications/5t34sj58c?locale=en&page={i}#release-items')


"""
Save webpage html as text files.

There seems to be a limit of around 100 url requests on the website,  so if 
len(webpages) is more than 100, might need to do in parts.

Use the first line of code to test if the query is working for this report 
before running it on all urls.
"""

create_dir('html')

#Test
urllib.request.urlretrieve(webpages[0], "html/doc_{}.txt".format(i))

#Run over all files except first
for i in range(1,len(webpages)):
    urllib.request.urlretrieve(webpages[i], "html/doc_{}.txt".format(i))

"""
Parse html to find links to all pdfs, txt, and zip files.
Save date published, link, file_name, file_type, and source page to csv.
"""


#Create export directory
create_dir('export')

# Note that price reports for different commodities do not have all three of these file types
date_list   = []
file_list   = []
type_list   = []
name_list   = []
source_list = []

for i in range(0,len(webpages)):
    
    #Open html with beautiful soup
    f = open("html/doc_{}.txt".format(i))
    soup = BeautifulSoup(f, 'html.parser')
    links = soup.find_all('a',{"class":"btn btn-info download_btn file_download"},href=True)

    for j in range(0,len(links)):
        date_list.append(links[j]['data-release-date'])
        file_list.append(links[j]['href'])
        type_list.append(links[j].find('div').contents[0].lower())
        name_list.append(links[j]['href'].rsplit('/', 1)[-1][:-4])
        source_list.append(webpages[i])

    d = {'date':date_list
         ,'file':file_list
         ,'file_type':type_list
         ,'file_name':name_list
         ,'source_page':source_list}
    
    link_df = pd.DataFrame(d)

# Do some cleaning before exporting

# Remove a few administrative files which definitely don't have prices
link_df = link_df[~link_df ['file'].str.contains('Report_Reschedule')]

# The most recent file is repeated in later webpages
# Keep the first of any unique file_name and file_type
link_df = link_df.drop_duplicates(subset=['file_name', 'file_type'], keep='first')

#Export
link_df.to_csv('export/files.csv')

