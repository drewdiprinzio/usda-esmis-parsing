# -*- coding: utf-8 -*-
"""
Last edited April 2020

@author: Drew DiPrinzio
"""

import urllib
import pandas as pd
from datetime import datetime
import re
import pickle
import os

"""
Create directories for saving analysis
"""

def create_dir(path): 
    try:
        # Create target Directory
        os.mkdir(path)
        print("Directory " , path ,  " Created ") 
    except FileExistsError:
        print("Directory " , path ,  " already exists")

create_dir('txt_files')
create_dir('export')


"""
Download txt files from the web and save in "txt_files" folder.
The links to these files were saved in 'export/files.csv' in 01_scrape_reports.py file.
"""

link_df = pd.read_csv('export/files.csv')

txt_files = link_df[link_df['file_type']=='txt']

def save_files(i):
    
    file_path = list(txt_files['file'])[i]
    date      = list(txt_files['date'])[i][0:10]
    name      = list(txt_files['file_name'])[i]
    
    file = urllib.request.urlopen(file_path)
    
    content = []
    
    for line in file:
    	decoded_line = line.decode("unicode_escape")
    	content.append(decoded_line)
        
    with open(f'txt_files/{i:04}_{date}_{name}.txt', 'wb') as fp:
        pickle.dump(content, fp)


# Run function

for i in range(0,len(txt_files)):
    save_files(i)
    if(i % 10 == 0):
        print(i)

"""
Open and parse txt files and append to a dataframe.
Each report contains two sub-tables:
    - Average weekly price by variety, dollars per pound
    - Weekly marketings by variety, in 1,000 pounds

The below functions do the following:
    - Reads in txt file and creates a list where each element is a line in the file
    - Finds beginning and end lines of both sub-tables, parses data and creates dataframe
    - Stack dataframe from each file and deduplicate based on date, since the same week
    can be in multiple reports
    - Return stacked_data_p1 and stacked_data_p2 which contains all data for panel 1 and panel 2
"""

def parse_file(i):
            
   # a_file = list(txt_files['Files'])[i]
    date   = list(txt_files['date'])[i][0:10] 
    name   = list(txt_files['file_name'])[i]
    
    #Returns indices in list where {string} is in the element
    def grepl(string,l):
        return [i for i, val in enumerate(l) if string in val]
     
    with open (f'txt_files/{i:04}_{date}_{name}.txt', 'rb') as fp:
        content = pickle.load(fp)
            
    """
    Find the lines which contain data tables
    """    
    
    # This line is always at the beginning of the table in the txt file
    start_table_index   = grepl('Peanut Prices and Marketings by Type',content)
    
    # More recent years have 'Statistical Methodology' immediately after the table
    # In the past this section was called 'Survey Procedures' so we use this to find 
    # the end of the table.
    end_table_index     = grepl('Statistical Methodology',content)
    end_table_index_alt = grepl('Survey Procedures',content)
    
    if(len(start_table_index)<1 and len(end_table_index)<1 and len(end_table_index_alt)<1):
        print(f'File {i:04} does not seem to have a table.')
        return(None,None)
      
    start_table_index = start_table_index[0]

    if (len(end_table_index)>0):
        end_table_index = end_table_index[0]
    else:
        end_table_index = end_table_index_alt[0]
    
    table_lines = content[start_table_index:end_table_index]
    
    """
    Parse Month and Day Lines to Create 6-element Date list
    """
    
    date_start = grepl('Item and type',table_lines)[0]
        
    md_line    = table_lines[date_start+1]
    year_line  = table_lines[date_start+2]
    
    def create_dates(months,years):
        
        month = re.compile('[A-Za-z]{3,10} [0-9]{1,2},')
        m_parsed = month.findall(months)
        
        year = re.compile('[0-9]{4}')
        y_parsed = year.findall(years)
    
        if(len(m_parsed)<5 or len(y_parsed)<5):
            print(f'File {i:04} might be missing dates.')
            return(None,None)
    
        dates = []
    
        for (item1, item2) in zip(m_parsed, y_parsed):
            dates.append(item1+' '+item2)
                        
        dates = list(map(lambda x: datetime.strptime(x, '%B %d, %Y'), dates))
    
        # print('The most recent date is {:%B %d, %Y}'.format(dates[len(dates)-1]))
    
        dates.insert(0,'Date')
    
        return(dates)
    
    dates = create_dates(md_line,year_line)
    
    """
    We now have a list of strings which only contains the table information as 'table_lines'.
    We also have a series called 'dates' which will be used as the date column in the df.
    
    We run parse_panel() on the first and second sub-tables, or "panels"
    """
    
    #Subset to two panels
    
    #Panel one lines
    beg_panel_one  = grepl('Runner',table_lines)[0]
    end_panel_one  = grepl('All .....',table_lines)[0]+1
    first_panel = table_lines[beg_panel_one:end_panel_one]
    
    #Panel two lines
    beg_panel_two = grepl('Runner',table_lines)[1]
    end_panel_two = grepl('All .....',table_lines)[1]+1
    second_panel = table_lines[beg_panel_two:end_panel_two]
    
    #Define parsing function
    def parse_panel(panel):
        
        parsed_panel = []
        
        def parse_line(l):
            parsed = [l[0:15].replace(".","").replace(":","").replace(" ",""),
                      l[15:25],
                      l[25:40],
                      l[40:55],
                      l[55:70],
                      l[70:85]]
            
            parsed = list(map(lambda x: str.strip(x).replace('Runners','Runner').replace('Valencias','Valencia').replace('Virginias','Virginia'), parsed))
            
            return(parsed)
        
        for line in panel:
            parsed_panel.append(parse_line(line))
        
        df = pd.DataFrame(parsed_panel, columns = ['type','date1','date2','date3','date4','date5'])
        
        # filters out row which is blank in type column
        df = df[(list(map(lambda x: len(x)!=0,df['type'])))]
        
        # reshapes table
        df = df.stack().unstack(0)
        df['name_date'] = dates
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
        df['file_num'] = i
    #    df.to_csv('export/{:%Y%m%d}_{}.csv'.format(max(df['date']),file_name))
        
        return(df)
        
    parsed_panel_one = parse_panel(first_panel)
    parsed_panel_two = parse_panel(second_panel)

    return(parsed_panel_one, parsed_panel_two)


#txt_files = list(link_df[link_df['Types']=='txt']['Files'])

"""
run_all() is a wrapper function to run parse_file() over a certian number
of txt files and append them together.
"""

def run_all(file_list,start,end):
    #for i in range(0,len(file_list)):
    for i in range(start,end):    
        if (i%50==0):
            print(f'Parsing file {i}')
            
        a, b = parse_file(i)

        if (a is not None):
            if(i==0):
                stacked_data_p1 = a
                stacked_data_p2 = b
            else:
                stacked_data_p1 = pd.concat([stacked_data_p1, a], axis=0)
                stacked_data_p2 = pd.concat([stacked_data_p2, b], axis=0)
                
    stacked_data_p1 = stacked_data_p1.sort_values('Date').replace(to_replace={'(D)': None, '(X)': None}).drop_duplicates('Date')
    stacked_data_p2 = stacked_data_p2.sort_values('Date').replace(to_replace={'(D)': None, '(X)': None}).drop_duplicates('Date')
    
    stacked_data_p1.reset_index(drop=True, inplace=True)
    stacked_data_p2.reset_index(drop=True, inplace=True)
    
    return(stacked_data_p1, stacked_data_p2)

"""
Run function
"""

stacked_data_p1, stacked_data_p2 = run_all(txt_files,0,500)


"""
Plot data with matplotlib
"""

#Change types of price columns to float
cols_to_change = ['Runner','Spanish','Valencia','Virginia','All']

for i in cols_to_change:
    stacked_data_p1[i] = stacked_data_p1[i].astype('float64') 

#Plot with matplotlib
from matplotlib import pyplot as plt
plt.style.use('seaborn')

plt.plot('Date', 'Runner', data=stacked_data_p1, color='midnightblue', linewidth=1.5)
plt.plot('Date', 'Spanish', data=stacked_data_p1, color='royalblue', linewidth=1.5)
plt.plot('Date', 'Virginia', data=stacked_data_p1, color='mediumslateblue', linewidth=1.5)
plt.plot('Date', 'Valencia', data=stacked_data_p1, color='c', linewidth=1.5)
plt.plot('Date', 'All', data=stacked_data_p1, color='grey', linewidth=1.5)
plt.ylabel('Dollars per Pound')
plt.suptitle('Prices of Peanut Varieties from Web-scraped USDA Reports\n2010-2020')
plt.xlabel('\n\nSource:  National Agricultural Statistics Service (NASS), Agricultural Statistics Board, \nUnited States Department of Agriculture (USDA), accessed at Cornell Mann Library website:\nhttps://usda.library.cornell.edu\n\nData from 500 files reported weekly, from June 2010 to April 2020.', position=(0., 1e6),horizontalalignment='left')
plt.legend()
plt.savefig('export/historical_peanut_prices.png', bbox_inches='tight')