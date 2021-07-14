# -*- coding: utf-8 -*-
"""
Created on Wed Jul  7 14:44:26 2021

@author: jaredmw2

This file creates a census sheet given a list of arks or pids. The census sheet contains a link to the person, 
birthyear, deathyear, and US census arks
"""

# arks/pids MUST be in the first column
arg_files = "1910_arks.csv" # list: must be delimited with "!" (if there is more than one). Assumes pids are in the first column of the csv. 

# Set to "true" if the input files have arks, not pids. Set to "false" if the input files have pids, not arks. 
arg_need_pids = "true" # "true" or "false"

# location of the files to scrape and where the scraped information will be saved
directory = r"V:\FHSS-JoePriceResearch\RA_work_folders\Jared_Wright\make_census_sheets\test2"

years = [1900, 1910, 1920, 1930, 1940]


# Don't modify below this line
######################################################################################################################
import os
import pandas as pd
import numpy as np
import subprocess

arg_type = "stats" # "stats", "hops" or "unattached_arks". To create the census sheet we use "stats"

# Possible features include: name, byear_dyear, parents, kids, residences, baptism_date, duplicates, records, specific_records, record_hints, us_census_arks, us_census_hints, contributors, creation_info
arg_features = "byear_dyear!us_census_arks" # list: must be delimited with "!" (if there is more than one)

# This structures a single string argument to be sent to the python script
arg_list = [arg_type, arg_need_pids, arg_features, arg_files, directory]

# Scrape people from family search and wait for scraper to finish
executable = r'V:\FHSS-JoePriceResearch\RA_work_folders\Neil_Duzett\scraping\master_scrape\run_scrape_v2.bat'
p = subprocess.Popen([executable, "+".join(arg_list)], shell=True)
print('Started scraping pids')
p.wait()
print('Finished scraping pids')

# Read in scraped data
os.chdir(directory)
df = pd.read_csv('scraped_' + arg_files, header=0, engine='python')
df.columns = ['pid', 'birthdate', 'deathdate', 'censuslinks', 'censusdesc']

# Extract birth years and death years from strings
df['birthyear']=df['birthdate'].str.extract(r'(\d{4})')
df['deathyear']=df['deathdate'].str.extract(r'(\d{4})')
df.drop(columns=['birthdate', 'deathdate'], inplace=True)

# this function reshapes the data from wide to long. It matches each census link with its corresponding description
#source: https://stackoverflow.com/questions/12680754/split-explode-pandas-dataframe-string-entry-to-separate-rows/40449726#40449726
def explode(df, lst_cols, fill_value='', preserve_index=False):
    # make sure `lst_cols` is list-alike
    if (lst_cols is not None
        and len(lst_cols) > 0
        and not isinstance(lst_cols, (list, tuple, np.ndarray, pd.Series))):
        lst_cols = [lst_cols]
    # all columns except `lst_cols`
    idx_cols = df.columns.difference(lst_cols)
    # calculate lengths of lists
    lens = df[lst_cols[0]].str.len()
    # preserve original index values    
    idx = np.repeat(df.index.values, lens)
    # create "exploded" DF
    res = (pd.DataFrame({
                col:np.repeat(df[col].values, lens)
                for col in idx_cols},
                index=idx)
             .assign(**{col:np.concatenate(df.loc[lens>0, col].values)
                            for col in lst_cols}))
    # append those rows that have empty lists
    if (lens == 0).any():
        # at least one list in cells is empty
        res = (res.append(df.loc[lens==0, idx_cols], sort=False)
                  .fillna(fill_value))
    # revert the original index order
    res = res.sort_index()
    # reset index if requested
    if not preserve_index:        
        res = res.reset_index(drop=True)
    return res

# parse census links and descriptions
df['censuslinks'] = df['censuslinks'].str.split(';')
df['censusdesc'] = df['censusdesc'].str.split(';')

# drop rows where the number of census links does not equal the number of census descriptions
df['samelen'] = df['censuslinks'].str.len()==df['censusdesc'].str.len()
df = df.loc[df.samelen, :]
df.drop(columns=['samelen'], inplace=True)

# reshape data from wide to long
df = explode(df,['censuslinks', 'censusdesc'], preserve_index=False)

# extract census years and arks from census links and census descriptions
df['censusyears']=df['censusdesc'].str.extract(r'United States Census\,\s(\d{4})')
df['censusarks']=df['censuslinks'].str.extract(r'(\w{4}-\w{3})$')

# create ark_year variables that contain arks for specified census year.
for year in years:
    df.loc[df.birthyear==str(year), 'ark'+ str(year)] = df['censusarks']
    df['ark'+ str(year)] = ""
    df.loc[df.censusyears==str(year), 'ark'+ str(year)] = df['censusarks']

# At this point we have a row for each ark. We want a row for each person. So this section
# collapses the data down so that each row is a separate person and contains multiple arks
df.drop(columns=['censusdesc', 'censuslinks', 'censusarks', 'censusyears'], inplace=True)
df = df.fillna('')
#df = df.groupby(['pid', 'birthyear', 'deathyear']).agg(''.join)
#df = df.groupby(['pid', 'birthyear', 'deathyear']).agg(lambda x: ''.join(x.unique()))
df = df.groupby(['pid', 'birthyear', 'deathyear']).max()
df.reset_index(level=df.index.names, inplace=True)

# Fill census years before birth and after death with '---'
df["byr"] = pd.to_numeric(df["birthyear"])
df["dyr"] = pd.to_numeric(df["deathyear"])
for year in years:
    df.loc[df.byr > year, 'ark'+ str(year)] = "---"
    df.loc[df.dyr < year, 'ark'+ str(year)] = "---"
df.drop(columns=['byr', 'dyr'], inplace=True)

# create urls for each person and drop pid
df['person_url'] = 'https://www.familysearch.org/tree/person/details/' + df['pid']
df.drop(columns=['pid'], inplace=True)
df = df[['person_url', 'birthyear', 'deathyear', 'ark1900', 'ark1910', 'ark1920',
       'ark1930', 'ark1940']]
#is_NaN = df.isnull()
#row_has_NaN = is_NaN.any(axis=1)

# split dataset into people with missing info and people without missing info
mdf = df.eq('')
df['missing'] = mdf.any(axis=1)
grouped = df.groupby(df.missing)
df_has_missing = grouped.get_group(True)
df_complete = grouped.get_group(False)
df_has_missing.drop(columns='missing', inplace=True)
df_complete.drop(columns='missing', inplace=True)

# create 'complete?' and 'volunteer' columns for the missing dataset
df_has_missing['complete?'] = ""
df_has_missing['volunteer'] = ""

# save census sheets
os.chdir(directory)
df_has_missing.to_csv('census_sheet_missing.csv', index=False)
df_complete.to_csv('census_sheet_complete.csv', index=False)


