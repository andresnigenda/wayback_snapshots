'''
This module takes a csv file with links you would like to save into wayback
and/or retrieve the last snapshot before a given date
'''

from bs4 import BeautifulSoup
from datetime import datetime
import xml.etree.ElementTree as ET
import internetarchive as ia
import pandas as pd
import savepagenow
import requests
import time
import csv
import sys

def update_csv(csv_name, dates, snapshot=False, csv_output='output.csv'):
    '''
    Updates a csv with the date of latest snapshot before a given date, it's wayback url,
    today's date and today's wayback snapshot of the url.

    Inputs:
        - csv_name: (str) name of the csv file from which to pull the urls. The target
                    column must be named as url or URL
        - dates: (list) latest date until which snapshots should be requested from wayback
                 machine
        - snapshot: (bool) if set to True, a snapshot of the url is taken

    Returns: a csv in the directory
    '''
    url_lst = get_links(csv_name) # get urls

    latest_urls_lst = []
    latest_dates_lst = []
    current_urls_lst = []
    current_dates_lst = []
    today = datetime.today().strftime('%m-%d-%Y')

    for url in url_lst:
        latest_date, latest_url = get_latest_wayback(url, dates)
        latest_urls_lst.append(latest_url)
        if latest_date:
            latest_date = latest_date.strftime('%m-%d-%Y')
        latest_dates_lst.append(latest_date)
        if snapshot:
            current_dates_lst.append(today)
            try:
                current_url = savepagenow.capture_or_cache(url)
                current_urls_lst.append(current_url[0])
            except Exception as e:
                current_urls_lst.append(e)

    all_lsts, all_cols = zip_lists([latest_urls_lst, latest_dates_lst,
                                   current_urls_lst, current_dates_lst])
    df = pd.DataFrame(all_lsts, columns=all_cols)
    df.to_csv(csv_output, index=False)

    #return df

def zip_lists(columns):
    '''
    Zips up columns and returns a list of the columns
    '''
    if len(columns[2]):
        all_lsts = list(zip(columns[0], columns[1], columns[2], columns[3]))
        all_cols = ['latest snapshot url', 'latest snapshot date', 'current snapshot date', 'current snapshot url']
    else:
        all_lsts = list(zip(columns[0], columns[1]))
        all_cols = ['latest snapshot url', 'latest snapshot date']

    return all_lsts, all_cols

def get_links(csv_name):
    '''
    Retrieves links from a csv, where the column storing the urls must be named as
    URL or url
    '''
    url_lst = []
    with open(csv_name) as f:
        next(f) # change this to not hard code
        header_lst = next(f).split(',')
        reader = csv.reader(f, delimiter=',')
        for i, col in enumerate(header_lst):
            if col.lower() == 'url':
                url_idx = i
                break

        url_lst = [str(row[url_idx]) for row in reader]

    return url_lst

def get_latest_wayback(url, dates):
    '''
    Gets the latest available snapshot captured by Wayback before the specified date

    Inputs:
        - url: (str) a url
        - dates: (lst) list of integers representing a date following the format
                 [year, month, day, hour, minute, second]

    Returns: a tuple of a datetime object and a url
    '''
    try:
        with ia.WaybackClient() as client:
            latest_date = datetime(dates[0], dates[1],
                                   dates[2], dates[3],
                                   dates[4], dates[5])
            versions = list(client.list_versions(url, to_date=latest_date))

            latest_version = versions[::-1][0]

            if latest_version:
                return latest_version.date, latest_version.view_url

    except Exception as e:
        return (None, e)

if __name__ == '__main__':

    csv_name = sys.argv[1]
    dates = sys.argv[2].replace('[', ' ').replace(']', ' ').replace(',', ' ').split()
    dates = [int(i) for i in dates]
    snap = sys.argv[3]
    csv_out = sys.argv[4]

    update_csv(csv_name, dates, snapshot=snap, csv_output=csv_out)
