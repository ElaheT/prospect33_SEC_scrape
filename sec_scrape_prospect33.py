# -*- coding: utf-8 -*-
# Author - Harish Ramani
# Email - harishramani1792@gmail.com
'''
Importing libraries
'''
from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
import time
from tqdm import  tqdm
import argparse
import logging

'''
Constants
'''
DAILY_FEED_URL = "https://www.sec.gov/Archives/edgar/daily-index/"
logging.basicConfig(filename='sec_scrape.log', level=logging.DEBUG)
'''
Helper functions
'''
def decide_quarter(date):
  '''
  Given a date, decide the quarter.
  :param date - timestamp
  :returns quarter - string
  '''

  if date.month >= 1 and date.month <= 3:
    return "QTR1"
  elif date.month >= 4 and date.month <= 6:
    return "QTR2"
  elif date.month >=7 and date.month <= 9:
    return "QTR3"
  return "QTR4"

def scrape_filing_page(url):
  '''
  Given a company filing page, scrape the relevant information.
  :param url - company filing url to scrape
  :returns dict of company information
  '''
  filing_page = BeautifulSoup(requests.get(url).content, 'html.parser')
  not_available = filing_page.find('h1', {'class': "goodbye text-center"})
  if (not_available is not None) and (not_available.text == 'This page is temporarily unavailable.'):
    return {"url": url, "address": [], "companyName": None, "cik_num": None, "irs_num": None}
  result = {}
  result["url"] = url
  # Parse Info head and Info.
  top_info = filing_page.find('div', {'class': "formGrouping"})
  info_head = [col.text for col in top_info.find_all('div', {'class': "infoHead"})]
  info = [col.text for col in top_info.find_all('div', {'class': 'info'})]
  result.update(dict(zip(info_head, info)))

  # Parse Documents and store them separately.
  '''
  The documents are intentionally not stored. However each filing attachments can be downloaded and kept on the server.
  '''
  # Parse Filer Divs

  filer = filing_page.find('div', {'id': "filerDiv"})
  result["address"] = [add.text for add in filer.find_all('div', {'class': "mailer"})] if filer is not None else []
  companyInfo = filer.find('div', {'class': 'companyInfo'}) if filer is not None else None
  c_name, cik = companyInfo.find('span', {'class': 'companyName'}).text.split('\n') if companyInfo is not None else (None, None)
  result["companyName"] = c_name.split('(')[0] if c_name is not None else None
  result["cik_num"] = cik.split(":")[1].split("(")[0].strip() if cik is not None else None
  
  identInfo = filer.find('p', {'class': "identInfo"}).text.split("|") if filer is not None else None
  result["irs_num"] = identInfo[0].split(":")[1].strip() if identInfo is not None else None
  '''
  result["state_of_incorp"] = identInfo[1].split(":")[1].strip()
  print(identInfo[2])
  sic_splits = identInfo[2].split("SIC:")
  fiscal_info = sic_splits[0]
  result["sic_code"] = sic_splits[1]
  result["form_type"] = fiscal_info.split("Type:")[1].strip()
  result["fiscal_year_end"] = fiscal_info.split("Type:")[0].split("Fiscal Year End:")[1].strip()
  '''
  return result

def scrape_and_save(urls):
  '''
  Parse all the urls and save the result onto a csv.
  :param urls - list of urls to scrape.
  :returns None - albeit saves a csv file in the current directory. This should be changed to saving in S3/GCP.
  '''
  all_result = []
  for url in tqdm(urls):
    logging.info(f'---------------Scraping {url}  ------------------------')
    all_result.append(scrape_filing_page(url))
    '''
    I initially processed the requests parallely, so that i scrape fast. But, SEC blocked my IP and hence
    had to implement a sleep to oblige with SEC website rate limit demand of 10 requests per second
    '''
    time.sleep(1)
  pd.DataFrame(all_result).to_csv('result.csv', index=False)

if __name__ == '__main__':
  try:
    parser = argparse.ArgumentParser(description="parse args")
    parser.add_argument('-parse_date', '--parse-date', default="20201130", type=str, help='Companies that were filed on this date, their info will be parsed')
    args = parser.parse_args()
    daily_page = BeautifulSoup(requests.get(DAILY_FEED_URL).content, 'html.parser')
    current_year_link = daily_page.find('a', {"href": str(datetime.now().year)+"/"}).get("href")
    current_url = DAILY_FEED_URL + current_year_link
    current_year_page = BeautifulSoup(requests.get(current_url).content, 'html.parser')
    search_quarter = decide_quarter(datetime.now())
    quarter_url = current_year_page.find('a', {"href": search_quarter + "/"}).get("href")
    current_quarter_url = current_url + quarter_url
    quarter_page = BeautifulSoup(requests.get(current_quarter_url).content, 'html.parser')
    date_to_search = args.parse_date
    today_index_to_scrape = [url for url in quarter_page.find_all('a') if date_to_search in  url["href"] and "sitemap" in url["href"]]
    today_index_to_scrape = today_index_to_scrape[0].get("href") if len(today_index_to_scrape) > 0 else 'NOTFOUND'
    if today_index_to_scrape == 'NOTFOUND':
      logging.info("----------------------------------------------------")
      logging.error("There are no companies filed on the SEC platform on this date.")
      logging.info("----------------------------------------------------")
    else:
      sitemap_url = current_quarter_url + today_index_to_scrape
      sitemap_page = BeautifulSoup(requests.get(sitemap_url).content, 'xml')
      current_filings = [url.text for url in sitemap_page.find_all('loc')]
      scrape_and_save(current_filings)
      logging.info("----------------------------------------------------")
      logging.info('The file is saved in the current directory')
      logging.info('----------------------------------------------------')
  except Exception as e:
    logging.info('----------------------------------------------------')
    logging.error(f'Error occured. Check logs {e}')
    logging.info('----------------------------------------------------')
  

