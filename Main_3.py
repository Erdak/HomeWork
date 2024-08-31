
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import re
import datetime

import os
os.chdir("Homework")

import util as ut
from joblib import Parallel, delayed # pip install joblib


## Collect the last page number
page_last = ut.find_last_page()
page_last = page_last - 1
# print(f'Last page: {page_last}')

## Collect data from all pages in parallel
page_list = range(5,page_last + 1)
results = Parallel(n_jobs=-1)(delayed(ut.data_collection)(n) for n in page_list)

all_data = []
for i in range(len(results)):
    for j in range(len(results[i])):
        all_data.append(results[i][j])
        
df = pd.DataFrame(all_data)

today = datetime.datetime.today().strftime("%Y-%d-%m")
df.to_csv(f'results/results_{today}.csv', encoding='utf-8-sig')


## Send email
ut.send_email()


## TO BE DONE: collect all ads in one csv file
##3 collect ads posted today and append to a csv file (parallelize the process):
##4 send email to the user with the csv file attached:
##5 create task scheduler to run the script every day at 18:00: