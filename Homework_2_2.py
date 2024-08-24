 #This code demonstrates data cleaning using unegui.mn sales ads data

#Importing packages which will be needed
import pandas as pd #Panel Data package
import os #Find folders path #os.getcwd() - " os.getcwd() - '/Users/erdak/Desktop/Python/Python_source'"

df = pd.read_csv('/Users/erdak/Desktop/Python/Python_source/2_Data_cleaning/unegui/input/data.csv') #Dataframe csv importing

#Data inspect
df.head() #First rows 
df.tail(5) # Last 5 rows
df.dtypes #Data type, String, Integer etc.
df.describe() #Summary statistics
df.info() #Data type
df.columns #Columns name


#Change working directory
os.chdir('/Users/erdak/Desktop/Python/Python_source/2_Data_cleaning/unegui')

#Bring another file that will be used in the project
import util as ut

#Rename Columns
df.rename(columns=ut.name_cols, inplace=True)
df.columns #Checking column name

#Missing values
df.isna() #ResultFalse which is 0, if true 1 Nud bolgon NA uu? gd utgatai bvl false NA bvl True Column
df.isna().sum() #Sum Column #axis=0

df[df['ad_text'].isna()] #ad_text column NaN check
df[df['ad_text'].isna()][['ad_text','title','ad_id','url']] #Checking other columns with 'ad_text'

df.isna().sum(axis=1) #checking by row
df[df.isna().sum(axis=1) > 0] 
df[df['progress_cons'].isna()]

#Drop duplicates
df.duplicated() #Checking duplicated rows
df.duplicated().sum() #Total duplicated rows

df[df.duplicated()]
df[df.duplicated()]['ad_id']
df[df['ad_id'] == 8270541]
df[df['ad_id'] == 8270541].values

df[df.duplicated()]['ad_id'].value_counts()

df[df['ad_id'] ==8241219]

df.drop_duplicates(keep='first') #Get first 1 From duplicates

#Updated data frame with no duplication
df = df.drop_duplicates(keep='first')
df.duplicated().sum() #Checking duplication again

df[df['ad_id'] == 8241219]
df.drop_duplicates(subset=['date','time','ad_id'], keep='first') #If Date, Time, ad_id are same, then keep 1st
df[df.duplicated(subset=['date','ad_id'])]['ad_id']
df[df['ad_id'] == 8199882]
df.duplicated().sum() #Checking duplication again

#Sorting Data
df.sort_values(by=['date_ind','date','time','ad_id'], inplace=True)
df.drop_duplicates(subset=['date','time','ad_id'], keep='first', inplace=True)
df.duplicated(subset=['date','time','ad_id']).sum()

#Date
df['date'] = pd.to_datetime(df['date'])
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df['day'] = df['date'].dt.day
df['weekday'] = df['date'].dt.day_name()


#Number of ads by date
df.groupby('date')['ad_id'].count()
df.groupby('month')['ad_id'].count()

#Time

df['hour'] = df['time'].apply(lambda x: int(x.split(':')[0]))
df.groupby('hour')['ad_id'].count()

#Number of ads by hour and weekday
df.pivot_table(index='hour', columns='weekday', values='ad_id', aggfunc='count', margins=True)

##Area
import re

# Short version of Below
#df['area'] = df['size'].apply(lambda x: re.findall('\d+[.\d]*', x)[0])
#df['area'].sort_values()
#df['area'] = df['size'].apply(lambda x: re.findall('\d+[.\d]*', x)[0]).astype(float)
#df['area'].sort_values()

# Function to extract the numeric value
def extract_number(x):
    numbers = re.findall(r'\d+\.?\d*', x)
    return float(numbers[0]) if numbers else None

# Apply function and create new column
df['area'] = df['size'].apply(extract_number)
df['area'].sort_values()

import numpy as np # Mathemtical, matrix package

a_int = [-np.inf,15,30,50,80,100,200,500,np.inf]
df['a_int'] = pd.cut(df['area'], bins=a_int, include_lowest=True)
df['a_int'].value_counts().sort_values()

#Fixing aree manully
df[df['area']<=15]
df.loc[df['ad_id'] == 8071019, 'area'] = 73.18
df[(df['area']>15) & (df['area']<500)]
df = df[(df['area']>15) & (df['area']<500)]

##PRICE
#df['price'] = df['price_total'].apply(lambda x: re.findall('\d+[.\d]*', x)[0]).astype(float)
def price_number(x):
    numbers = re.findall(r'\d+\.?\d*', x)
    return float(numbers[0]) if numbers else None

# Apply function and create new column
df['price'] = df['price_total'].apply(price_number)
df[['price_total','price']]

#Terbum masking
mask = (df['price_total'].str.contains('бум', na=False)) & (df['price'] < 5)
df.loc[mask, 'price'] = df.loc[mask, 'price'] * 1000
df['mask'] = mask
df[df['mask'] == True].sort_values(by = 'price')

#Price Interval
p_int = [-np.inf,12,50,100,200,300,500,1000,5000,np.inf]
df['p_int'] = pd.cut(df['price'], bins=p_int, include_lowest=True)
df[['price','p_int']]
df['p_int'].value_counts().sort_values()

df[df['p_int'] == pd.Interval(12.0, 50.0, closed='right')]
df[df['p_int'] == pd.Interval(-np.inf,12, closed='right')]

df = df[~(df['price'] < 30) | ~(df['price'] > 12)] # remove outliers, price < 50 and price > 12
# df = df[(df['price'] < 30) & (df['price'] > 12)] # Above reverse

#Price per m2
df['price_m2'] = df['price']

# no price m2 above 12, if larger than 12, then divide by area
mask = df['price'] > 12 
df.loc[mask, 'price_m2'] = df.loc[mask, 'price'] / df.loc[mask, 'area'] 
df = df[df['price_m2'] > 1]

df.sort_values(by='price_m2', ascending=False)
df = df[df['price_m2'] > 0.8]
df = df[df['price_m2'] < 15]

##Location
df_loc = pd.read_csv('/Users/erdak/Desktop/Python/Python_source/2_Data_cleaning/unegui/input/location.csv')
df = df.merge(df_loc, on='location', how='left')
df['neighborhood'].value_counts()
df.groupby('neighborhood')['price_m2'].agg(['mean','median','max','min','count']).sort_values(by='median', ascending=False)

#Homework_2_1: Compare the median price per square meter between apartments with and without garage

df['garage'].value_counts()
df.groupby('garage')['price_m2'].agg(['median']).sort_values(by='median', ascending=False)

#Homework_2_2: Calculate the median price by “floor_at”
df.groupby('floor_at')['price_m2'].agg(['median']).sort_values(by='floor_at', ascending=True)

#Homework_2_3: For the ads (identified by ad_id) that were renewed or updated, how long after the initial posting did this occur?

df.duplicated().sum()
df[df.duplicated()]['ad_id'].value_counts()

df.drop_duplicates(keep='first') #Get first 1 From duplicates


df[df['ad_id'] ==7981899]
df.drop_duplicates(subset=['date','time','ad_id'], keep='first') #If Date, Time, ad_id are same, then keep 1st
df[df.duplicated(subset=['date','ad_id'])]['ad_id']
df[df['ad_id'] == 7981899]
df.duplicated().sum() #Checking duplication again


df.drop_duplicates(keep='first') #Get first 1 From duplicates


#Updated data frame with no duplication
df = df.drop_duplicates(keep='first')
df.duplicated().sum() #Checking duplication again


df[df['ad_id'] == 7981899]
selected_columns = df[df['ad_id'] == 7981899][['date_ind','date', 'time', 'ad_id']]
print(selected_columns)
duplicated_rows = df[df.duplicated(subset=['ad_id', 'date'], keep=False)]
result = duplicated_rows[['ad_id','date','time']] 
print(result)




df.sort_values(by=['date_ind','date','time','ad_id'],)
df[['ad_id', 'date',]]
df['ad_id']
df.duplicated().sum() #Checking duplicated rows

df[df.duplicated()]
df[df.duplicated()]['ad_id']
df[df['ad_id'] == 8270541]
df[df['ad_id'] == 8270541].values

df[df.duplicated()]['ad_id'].value_counts()

df[df['ad_id'] ==8241219]

df.drop_duplicates(keep='first') #Get first 1 From duplicates
df.drop_duplicates(subset=['date','time','ad_id'], keep='first') #If Date, Time, ad_id are same, then keep 1st