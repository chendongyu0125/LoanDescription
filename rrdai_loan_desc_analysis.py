# coding=utf-8
import pandas as pd
from sqlalchemy import create_engine
import re
import logging
logging.basicConfig(level=logging.DEBUG)

engine = create_engine('mysql+pymysql://root:root@localhost:3306/rrdai_2016?charset=utf8')
sql = 'select * from rrdai_project_main limit 100000'
df = pd.read_sql(sql, engine)
df['ProjectStatus'].value_counts()

df.columns #查看所有列的名字
list(df['LoanDesc'].head())

def written_by_borrowers(txt):
    if ('我' in txt) or ('本人' in txt):
        return True
    else:
        return False

def verification(txt):
    pattern = re.compile(u'.*上述信息已经.*考察认证.*')
    match = pattern.findall(txt)
    if len(match)>0:
        return True
    else:
        return False

verified_listings=[]
verified_listings_written_by_borrowers=[]
verified_listings_written_by_platform=[]
unverified_listings=[]
for index, row in df.iterrows():
    txt = row['LoanDesc']
    if verification(txt):
        verified_listings.append(row)
        if written_by_borrowers(txt):
            verified_listings_written_by_borrowers.append(row)
        else:
            verified_listings_written_by_platform.append(row)
    else:
        unverified_listings.append(row)

logging.debug(len(verified_listings))
logging.debug(len(unverified_listings))

print(len(verified_listings))
print(len(unverified_listings))
df_verified=pd.DataFrame(verified_listings)
df_verified.to_csv("verified_listings.csv")
print(df_verified['ProjectStatus'].value_counts())


df_unverified=pd.DataFrame(unverified_listings)
df_unverified.to_csv("unverified_listings.csv")
print(df_unverified['ProjectStatus'].value_counts())



df_verified_listings_written_by_borrowers = pd.DataFrame(verified_listings_written_by_borrowers)
df_verified_listings_written_by_borrowers.to_csv("verified_written_by_borrowers.csv")
print(df_verified_listings_written_by_borrowers['ProjectStatus'].value_counts())

df_unverified_listings_written_by_borrowers = pd.DataFrame(verified_listings_written_by_platform)
df_unverified_listings_written_by_borrowers.to_csv("verified_written_by_platform.csv")
print(df_unverified_listings_written_by_borrowers['ProjectStatus'].value_counts())



engine = create_engine('mysql+pymysql://root:root@localhost:3306/rrdai_2016?charset=utf8')
sql = 'select * from temp_default_project_main'
df = pd.read_sql(sql, engine)
df['ProjectStatus'].value_counts()

df.columns #查看所有列的名字
list(df['LoanDesc'].head())


verified_listings=[]
verified_listings_written_by_borrowers=[]
verified_listings_written_by_platform=[]
unverified_listings=[]
for index, row in df.iterrows():
    txt = row['LoanDesc']
    if verification(txt):
        verified_listings.append(row)
        if written_by_borrowers(txt):
            verified_listings_written_by_borrowers.append(row)
        else:
            verified_listings_written_by_platform.append(row)
    else:
        unverified_listings.append(row)

logging.debug(len(verified_listings))
logging.debug(len(unverified_listings))

print(len(verified_listings))
print(len(unverified_listings))
df_verified=pd.DataFrame(verified_listings)
df_verified.to_csv("default_verified_listings.csv")
print(df_verified['ProjectStatus'].value_counts())
df_verified['LoanTime2']=pd.to_datetime(df_verified['LoanTime'], format="%Y-%m-%d %H:%M:%S")
df_verified.LoanTime2.dt.year.value_counts()

df_unverified=pd.DataFrame(unverified_listings)
df_unverified.to_csv("default_unverified_listings.csv")
print(df_unverified['ProjectStatus'].value_counts())



df_verified_listings_written_by_borrowers = pd.DataFrame(verified_listings_written_by_borrowers)
df_verified_listings_written_by_borrowers.to_csv("default_verified_written_by_borrowers.csv")
print(df_verified_listings_written_by_borrowers['ProjectStatus'].value_counts())

df_unverified_listings_written_by_borrowers = pd.DataFrame(verified_listings_written_by_platform)
df_unverified_listings_written_by_borrowers.to_csv("default_verified_written_by_platform.csv")
print(df_unverified_listings_written_by_borrowers['ProjectStatus'].value_counts())