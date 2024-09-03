#!/usr/bin/env python
# coding: utf-8

# In[17]:


import pandas as pd
from sqlalchemy import create_engine
import psycopg2


# In[3]:


pd.__version__


# In[13]:


df = pd.read_csv('data/nyc_taxi.csv', nrows = 100000)


# In[10]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data_table'))


# In[18]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[19]:


engine.connect()


# In[20]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data_table', con=engine))


# In[21]:


df_iter = pd.read_csv('data/nyc_taxi.csv',iterator=True, chunksize=100000) 


# In[26]:


df = next(df_iter)


# In[28]:


df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)


# In[30]:


df.head()


# In[31]:


# we want to insert the column names first, 
df.head(n=0).to_sql(name='yellow_taxi_data_table', con=engine, if_exists='replace')


# In[33]:


df.to_sql(name='yellow_taxi_data_table', con=engine, if_exists='append')


# In[34]:


from time import time


# In[35]:


while True:
    t_start = time()
    
    df = next(df_iter)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.to_sql(name='yellow_taxi_data_table', con=engine, if_exists='append')

    t_end = time()
    print('Inserted another chunk..., took %.3f seconds.' % (t_end - t_start))


# In[ ]:




