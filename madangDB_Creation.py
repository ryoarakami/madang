#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import duckdb

conn = duckdb.connect(database='madang.db')
conn.sql("create table Customer as select * from 'Customer_madang.csv'")
conn.sql("create table Book as select * from 'Book_madang.csv'")
conn.sql("create table Orders as select * from 'Orders_madang.csv'")
conn.close()


# In[2]:


import duckdb
conn = duckdb.connect(database='madang.db')


# In[3]:


conn.sql("select * from Customer")


# In[ ]:


def query(sql, retunrType='df'):
       if retunrType == 'df':
              return conn.execute(sql).df()
       else:
              return conn.execute(sql).fetchall()

#query("select * from Book", "df")
query("select * from Book", "list")


# In[9]:


conn.execute("select * from Orders").fetchall()


# In[4]:


conn.sql("select * from Orders")


# In[6]:


query("select * from Customer c, Orders o where c.custid = o.custid")


# In[ ]:




