#!/usr/bin/env python
# coding: utf-8

# In[2]:


get_ipython().system('pip install the cassandra-driver')


# In[66]:


from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import json

def connect_to_astra():
    # Path to the secure connect bundle you downloaded
    SECURE_CONNECT_BUNDLE = "D:\\Final Sem\\Big Data\\assignment\\secure-connect-assignment.zip"
    
    try:
        # Authentication provider using the secure connect bundle
        print("Initializing authentication provider...")
        auth_provider = PlainTextAuthProvider(
            username="qbjTqQlMDUZCfCNZoWbGaiqe",
            password="_TAyNjlb0R2te_8WiCWMXmbE8c8xtupJL-Iv9U-DZNNHslNah1K9FZHT-5xZqJzsAdYZ156KheSM0KxILs5F9wG42E9u9WZUJe..eeuhzkdRI4x5ktozkUPuwp8Gdr+l"  # Replace with your secret from the credentials
        )
        
        print("Connecting to AstraDB cluster...")
        cluster = Cluster(cloud={'secure_connect_bundle': SECURE_CONNECT_BUNDLE}, auth_provider=auth_provider)
        session = cluster.connect()

        if session:
            print('Connection to AstraDB was successful.')
        else:
            print("Failed to connect to AstraDB.")

        # Set the default keyspace (if needed)
        print("Setting keyspace...")
        session.set_keyspace('bigdata') 
        print("Keyspace set successfully.")
        
        return session

    except Exception as e:
        print(f"An error occurred: {e}")

# Call the function
session = connect_to_astra()
if session:
    print("Session is active and ready to use.")
else:
    print("No session returned.")



# In[67]:


session.set_keyspace("bigdata")


# # Inserting Data-Bronze Level

# In[68]:


# Read the CSV file into a DataFrame
df = pd.read_csv("D:\\Final Sem\\Big Data\\assignment\\sales_100 (1).csv")
df.rename(columns={
    'Region': 'region',
    'Country': 'country',
    'Item Type': 'item_type',
    'Sales Channel': 'sales_channel',
    'Order Priority': 'order_priority',
    'Order Date': 'order_date',
    'Order ID': 'order_id',
    'Ship Date': 'ship_date',
    'UnitsSold': 'units_sold',
    'UnitPrice': 'unit_price',
    'UnitCost': 'unit_cost',
    'TotalRevenue': 'total_revenue',
    'TotalCost': 'total_cost',
    'TotalProfit': 'total_profit'
}, inplace=True)




# Check the first few rows to ensure the data is loaded correctly
print(df.head())


# In[71]:


from datetime import datetime
import pandas as pd

# Convert dates to the correct format if they are not already
df['order_date'] = pd.to_datetime(df['order_date']).dt.date
df['ship_date'] = pd.to_datetime(df['ship_date']).dt.date

# Insert rows into Cassandra
for _, row in df.iterrows():
    session.execute("""
    INSERT INTO sales_data (
        region, country, item_type, sales_channel, order_priority, order_date, 
        order_id, ship_date, units_sold, unit_price, unit_cost, total_revenue, 
        total_cost, total_profit
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['region'], row['country'], row['item_type'], row['sales_channel'], 
        row['order_priority'], row['order_date'], row['order_id'], row['ship_date'], 
        row['units_sold'], row['unit_price'], row['unit_cost'], row['total_revenue'], 
        row['total_cost'], row['total_profit']
    ))


# In[72]:


rows = session.execute("SELECT * FROM sales_data LIMIT 5")
for row in rows:
    print(row)


# # Silver Level

# In[76]:


df.head(3)


# In[79]:


# Calculate profit margin
df['ProfitMargin_in_Percent'] = df['profit_margin_in_percent'] = ((df['total_profit'] / df['total_revenue']) * 100).round(2)
df.head(3)


# In[80]:


# Drop the existing table if already created
session.execute("DROP TABLE IF EXISTS silver_sales_data")
session.execute("""
CREATE TABLE IF NOT EXISTS silver_sales_data (
    region TEXT,
    country TEXT,
    item_type TEXT,
    sales_channel TEXT,
    order_priority TEXT,
    order_date DATE,
    order_id BIGINT PRIMARY KEY,
    ship_date DATE,
    units_sold INT,
    unit_price FLOAT,
    unit_cost FLOAT,
    total_revenue FLOAT,
    total_cost FLOAT,
    total_profit FLOAT,
    ProfitMargin_in_Percent FLOAT
)
""")


# In[83]:


# Insert into the Silver table
for _, row in df.iterrows():
    session.execute("""
    INSERT INTO silver_sales_data (
        region, country, item_type, sales_channel, order_priority, order_date, 
        order_id, ship_date, units_sold, unit_price, unit_cost, total_revenue, 
        total_cost, total_profit, ProfitMargin_in_Percent
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['region'], row['country'], row['item_type'], row['sales_channel'], 
        row['order_priority'], row['order_date'], row['order_id'], row['ship_date'], 
        row['units_sold'], row['unit_price'], row['unit_cost'], row['total_revenue'], 
        row['total_cost'], row['total_profit'], row['ProfitMargin_in_Percent']
    ))


# In[84]:


rows = session.execute("SELECT * FROM silver_sales_data LIMIT 5")
for row in rows:
    print(row)       


# # Gold Level Table

# In[85]:


# Create Gold Table 1
session.execute("""
CREATE TABLE IF NOT EXISTS gold_total_profit_by_region (
    region TEXT,
    country TEXT,
    total_profit FLOAT,
    PRIMARY KEY (region, country)
)
""")

gold1_df = df.groupby(['region', 'country'])['total_profit'].sum().reset_index()
for _, row in gold1_df.iterrows():
    session.execute("""
    INSERT INTO gold_total_profit_by_region (region, country, total_profit)
    VALUES (%s, %s, %s)
    """, (row['region'], row['country'], row['total_profit']))


# In[86]:


rows = session.execute("SELECT * FROM gold_total_profit_by_region LIMIT 5")
for row in rows:
    print(row)


# In[88]:


# Create Gold Table 2
session.execute("""
CREATE TABLE IF NOT EXISTS gold_total_revenue_by_region (
    region TEXT,
    total_revenue FLOAT,
    PRIMARY KEY (region)
)
""")

gold2_df = df.groupby(['region'])['total_revenue'].sum().reset_index()
for _, row in gold2_df.iterrows():
    session.execute("""
    INSERT INTO gold_total_revenue_by_region (region, total_revenue)
    VALUES (%s, %s)
    """, (row['region'], row['total_revenue']))
rows = session.execute("SELECT * FROM gold_total_revenue_by_region LIMIT 5")
for row in rows:
    print(row)


# In[94]:


# Create Gold Table 3
session.execute("""
CREATE TABLE IF NOT EXISTS gold_avg_profit_margin_by_channel (
    sales_channel TEXT,
    avg_profit_margin FLOAT,
    PRIMARY KEY (sales_channel)
)
""")

# ProfitMargin 
df['profit_margin'] = (df['total_profit'] / df['total_revenue'])

gold3_df = df.groupby(['sales_channel'])['profit_margin'].mean().reset_index()
for _, row in gold3_df.iterrows():
    session.execute("""
    INSERT INTO gold_avg_profit_margin_by_channel (sales_channel, avg_profit_margin)
    VALUES (%s, %s)
    """, (row['sales_channel'], row['profit_margin']))
    
rows = session.execute("SELECT * FROM gold_avg_profit_margin_by_channel")
for row in rows:
    print(row)


# In[ ]:




