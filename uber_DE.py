#%%
# Review the data
import pandas as pd

UBER_RAW_LINK = "https://raw.githubusercontent.com/darshilparmar/uber-etl-pipeline-data-engineering-project/main/data/uber_data.csv"

df = pd.read_csv(UBER_RAW_LINK)

# Define datetime format 
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

#%%
# Date dimension (wrong on drop_duplicate, the id is wrong that)
date_dim = df[['tpep_dropoff_datetime', 'tpep_pickup_datetime']]
date_dim = date_dim.drop_duplicates().reset_index(drop=True)

date_dim['pick_hour'] = date_dim['tpep_pickup_datetime'].dt.hour
date_dim['pick_day'] = date_dim['tpep_pickup_datetime'].dt.day
date_dim['pick_month'] = date_dim['tpep_pickup_datetime'].dt.month
date_dim['pick_year'] = date_dim['tpep_pickup_datetime'].dt.year
date_dim['pick_weekday'] = date_dim['tpep_pickup_datetime'].dt.weekday

date_dim['drop_hour'] = date_dim['tpep_dropoff_datetime'].dt.hour
date_dim['drop_day'] = date_dim['tpep_dropoff_datetime'].dt.day
date_dim['drop_month'] = date_dim['tpep_dropoff_datetime'].dt.month
date_dim['drop_year'] = date_dim['tpep_dropoff_datetime'].dt.year
date_dim['drop_weekday'] = date_dim['tpep_dropoff_datetime'].dt.weekday

date_dim = date_dim.reset_index(drop=False, names='datetime_id')

# Passenger part
passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=False, names='passenger_count_id')

# Trip distance part
trip_dis_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=False, names='trip_distance_id')

#%%
# Rate code 
rate_code_type = {
    1:"Stardard",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Nego",
    6:"Group ride",

}

rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=False, names='rate_code_id')
rate_code_dim['rate_code_name']  = rate_code_dim['RatecodeID'].map(rate_code_type)
#%%
# Pick up and drop off dimenstion
pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index(drop=False, names='pickup_location_id')
dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop=False, names='dropoff_location_id')

# Payment dimension
payment_type_name ={
    1:"Credit card",
    2:"Cash",
    3:"No Charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip",
}

payment_id_dim = df[['payment_type']].drop_duplicates().reset_index(drop=False, names='payment_type_id')
payment_id_dim['payment_type_name'] = payment_id_dim['payment_type'].map(payment_type_name)

#%%
# Merging to fact table \

fact_table = df.merge(passenger_count_dim, on='passenger_count')\
.merge(trip_dis_dim, on='trip_distance')\
.merge(rate_code_dim, on='RatecodeID')\
.merge(pickup_location_dim, on=['pickup_longitude', 'pickup_latitude'])\
.merge(dropoff_location_dim, on=['dropoff_longitude', 'dropoff_latitude'])\
.merge(date_dim, on=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])\
.merge(payment_id_dim, on='payment_type')

fact_table = fact_table[['VendorID', 'datetime_id', 'passenger_count_id', 'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id', 'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount']]


# %%
