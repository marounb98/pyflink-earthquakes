import pandas as pd
from sqlalchemy import create_engine

df=pd.read_csv('Earthquakes.csv')

# clean data and rename columns
df.dropna()
df = df.drop(df.columns[[0]], axis=1)
df.drop(['No','Ref1','Source Description 1','Depth','Constant Deg.','Source No 2','Source Description 2','Type','Source No 3','Source Description 3'], axis = 1, inplace = True) 
df.rename(columns={'Date(UTC)': 'creationdate',
                   'Latitude': 'latitude','Longitude': 'longitude','Magnitude': 'magnitude'},
          inplace=True, errors='raise')

# Ingest in postgresDB
engine = create_engine('<connectionString>')
df.to_sql('earthquakes', engine)
print('data Ingested successfully in postgres DB.')

