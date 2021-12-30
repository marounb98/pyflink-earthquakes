from pyflink.table import EnvironmentSettings,StreamTableEnvironment,DataTypes
from pyflink.table.udf import udf
from pyflink.datastream import StreamExecutionEnvironment
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from geopy.geocoders import Nominatim

ddl_jdbc_source = """CREATE TABLE earthquakes (
                            index BIGINT,
                            creationdate STRING,
                            latitude DOUBLE PRECISION,
                            longitude DOUBLE PRECISION,
                            magnitude DOUBLE PRECISION
                    ) WITH (
                        'connector'= 'jdbc',
                        'url' = 'jdbc:postgresql://postgres:5432/ds_project',
                        'table-name' = 'public.earthquakes',
                        'username' = 'postgres',
                        'password' = 'postgres'
                    )"""

ddl_jdbc_sink = """CREATE TABLE earthquakedetails (
                        index BIGINT,
                        creationdate STRING,
                        magnitude DOUBLE PRECISION,
                        severity STRING,
                        country STRING
                ) WITH (
                    'connector'= 'jdbc',
                    'url' = 'jdbc:postgresql://postgres:5432/ds_project',
                    'table-name' = 'public.earthquakedetails',
                    'username' = 'postgres',
                    'password' = 'postgres'
                )"""

geolocator = Nominatim(user_agent="geoapiExercises")

@udf(input_types=DataTypes.STRING(), result_type=DataTypes.STRING())
def check_severity(magnitude):
    if(magnitude<=3.9):
        return 'minor'
    if(magnitude>= 4.0 and magnitude<=4.9):
        return 'light'
    if(magnitude>= 5.0 and magnitude<=5.9):
        return 'moderate'
    if(magnitude>= 6.0 and magnitude<=6.9):
        return 'strong'
    if(magnitude>= 7.0 and magnitude<=7.9):
        return 'major'
    if(magnitude>= 8.0):
        return 'great'

def check_country(long,lat):
    location = geolocator.reverse(str(long)+","+str(lat))
    if location is not None:
        res = location.raw['address']['country']
    else:
        res = 'N/A'
    return res

sink_table_keys = ['index', 'creationdate', 'magnitude', 'severity', 'country']
def filterKeys(document):
    return {key: document[key] for key in sink_table_keys }

def send_to_elastic(df):
    print('sending',df['index'])
    yield {
            "_index": 'theearthquakedetails',
            "_type": "_doc",
            "_id" : f"{df['index']}",
            "_source": filterKeys(df)
        }

if __name__ == '__main__':

    # Create Table Environement
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    config = t_env.get_config().get_configuration()
    config.set_string("taskmanager.memory.task.off-heap.size", "80mb") #512mb

    create_jdbc_source = t_env.execute_sql(ddl_jdbc_source)
    create_jdbc_sink = t_env.execute_sql(ddl_jdbc_sink)

    t_env.register_function("SEVERITY_CHECKER", check_severity)


    # Using the SEVERITY_CHECKER UDF in order to classify the severity of the earthquakes
    # and insert the results to earthquakedetails sink table
    
    t_env.execute_sql("""INSERT INTO earthquakedetails (index,creationdate,magnitude,severity,country) 
                        SELECT index, creationdate, magnitude, SEVERITY_CHECKER(magnitude),creationdate FROM earthquakes""")
    

    # Configuring the ElasticSearch client
    es = Elasticsearch(
    cloud_id="<cloudid>",
    http_auth=("<username>", "<password>"))

    resultSource = t_env.sql_query("""SELECT * FROM earthquakes""")
    resultSink = t_env.sql_query("""SELECT * FROM earthquakedetails""")

    resultSourcePd = resultSource.to_pandas()
    source_df_iter = resultSourcePd.iterrows()

    resultSinkPd = resultSink.to_pandas()

    # Mapping the records from source and sink tables in order to get the country
    # where the earthquakes happened and ingest them into an ElasticSearch Index


    for index, row in source_df_iter:
        df = resultSinkPd.loc[resultSinkPd['index'] == row['index']]
        df['country']=check_country(row['longitude'],row['latitude'])
        helpers.bulk(es, send_to_elastic(df))


    # reverse row order
    # resultSinkPd = resultSinkPd.loc[::-1, :]
