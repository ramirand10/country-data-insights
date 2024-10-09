from google.cloud import bigquery

schema = [
    bigquery.SchemaField('extract_data', 'TIMESTAMP', mode='NULLABLE', description='Date of data extraction or insertion'),
    bigquery.SchemaField('id', 'STRING', mode='REQUIRED', description='Unique identifier based on cca3'),
    bigquery.SchemaField('name', 'STRING', mode='NULLABLE', description='Country name'),
    bigquery.SchemaField('capital', 'STRING', mode='NULLABLE', description='Capital city'),
    bigquery.SchemaField('population', 'INTEGER', mode='NULLABLE', description='Country population size'),
    bigquery.SchemaField('area', 'FLOAT', mode='NULLABLE', description='Geographical size of the country'),
    bigquery.SchemaField('languages', 'STRING', mode='REPEATED', description='List of official languages'),
    bigquery.SchemaField('region', 'STRING', mode='NULLABLE', description='Geographic region'),
    bigquery.SchemaField('subregion', 'STRING', mode='NULLABLE', description='Geographic subregion'),
    bigquery.SchemaField('currencies', 'STRING', mode='REPEATED', description='List of currencies'),
    bigquery.SchemaField('timezones', 'STRING', mode='REPEATED', description='List of time zones'),
    bigquery.SchemaField('json_data', 'STRING', mode='NULLABLE', description='Raw JSON data from the API')

]
