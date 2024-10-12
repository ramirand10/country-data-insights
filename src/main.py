from datetime import datetime
import functions_framework
from logger import logger
from restcountries.api import CountriesAPI
from schema.schema import schema
from gcp.bigquery.bigquery import BigQueryClient
import json

GCP_PROJECT_ID = 'data-integration-hub-437714'
GCP_DATASET = 'country_raw'
TABLE_ID = 'countries_data'


@functions_framework.http
def main(request):
    countries_api = CountriesAPI()

    try:
        countries_data = countries_api.fetch_countries_data()
        logger.info("Dados obtidos com sucesso da API Rest Countries.")
    except Exception as e:
        logger.error(f"Erro ao buscar dados da API ou processar datas: {str(e)}")
        return {'error': 'Erro ao buscar dados da API.'}, 500

    bigquery_client = BigQueryClient(GCP_PROJECT_ID, GCP_DATASET)
    bigquery_client.create_table_if_not_exists(TABLE_ID, schema)

    result = bigquery_client.query(TABLE_ID)
    last_extract_date = result[0].get('last_extract_date', None) if result else None

    if last_extract_date:
        logger.info(f"Última data de extração: {last_extract_date}")
    else:
        logger.info("Nenhuma data de extração encontrada. Inserindo todos os registros.")

    rows_to_insert = []
    for country in countries_data:
        rows_to_insert.append({
            'extract_data': datetime.utcnow().isoformat(),
            'id': country.get('cca3', ''),
            'name': country.get('name', {}).get('common', ''),
            'capital': country.get('capital', [''])[0],
            'population': country.get('population', 0),
            'area': country.get('area', 0.0),
            'languages': list(country.get('languages', {}).values()),
            'region': country.get('region', ''),
            'subregion': country.get('subregion', ''),
            'currencies': list(country.get('currencies', {}).keys()),
            'timezones': country.get('timezones', []),
            'json_data': json.dumps(country)
        })

    bigquery_client.insert_rows(rows_to_insert, TABLE_ID)
    logger.info("Processo concluído com sucesso!")

    return {'message': 'Dados processados com sucesso!'}, 200
