from google.cloud import bigquery
from google.cloud import exceptions
from typing import List, Dict
from logger import logger


class BigQueryClient:
    def __init__(self, project_id: str, dataset_id: str) -> None:
        self.client = bigquery.Client(project=project_id)
        self.full_dataset_id = f'{project_id}.{dataset_id}'

    def create_dataset_if_not_exists(self) -> None:
        try:
            self.client.get_dataset(self.full_dataset_id)
        except exceptions.NotFound:
            dataset = bigquery.Dataset(self.full_dataset_id)
            dataset.location = "US"
            self.client.create_dataset(dataset, timeout=30)
            logger.info(f'Dataset {self.full_dataset_id} criado com sucesso.')
        except Exception as exp:
            logger.error(f'Erro ao criar o dataset {self.full_dataset_id}. Err: {str(exp)}')

    def create_table_if_not_exists(self, table_id: str, schema: List[bigquery.SchemaField],
                                   partitioning_field: str = None) -> None:
        full_table_id = f'{self.full_dataset_id}.{table_id}'
        try:
            self.client.get_table(full_table_id)
            logger.info(f"Tabela {full_table_id} já existe.")
        except exceptions.NotFound:
            table = bigquery.Table(full_table_id, schema=schema)
            if partitioning_field:
                table.time_partitioning = bigquery.TimePartitioning(field=partitioning_field)
            self.client.create_table(table)
            logger.info(f"Tabela {full_table_id} criada com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao criar a tabela {full_table_id}. Erro: {str(e)}")

    def query(self, table_id: str, query_type: str = "last_extract_date", additional_query: str = "") -> List[Dict]:
        if query_type == "last_extract_date":
            query = f"""
                SELECT MAX(extract_data) AS last_extract_date
                FROM `{self.full_dataset_id}.{table_id}`
            """
        else:
            query = additional_query

        query_job = self.client.query(query)
        result = query_job.result()
        return [dict(row) for row in result]

    def insert_rows(self, data: List[Dict], table_id: str) -> None:
        full_table_id = f'{self.full_dataset_id}.{table_id}'

        self.create_dataset_if_not_exists()

        errors = self.client.insert_rows_json(full_table_id, data)
        if errors:
            logger.error(f"Erros ao inserir linhas: {errors}")
        else:
            logger.info(f'Dados inseridos com sucesso na tabela {full_table_id}.')
