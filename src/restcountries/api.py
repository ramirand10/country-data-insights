import requests
from logger import logger
from typing import List, Dict


class CountriesAPI:
    def __init__(self) -> None:
        self.base_url = 'https://restcountries.com/v3.1/all'

    def fetch_countries_data(self) -> List[Dict]:
        response = requests.get(self.base_url)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f'Erro na requisição: {response.status_code}')
            return []
