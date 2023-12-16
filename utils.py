from typing import Any

import requests


def get_hh_data(ids: list):
    """Получает все вакансии от всех работодателей,
    список которых передается"""

    params = {
        'employer_id': 39305,  # ID 2ГИС
        # 'area': area,         # Поиск в зоне
        # 'page': page,         # Номер страницы
        # 'per_page': 100  # Кол-во вакансий на 1 странице
    }
    req = requests.get('https://api.hh.ru/vacancies', params)
    data = req.content.decode()
    req.close()
    return data


def create_database(database_name: str, params: dict):
    """Создает базу данных с указанным именем и параметрами для конекта"""


def save_data_to_database(data: list[dict[str, Any]], database_name: str, params: dict):
    """Сохраняет переданные данные в указанную базу данных"""
