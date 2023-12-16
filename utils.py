from typing import Any

import psycopg2
import requests


def get_hh_data(ids: list):
    """Получает все вакансии от всех работодателей,
    список которых передается"""
    data = []
    for employer_id in ids:
        page = 0
        vacancies = []
        req_emp = requests.get(f'https://api.hh.ru/employers/{employer_id}').json()
        # print(req_emp.json())
        employer_data = {
            'id': req_emp['id'],
            'name': req_emp['name'],
            'site': req_emp.get('site_url'),
            'url_hh': req_emp['alternate_url']
        }
        while True:
            params = {
                'employer_id': employer_id,  # id работодателя
                'page': page,  # Номер страницы
                'per_page': 50  # Кол-во вакансий на 1 странице
            }
            req = requests.get('https://api.hh.ru/vacancies', params)
            # print(req.json())
            vacancies.extend(req.json()['items'])
            page += 1
            if page == 2:  # req.json()['pages']:
                req.close()
                data.append({
                    'employer_data': employer_data,
                    'vacancies': vacancies
                })
                break
    return data


def create_database(database_name: str, params: dict):
    """Создает базу данных с указанным именем и параметрами для конекта"""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")

    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE employers (
                employer_id SERIAL PRIMARY KEY,
                title_employer VARCHAR(255) NOT NULL,
                employer_id_hh INTEGER NOT NULL,
                HH_url TEXT NOT NULL,
                site_employer TEXT
            )
        """)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE vacancies (
                vacancy_id SERIAL PRIMARY KEY,
                employer_id INT REFERENCES employers(employer_id),
                title VARCHAR NOT NULL,
                salary_from INT DEFAULT 0,
                salary_to INT DEFAULT 0,
                currency VARCHAR(25) DEFAULT 'Не указано',
                city VARCHAR(50) NOT NULL,
                url_vacancies TEXT NOT NULL,
                requirements TEXT,
                employment VARCHAR(50),
                experience VARCHAR(50)               
            )
        """)

    conn.commit()
    conn.close()


def save_data_to_database(data: list[dict[str, Any]], database_name: str, params: dict):
    """Сохраняет переданные данные в указанную базу данных"""

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for employer in data:
            employer_now = employer['employer_data']
            cur.execute(
                """
                INSERT INTO employers (title_employer, 
                employer_id_hh, HH_url, site_employer)
                VALUES (%s, %s, %s, %s)
                RETURNING employer_id
                """,
                (employer_now['name'], employer_now['id'],
                 employer_now['url_hh'], employer_now.get('site'))
            )
            employer_id = cur.fetchone()[0]
            vacancies_all = employer['vacancies']
            for vacancy in vacancies_all:
                cur.execute(
                    """
                        INSERT INTO vacancies (employer_id, 
                        title, salary_from, salary_to, currency, city, 
                        url_vacancies, requirements, employment, experience)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                    (employer_id, vacancy['name'],
                     vacancy.get('salary').get('from'), vacancy.get('salary').get('to'),
                     vacancy.get('salary').get('currency'), vacancy['area']['name'],
                     vacancy['alternate_url'], vacancy.get('snippet').get('requirement'),
                     vacancy.get('employment').get('name'),
                     vacancy.get('experience').get('name'))
                )
