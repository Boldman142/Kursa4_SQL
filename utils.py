from abc import ABC, abstractmethod
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
        print(employer_id)
        while True:
            params = {
                'employer_id': employer_id,  # id работодателя
                'page': page,  # Номер страницы
                'per_page': 50,  # Кол-во вакансий на 1 странице
            }
            req = requests.get('https://api.hh.ru/vacancies', params)
            # print(type(req.json()))

            vacancies.extend(req.json().get('items'))
            page += 1
            if page == 5:  # req.json()['pages']:  Если делать так, чтобы
                # все вакансии выходили, постоянно падает как-будто переполняется
                req.close()
                data.append({
                    'employer_data': employer_data,
                    'vacancies': vacancies
                })
                break
        print("Norm")
    return data


def create_database(database_name: str, params: dict):
    """Создает базу данных с указанным именем и параметрами для конекта"""
    # conn = psycopg2.connect(dbname='postgres', **params)
    conn = psycopg2.connect(host='localhost',
                            database='postgres',
                            user='postgres',
                            password='JutsU#69')
    conn.autocommit = True
    cur = conn.cursor()
    # cur.execute(f"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity "
    #             f"WHERE pg_stat_activity.datname = '{database_name}' "
    #             f"AND pid <> pg_backend_pid()")
    cur.execute(f"DROP DATABASE {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")

    conn.close()

    # conn = psycopg2.connect(dbname=database_name, **params)
    conn = psycopg2.connect(host='localhost',
                            database=f'{database_name}',
                            user='postgres',
                            password='JutsU#69')

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
                salary_from INT,
                salary_to INT,
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

    # conn = psycopg2.connect(dbname=database_name, **params)
    conn = psycopg2.connect(host='localhost',
                            database=f'{database_name}',
                            user='postgres',
                            password='JutsU#69')

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
                if vacancy is None:
                    continue
                if vacancy.get('salary') is None:
                    salary_from = None
                    salary_to = None
                    salary_currency = None
                else:
                    salary_from = vacancy.get('salary').get('from')
                    salary_to = vacancy.get('salary').get('to')
                    salary_currency = vacancy.get('salary').get('currency')

                cur.execute(
                    """
                        INSERT INTO vacancies (employer_id, 
                        title, salary_from, salary_to, currency, city, 
                        url_vacancies, requirements, employment, experience)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                    (employer_id, vacancy['name'],
                     salary_from, salary_to, salary_currency, vacancy['area']['name'],
                     vacancy['alternate_url'], vacancy.get('snippet').get('requirement'),
                     vacancy.get('employment').get('name'),
                     vacancy.get('experience').get('name'))
                )
    conn.commit()
    conn.close()


class DBMForm(ABC):

    @abstractmethod
    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании."""
        pass

    @abstractmethod
    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии, зарплаты и ссылки на вакансию."""
        pass

    @abstractmethod
    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""
        pass

    @abstractmethod
    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше
        средней по всем вакансиям."""
        pass

    @abstractmethod
    def get_vacancies_with_keyword(self, text):
        """Получает список всех вакансий, в названии которых
        содержатся переданные в метод слова, например python."""
        pass


class DBManager(DBMForm):

    def __init__(self, connect):
        self.conn = connect

    def get_companies_and_vacancies_count(self):
        with self.conn.cursor() as cur:
            cur.execute("""SELECT title_employer, COUNT(*)
            FROM vacancies
            JOIN employers USING(employer_id)
            GROUP BY title_employer""")
            data = cur.fetchall()
            print("Организация - количество вакансий")
            num = 0
            for i in data:
                num += 1
                print(f'{num}) {i[0]} - {i[1]}')

    def get_all_vacancies(self):
        with self.conn.cursor() as cur:
            cur.execute("""SELECT (employers.title_employer, title, salary_from, salary_to, 
                    currency, url_vacancies) FROM vacancies, employers
            WHERE employers.employer_id = vacancies.employer_id
            ORDER BY employers.title_employer""")
            data = cur.fetchall()
            num = 0
            for i in data:
                clear_data = i[0][1:-1].split(",")
                num += 1
                employer = clear_data[0]
                vacant = clear_data[1]
                salary_from = clear_data[2]
                salary_to = clear_data[3]
                cur = clear_data[4]
                url = clear_data[5]
                if len(cur) == 0:
                    salary = 'оплата не указана'
                elif (len(salary_from) != 0) and (len(salary_to) == 0):
                    salary = f'оплата от {salary_from} {cur}'
                elif (len(salary_from) == 0) and (len(salary_to) != 0):
                    salary = f'оплата до {salary_to} {cur}'
                else:
                    salary = f'оплата от {salary_from} до {salary_to} {cur}'
                print(f'{num}) {employer} - Требуется {vacant}, {salary}, '
                      f'ссылка на вакансию - {url}')

    def get_avg_salary(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT AVG(salary_from) as avg_from, 
                AVG(salary_to) as avg_to, currency 
                FROM vacancies 
                WHERE salary_to IS NOT NULL AND salary_from IS NOT NULL
                GROUP BY currency""")
            data = cur.fetchall()
            for i in range(len(data)):
                print(f'Средняя оплата в {data[i][2]}: от - {round(data[i][0])}; '
                      f'до - {round(data[i][1])}')

    def get_vacancies_with_higher_salary(self):
        # SELECT * FROM vacancies
        # WHERE salary_from > AVG(salary_from) AND salary_to > AVG(salary_to)

        pass

    def get_vacancies_with_keyword(self, text):
        with self.conn.cursor() as cur:
            text_low = text.lower()
            text_hight = text.capitalize()
            cur.execute(
                f"""SELECT employers.title_employer, title, salary_from, salary_to, 
                currency, city, requirements, url_vacancies, employment, experience 
                FROM vacancies, employers
                WHERE title LIKE '%{text_low}%'
                UNION
                SELECT employers.title_employer, title, salary_from, salary_to, 
                currency, city, requirements, url_vacancies, employment, experience 
                FROM vacancies, employers
                WHERE title LIKE '%{text_hight}%'"""
            )
            data = cur.fetchall()
            num = 0
            for clear_data in data:
                num += 1
                employer = clear_data[0]
                vacant = clear_data[1]
                salary_from = clear_data[2]
                salary_to = clear_data[3]
                cur = clear_data[4]
                city = clear_data[5]
                requirements = clear_data[6]
                url = clear_data[7]
                employment = clear_data[8]
                experience = clear_data[9]
                if cur is None:
                    salary = 'оплата не указана'
                elif (salary_from is not None) and (salary_to is None):
                    salary = f'оплата от {salary_from} {cur}'
                elif (salary_from is None) and (salary_to is not None):
                    salary = f'оплата до {salary_to} {cur}'
                else:
                    salary = f'оплата от {salary_from} до {salary_to} {cur}'
                print(f'{num}) {employer} - Требуется {vacant}, {salary}, '
                      f'в городе {city}, \nТребования: {requirements} '
                      f'\nнеобходимый опыт: {experience}, {employment}. '
                      f'ссылка на вакансию - {url}')



try:
    conn_ = psycopg2.connect(host='localhost',
                             database='head_hunt',
                             user='postgres',
                             password='JutsU#69')
    a = DBManager(conn_)
    # a.get_companies_and_vacancies_count()
    # a.get_all_vacancies()
    # a.get_avg_salary()
    # a.get_vacancies_with_higher_salary
    # a.get_vacancies_with_keyword("механик")
    # print(a.get_companies_and_vacancies_count())

finally:
    conn_.close()
