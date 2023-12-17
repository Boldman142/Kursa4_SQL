import psycopg2

from config import config
from utils import get_hh_data, create_database, save_data_to_database, DBManager


def main():
    employer_ids = [
        1740,  # Яндекс
        1035394,  # КБ
        64174,  # 2гис
        23427,  # РЖД
        39305,  # газпром
        78638,  # тинькофф
        1473866,  # Сбер сервис
        2211644,  # Росатом
        3543069,  # Минэнерго
        2624085,  # Вкусно и точка
        1373  # Аэрофлот
    ]
    params = config()
    data = get_hh_data(employer_ids)
    #  print(data)
    create_database('head_hunt', params)
    save_data_to_database(data, 'head_hunt', params)
    print('Получены вакансии с сайта HH.ru, и сохранены в базу данных.')
    while True:
        try:
            conn_ = psycopg2.connect(host='localhost',
                                     database='head_hunt',
                                     user='postgres',
                                     password='JutsU#69')
            user_choice = int(input('\nВведите номер цифру того, что теперь вы хотите сделать:\n'
                                    '1 - Посмотреть список всех компаний и количество '
                                    'вакансий у каждой компании.\n'
                                    '2 - Посмотреть список всех вакансий с указанием'
                                    ' названия компании, названия вакансии, зарплаты'
                                    ' и ссылки на вакансию.\n'
                                    '3 - Получить среднюю оплату по всем вакансиям.\n'
                                    '4 - Посмотреть все вакансии, у которых зарплата выше'
                                    ' средней по всем вакансиям.\n'
                                    '5 - Посмотреть вакансии, в описании которых есть'
                                    ' указанное вами слово.\n'
                                    '0 - Выход из программы.\n'))
            manager = DBManager(conn_)
            if user_choice not in range(6):
                print('Данное число не предусмотрено')
                continue
            elif user_choice == 0:
                print('Всего хорошего! <3')
                exit()
            elif user_choice == 1:
                manager.get_companies_and_vacancies_count()
                continue
            elif user_choice == 2:
                manager.get_all_vacancies()
                continue
            elif user_choice == 3:
                manager.get_avg_salary()
                continue
            elif user_choice == 4:
                print('Увы, пока в разработке')
                continue
                # manager.get_vacancies_with_higher_salary()
            else:
                user_word = input('Введите слово, вакансии с которым вам показать:\n')
                manager.get_vacancies_with_keyword(user_word)
                continue

        except ValueError:
            print('Необходимо ввести цифру\n')
            continue
        finally:
            conn_.close()


if __name__ == "__main__":
    main()
