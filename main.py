from config import config
from utils import get_hh_data, create_database, save_data_to_database


def main():
    employer_ids = [
        # 1740,  # Яндекс
        64174,  # 2гис
        39305,  # газпром
        # 78638,  # тинькофф
        # 1473866,  # Сбер сервис
        # 2211644,  # Росатом
        # 3543069,  # Минэнерго
        # 2624085,  # Вкусно и точка
        # 23427,  # РЖД
        # 1373  # Аэрофлот
    ]
    params = config()
    data = get_hh_data(employer_ids)
    print(data)
    # create_database('HeadHunt', params)
    # save_data_to_database(data, 'HeadHunt', params)


if __name__ == "__main__":
    main()
