import requests
import psycopg2


def is_respond_error(api_data):

    error_list = api_data["errors"]
    if len(error_list) > 0:
        print("УВАГА! Виявлені помилки під час запиту: "+str(error_list))
        return True
    else:
        return False


def update_areas(url, api_key, conn_db):
    # створимо таблицю областей в базі, якщо її немає
    create_table_query = """
        CREATE TABLE IF NOT EXISTS areas (
            id SERIAL PRIMARY KEY,
            Ref CHAR(36) UNIQUE NOT NULL,
            Description VARCHAR(150),
            AreasCenter CHAR(36)
        );
        """
    cursor_ar = conn_db.cursor()  # streets
    cursor_ar.execute(create_table_query)
    conn_db.commit()

    data = {
        "apiKey": api_key,
        "modelName": "Address",
        "calledMethod": "getAreas",
        "methodProperties": {},
    }

    response = requests.post(url, json=data)
    api_data = response.json()
    if is_respond_error(api_data):
        return  # трапилась помилка

    object_list = api_data['data']

    for api_object in object_list:
        print(api_object['Description'])

        # Спробуємо оновити запис, і якщо він не існує, вставляємо новий рядок
        update_query = """
            INSERT INTO areas (Ref, Description, AreasCenter)
            VALUES (%s, %s, %s)
            ON CONFLICT (Ref) DO UPDATE
            SET Description = excluded.Description,
                AreasCenter = excluded.AreasCenter;
            """
        cursor_ar.execute(update_query, (api_object['Ref'], api_object['Description'], api_object['AreasCenter']))

        conn_db.commit()

    cursor_ar.close()


def update_streets_by_city(url, api_key, conn_db, city_ref):
    # створимо таблицю вулиць міста в базі, якщо її немає
    create_table_query = """
        CREATE TABLE IF NOT EXISTS streets (
            id SERIAL PRIMARY KEY,
            Ref CHAR(36) UNIQUE NOT NULL,
            Description VARCHAR(150),
            StreetsType VARCHAR(50),
            CityRef CHAR(36)
        );
        """
    cursor_str = conn_db.cursor()  # streets
    cursor_str.execute(create_table_query)
    conn_db.commit()

    num_page = 1  # запит з обробкою пагінації
    per_page = 100  # кількість об'єктів на сторінці

    while True:

        data = {
            "apiKey": api_key,
            "modelName": "Address",
            "calledMethod": "getStreet",
            "methodProperties": {
                "CityRef": city_ref,
                "Page": num_page,
                "Limit": per_page
            },
        }

        response = requests.post(url, json=data)
        api_data = response.json()
        if is_respond_error(api_data):
            break  # трапилась помилка

        object_list = api_data['data']
        if not object_list:
            break  # натрапили на порожній блок даних, завершуємо цикл

        # print(str(num_page) + " - " + str(len(object_list)))
        for api_object in object_list:
            # Спробуємо оновити запис, і якщо він не існує, вставляємо новий рядок
            update_query = """
            INSERT INTO streets (Ref, Description, StreetsType, CityRef)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (Ref) DO UPDATE
            SET Description = excluded.Description,
                StreetsType = excluded.StreetsType,
                CityRef = excluded.CityRef;
            """
            cursor_str.execute(update_query, (api_object['Ref'], api_object['Description'],
                                              api_object['StreetsType'], city_ref))

        conn_db.commit()
        num_page += 1

    cursor_str.close()


def update_cities(url, api_key, conn_db):

    # створимо таблицю міст доставлення в базі, якщо її немає
    create_table_query = """
    CREATE TABLE IF NOT EXISTS cities (
        id SERIAL PRIMARY KEY,
        Ref CHAR(36) UNIQUE NOT NULL,
        Description VARCHAR(150),
        SettlementTypeDescription VARCHAR(50),
        Area CHAR(36)
    );
    """
    cursor_city = conn_db.cursor()  #
    cursor_city.execute(create_table_query)
    conn_db.commit()

    num_page = 1    # запит з обробкою пагінації
    per_page = 100  # кількість об'єктів на сторінці

    while True:

        data = {
            "apiKey": api_key,
            "modelName": "Address",
            "calledMethod": "getCities",
            "methodProperties": {
                "Page": num_page,
                "Limit": per_page
            },
        }

        response = requests.post(url, json=data)
        api_data = response.json()
        if is_respond_error(api_data):
            break   # трапилась помилка

        object_list = api_data['data']
        if not object_list:
            break   # натрапили на порожній блок даних, завершуємо цикл

        print("Населені пункти з Новою поштою: сторінка "+str(num_page)+" - "+str(len(object_list)))

        for api_object in object_list:
            # Спробуємо оновити запис, і якщо він не існує, вставляємо новий рядок
            update_query = """
            INSERT INTO cities (Ref, Description, SettlementTypeDescription, Area)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (Ref) DO UPDATE
            SET Description = excluded.Description,
                SettlementTypeDescription = excluded.SettlementTypeDescription,
                Area = excluded.Area;
            """
            cursor_city.execute(update_query, (api_object['Ref'], api_object['Description'],
                                               api_object['SettlementTypeDescription'], api_object['Area']))

            conn_db.commit()

            update_streets_by_city(url, api_key, conn_db, api_object['Ref'])

        num_page += 1
        # if num_page == 2: break

    cursor_city.close()


def update_settlements(url, api_key, conn_db):

    # створимо таблицю населених пунктів в базі, якщо її немає
    create_table_query = """
    CREATE TABLE IF NOT EXISTS settlements (
        id SERIAL PRIMARY KEY,
        Ref CHAR(36) UNIQUE NOT NULL,
        Description VARCHAR(150),
        SettlementTypeDescription VARCHAR(50),
        Latitude VARCHAR(36),
        Longitude VARCHAR(36),
        RegionsDescription VARCHAR(50),
        Area CHAR(36),
        AreaDescription VARCHAR(50),
        Index1 VARCHAR(10),
        Index2 VARCHAR(10),
        Warehouse BOOLEAN
    );
    """
    cursor_ctw = conn_db.cursor()  # city town village
    cursor_ctw.execute(create_table_query)
    conn_db.commit()

    num_page = 1    # запит з обробкою пагінації
    per_page = 100  # кількість об'єктів на сторінці

    while True:

        data = {
            "apiKey": api_key,
            "modelName": "Address",
            "calledMethod": "getSettlements",
            "methodProperties": {
                "Page": num_page,
                "Limit": per_page
            },
        }

        response = requests.post(url, json=data)
        api_data = response.json()
        if is_respond_error(api_data):
            break   # трапилась помилка

        object_list = api_data['data']
        if not object_list:
            break   # натрапили на порожній блок даних, завершуємо цикл

        print("Населені пункти України: сторінка "+str(num_page)+" - "+str(len(object_list)))

        for api_object in object_list:
            # Спробуємо оновити запис, і якщо він не існує, вставляємо новий рядок
            update_query = """
            INSERT INTO settlements (Ref, Description, SettlementTypeDescription, Latitude, Longitude, 
                                        RegionsDescription, Area, AreaDescription, Index1, Index2, Warehouse)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Ref) DO UPDATE
            SET Description = excluded.Description,
                SettlementTypeDescription = excluded.SettlementTypeDescription,
                Latitude = excluded.Latitude,
                Longitude = excluded.Longitude,
                RegionsDescription = excluded.RegionsDescription,
                Area = excluded.Area,
                AreaDescription = excluded.AreaDescription,
                Index1 = excluded.Index1,
                Index2 = excluded.Index2,
                Warehouse = excluded.Warehouse;
            """
            cursor_ctw.execute(update_query, (api_object['Ref'],
                                              api_object['Description'], api_object['SettlementTypeDescription'],
                                              api_object['Latitude'], api_object['Latitude'],
                                              api_object['RegionsDescription'], api_object['Area'], api_object['AreaDescription'],
                                              api_object['Index1'], api_object['Index2'], api_object['Warehouse']))

        conn_db.commit()
        num_page += 1
        # if num_page == 2: break

    cursor_ctw.close()


if __name__ == '__main__':

    conn_db = psycopg2.connect(host="localhost", dbname="novaposhta", user="postgres", password="94027", port="5432")

    url = "https://api.novaposhta.ua/v2.0/json/"
    api_key = "0362f092a5fcc0dbd0bd516d6862ffb7"

    update_areas(url, api_key, conn_db)
    update_settlements(url, api_key, conn_db)
    update_cities(url, api_key, conn_db)

    conn_db.close()
