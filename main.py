import requests
from sqlalchemy import create_engine, ForeignKey, Column, Integer, String, Boolean
from sqlalchemy.orm import DeclarativeBase, sessionmaker, relationship


def is_respond_error(api_data):
    error_list = api_data["errors"]
    if len(error_list) > 0:
        print("УВАГА! Виявлені помилки під час запиту: "+str(error_list))
        return True
    else:
        return False


class Base(DeclarativeBase):
    id = Column(Integer, autoincrement=True, primary_key=True, nullable=False, unique=True)
    ref = Column(String(36), unique=True, nullable=False)
    description = Column(String(150))


class Areas(Base):
    __tablename__ = "areas"
    areas_center = Column(String(36), nullable=False, index=True)

class Streets(Base):
    __tablename__ = "streets"
    streets_type = Column(String(50))
    city_ref = Column(String(36), ForeignKey("cities.ref"), nullable=False, index=True)
    city = relationship("Cities")


class Cities(Base):
    __tablename__ = "cities"
    settlement_type = Column(String(50))
    area_ref = Column(String(36), ForeignKey("areas.ref"), index=True)
    area = relationship("Areas")


class Settlements(Base):
    __tablename__ = "settlements"
    settlement_type = Column(String(50))
    latitude = Column(String(36))
    longitude = Column(String(36))
    regions_description = Column(String(50))
    area = Column(String(36))
    area_description = Column(String(50))
    index1 = Column(String(10))
    index2 = Column(String(10))
    warehouse = Column(Boolean)


def update_areas():

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
        row_db = session.query(Areas).filter_by(ref=api_object['Ref']).first()
        if row_db:
            row_db.description = api_object['Description']
            row_db.areas_center = api_object['AreasCenter']
        else:
            new_row_db = Areas(ref=api_object['Ref'],
                               description=api_object['Description'],
                               areas_center=api_object['AreasCenter'])
            session.add(new_row_db)

        session.commit()


def update_streets_by_city(city_ref):
    num_page = 1  # запит з обробкою пагінації
    per_page = 100  # кількість об'єктів на сторінці
    q_streets = 0
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

        q_streets = q_streets + len(object_list)  # підсумок кількості вулиць
        for api_object in object_list:
            # Спробуємо оновити запис, і якщо він не існує, вставляємо новий рядок
            row_db = session.query(Streets).filter_by(ref=api_object['Ref']).first()
            if row_db:
                row_db.description = api_object['Description']
                row_db.streets_type = api_object['StreetsType']
                row_db.city_ref = city_ref
            else:
                new_row_db = Streets(ref=api_object['Ref'],
                                     description=api_object['Description'],
                                     city_ref=city_ref,
                                     streets_type=api_object['StreetsType'])
                session.add(new_row_db)

        session.commit()
        num_page += 1


def update_cities_streets():
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
            row_db = session.query(Cities).filter_by(ref=api_object['Ref']).first()
            if row_db:
                row_db.description = api_object['Description']
                row_db.settlement_type = api_object['SettlementTypeDescription']
                row_db.area_ref = api_object['Area']
            else:
                new_row_db = Cities(ref=api_object['Ref'],
                                    description=api_object['Description'],
                                    settlement_type=api_object['SettlementTypeDescription'],
                                    area_ref=api_object['Area'])
                session.add(new_row_db)

            session.commit()

            update_streets_by_city(api_object['Ref'])

        num_page += 1
        q_streets = session.query(Streets).count()
        print("     Оброблено вулиць: " + str(q_streets))
        # if num_page == 2: break


def update_settlements():
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
            row_db = session.query(Settlements).filter_by(ref=api_object['Ref']).first()
            if row_db:
                row_db.description = api_object['Description']
                row_db.settlement_type = api_object['SettlementTypeDescription']
                row_db.latitude = api_object['Latitude']
                row_db.longitude = api_object['Longitude']
                row_db.regions_description = api_object['RegionsDescription']
                row_db.area = api_object['Area']
                row_db.area_description = api_object['AreaDescription']
                row_db.index1 = api_object['Index1']
                row_db.index2 = api_object['Index2']
                row_db.warehouse = bool(int(api_object['Warehouse']))
            else:
                new_row_db = Settlements(ref=api_object['Ref'],
                                         description=api_object['Description'],
                                         settlement_type=api_object['SettlementTypeDescription'],
                                         latitude=api_object['Latitude'],
                                         longitude=api_object['Longitude'],
                                         regions_description=api_object['RegionsDescription'],
                                         area=api_object['Area'],
                                         area_description=api_object['AreaDescription'],
                                         index1=api_object['Index1'],
                                         index2=api_object['Index2'],
                                         warehouse=bool(int(api_object['Warehouse'])))
                session.add(new_row_db)

        session.commit()
        num_page += 1
        # if num_page == 2: break


if __name__ == '__main__':
    # dialect+driver://username:password@host:port/database
    engine = create_engine("postgresql+psycopg2://postgres:94027@localhost:5432/novaposhta")
    # створення сесії для роботи з базою даних
    Session = sessionmaker(bind=engine)
    session = Session()

    url = "https://api.novaposhta.ua/v2.0/json/"
    api_key = "0362f092a5fcc0dbd0bd516d6862ffb7"

    Base.metadata.create_all(engine)  # створення таблиць які успадкувались від об'єкта Base

    update_areas()
    update_settlements()
    update_cities_streets()

    session.close()
