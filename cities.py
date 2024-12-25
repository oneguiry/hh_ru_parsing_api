import requests
from datetime import datetime
import psycopg2
import random
import time
import logging
import urllib3
import certifi
from requests.exceptions import ConnectTimeout

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



hh_api_token = ''

db_config = {
    'dbname': 'parser',
    'user': 'postgres',
    'password': '1',
    'host': 'localhost',
    'port': '5432'
}


def create_table(conn):
    cursor = conn.cursor()

    create_table_query = """
        CREATE TABLE IF NOT EXISTS vacancies_top (
            id SERIAL PRIMARY KEY,
            title VARCHAR(200),
            area VARCHAR(50),
            salary_from VARCHAR(50),
            salary_to VARCHAR(50),
            id_vac INTEGER,
            skills VARCHAR(1000),
            status_vac VARCHAR(50),
            published_at TIMESTAMP WITH TIME ZONE,
            url VARCHAR(70),
            experience VARCHAR(50),
            employment VARCHAR(50),
            professional_roles VARCHAR(200),
            working_hours VARCHAR(50),
            work_format VARCHAR(50),
            schedule VARCHAR(50)
        )
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    logging.info("Таблица 'vacancies_top' успешно создана.")

# Функция для удаления таблицы vacancies
def drop_table(conn):
    cursor = conn.cursor()

    drop_table_query = "DROP TABLE IF EXISTS vacancies_top"
    cursor.execute(drop_table_query)

    conn.commit()
    cursor.close()
    logging.info("Таблица 'vacancies' успешно удалена.")


def create_tables_by_area():
    url = 'https://api.hh.ru/areas/113'
    
    # Заголовки запроса
    headers = {
        'Authorization': f'Bearer {hh_api_token}',
        'User-Agent': 'HH-User-Agent'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = response.json()
    with open("areas.txt", "w", encoding="utf-8") as file:
        for area in data['areas']:
            file.write(f'"{area["name"]}": "{area["id"]}"\n')
            for sub_area in area['areas']:
                file.write(f'"{sub_area["name"]}": "{sub_area["id"]}"\n')

def get_industry():
    url = 'https://api.hh.ru/industries'
    headers = {
        'Authorization': f'Bearer {hh_api_token}',
        'User-Agent': 'HH-User-Agent'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    print(response.json())

def get_areas_roles():
    professional_roles = {}
    with open('specialties.txt', 'r', encoding='utf-8') as file:
        for line in file:
            name, id = line.strip().split(":")
            professional_roles[name.strip('"')] = int(id.strip().strip('"'))

    areas = {}
    with open('areas.txt', 'r', encoding='utf-8') as file:
        for line in file:
            name, id = line.strip().split(":")
            areas[name.strip('"')] = int(id.strip().strip('"'))

    return areas, professional_roles

def get_vacancy(area_id, professional_role, page):
    url = 'https://api.hh.ru/vacancies'
    params = {
        "area": area_id,
        "industry": 7,
        "page": page,
        #"text": {professional_role},
        "host": "hh.ru"
    }
    headers = {
        'Authorization': f'Bearer {hh_api_token}',
        'User-Agent': 'HH-User-Agent'
    }
    print(url, params)
    retries = 5
    for _ in range(retries):
        try:
            response = requests.get(url, params=params, headers=headers, proxies={},  verify=certifi.where(), timeout=30)
            response.raise_for_status()
        except ConnectTimeout:
            print("Превышено время ожидания")
            time.sleep(3)
        except requests.exceptions.RequestException as e:
            print(f"Ошибка запроса: {e}")
    return response.json()

def get_vacancy_skills(vac_id):
    url = f'https://api.hh.ru/vacancies/{vac_id}'
    headers = {
        'Authorization': f'Bearer {hh_api_token}',
        'User-Agent': 'HH-User-Agent'
    }
    retries = 5
    for _ in range(retries):
        try:
            response = requests.get(url, headers=headers, proxies={}, verify=certifi.where(), timeout=30)
        except ConnectTimeout:
            print("Превышено время ожидания, новая попытка")
            time.sleep(3)
        except requests.exceptions.RequestException as e:
            print(f"Ошибка запроса: {e}")
    data = response.json()
    skills = [skill['name'] for skill in data.get('key_skills', [])]
    skills_str = ', '.join(skills)
    if len(skills_str) > 1000:
        skills_str = skills_str[:1000]
    return skills_str

def get_vacancies():
    areas, all_variant_roles = get_areas_roles()

    with psycopg2.connect(**db_config) as conn:
        drop_table(conn)
        create_table(conn)

        for city, city_id in areas.items():
            #for vacancy, vacancy_id in all_variant_roles.items():
            page = 0
            max_page = 100
            while page < max_page - 1:
                try:
                    data = get_vacancy(city_id, None, page)
                    if not data.get('items'):
                        break
                    max_page = data.get('pages')
                    with conn.cursor() as cursor:
                        for item in data['items']:
                            title = item['name']
                            area = item['area'].get('name')
                            if item.get('salary'):
                                salary_from = item['salary'].get('from') 
                                salary_to = item['salary'].get('to')
                            else:
                                salary_from = None
                                salary_to = None
                            id_vac = item['id']
                            time.sleep(1)
                            skills = get_vacancy_skills(id_vac)
                            status_vac = item['type'].get('id')
                            published_at = datetime.strptime(item['published_at'], "%Y-%m-%dT%H:%M:%S%z")
                            url = item['apply_alternate_url']
                            experience = item['experience'].get('name')
                            employment = item['employment'].get('name')
                            professional_roles = item['professional_roles'][0]['name']
                            if item.get('working_hours'):
                                working_hours = item['working_hours'][0].get('name')
                            else:
                                working_hours = None
                            if item.get('work_format'):
                                work_format = item['work_format'][0].get('name')
                            else:
                                work_format = None
                            schedule = item['schedule']['name']

                            insert_query = """
                            INSERT INTO vacancies_top 
                            (title, area, salary_from, salary_to, id_vac, skills, status_vac, published_at, url, experience, employment, professional_roles, working_hours, work_format, schedule) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor.execute(insert_query,
                                        (title, area, salary_from, salary_to, id_vac, skills, status_vac, published_at, url, experience, employment, professional_roles, working_hours, work_format, schedule))
                            conn.commit()
                        #logging.info(f"Данные по странице {page} проффессии {vacancy} сохранены")
                        time.sleep(2)
                        page += 1
                except requests.HTTPError as e:
                    print(f"Ошибка при обработке города {city}: {e}")
                    continue 
        conn.commit()
    logging.info("Парсинг завершен")

get_vacancies()
#get_industry()
#create_tables_by_area()
