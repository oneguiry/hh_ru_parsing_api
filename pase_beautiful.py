import requests
from bs4 import BeautifulSoup
import csv
import re


TYPE_EMPLOYMENT = {
    'Полная занятость': '1',
    'Частичная занятость': '2',
    'Проектная работа': '3',
    'Волонтерство': '4',
    'Оформление по ГПХ или по совместительству': '5',
    'Стажировка': '6'
}

TYPE_EXPERIENCE = {
    'не требуется': '1',
    '1–3 года': '2',
    '3–6 лет': '3',
    'более 6 лет': '4'
}


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
}

url = 'https://hh.ru/search/vacancy?L_save_area=true&area=113&items_on_page=20&search_field=name&search_field=company_name&search_field=description&enable_snippets=false&industry=7&text=&page=0'

def get_salary(msg: str):
    if msg == 'Уровень дохода не указан':
        return None
    else:
        msg = msg.replace('\xa0', ' ').replace(' ', '')
        return [int(num) for num in re.findall(r'\d+', msg)]

def get_work_hours(msg: str):
    msg = msg.replace('\xa0', ' ').replace(' ', '')
    number = re.findall(r'\d+', msg)
    if number:
        return int(number[0])
    else:
        return None

response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'lxml')

with open('vacancies.csv', 'w', newline='', encoding='utf-8') as csvfile:
    # Настраиваем запись
    fieldnames = [
        'Title', 'Salary', 'Experience',
        'Type of employment', 'Schedule', 'Working hours', 'Type of work', 'Views'
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()  # Записываем заголовки

    vacancies = soup.find_all('div', class_='magritte-redesign')
    for vacancy in vacancies:
        h2 = vacancy.find('h2', class_='bloko-header-section-2')
        span = h2.find('span')
        list_v = span.find('a', href=True)
        vacancy_url = list_v['href']
        print(vacancy_url)

        response = requests.get(vacancy_url, headers=headers)
        new_page = BeautifulSoup(response.text, 'lxml')
        info_header = new_page.find('div', class_='bloko-columns-row')
        spans = info_header.find_all('span')
        i = 0
        for span in spans:
            print(i, span.text.strip())
            i = i+1
            if i==12:
                break
        #salary = get_salary(spans[0].text.strip())
        title = info_header.find('div', {'data-qa': 'vacancy-title'})
        type_emp = info_header.find('p', {'data-qa': 'vacancy-view-employment-mode'})
        #print(type_emp)
        #experience = TYPE_EXPERIENCE.get(spans[2].text.strip())
        #employment_type = TYPE_EMPLOYMENT.get(spans[3].text.strip())
        #work_hours = get_work_hours(spans[4].text.strip())
        #print(experience, employment_type, work_hours, salary)
        """
        writer.writerow({
                    'Title': title.text.strip() if title else 'None',
                    'Salary': salary.text.strip() if salary else 'None',
                    'Experience': experience.text.strip() if experience else 'None',
                    'Type of employment': TYPE_EMPLOYMENT.get(employment_type.text.strip()) if employment_type else 'None',
                    'Schedule': schedule.text.strip() if schedule else 'None',
                    'Working hours': work_hours.text.strip() if work_hours else 'None',
                    'Type of work': work_format.text.strip() if work_format else 'None',
                    'Views': views.text.strip() if views else 'None',
                })
        """