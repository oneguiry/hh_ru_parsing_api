from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.chrome.options import Options


hh_ru_url = "https://hh.ru/search/resume?area=113&exp_period=all_time&filter_exp_industry=7.541&filter_exp_industry=7.538&filter_exp_industry=7.540&filter_exp_industry=7.539&job_search_status=looking_for_offers&job_search_status=active_search&job_search_status=has_job_offer&logic=normal&no_magic=true&order_by=relevance&ored_clusters=true&pos=full_text&search_period=0&text=Информационные+технологии&items_on_page=100&searchSessionId=86fc51dc-7bb6-4a1c-b0cf-008b5b4260d9"

chrome_options = Options()
chrome_options.add_argument("--disable-webrtc")

driver = webdriver.Chrome(options=chrome_options)
driver.get(hh_ru_url)


vacancies = set()
last_height = driver.execute_script("return document.body.scrollHeight")

while True:
	driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
	try:
		first_div = driver.find_element(By.XPATH, '//main[@class="resume-serp-content"]')
		resume_divs = first_div.find_elements(By.XPATH, './/div[@data-resume-id]')
		for resume_div in resume_divs:
			link_element = resume_div.find_element(By.XPATH, './/a[@data-qa="serp-item__title"]')
			resume_url = link_element.get_attribute('href')

			driver.execute_script("window.open('');")
			driver.switch_to.window(driver.window_handles[-1])
			driver.get(resume_url)
			driver.find_element(By.XPATH, '//div[@class="resume-applicant"]')
			div_info_resume = driver.find_element(By.XPATH, '//div[@class="resume-applicant"]')
			#print(div_info_resume.text)
			#full_name = div_info_resume.find_element(By.TAG, '//span[@class="blocko-text"]')
			gender = driver.find_element(By.XPATH, './/span[@data-qa="resume-personal-gender"]').text
			age = driver.find_element(By.XPATH, '//span[@data-qa="resume-personal-age"]').text
			location = driver.find_element(By.XPATH, '//span[@data-qa="resume-personal-address"]').text

			driver.find_element(By.XPATH, '//div[@class="resume-wrapper"]')
			name_title = driver.find_element(By.XPATH, '//span[@class="resume-block__title-text"]').text
			sallary = driver.find_element(By.XPATH, '//span[@class="resume-block__salary"]').text
			specialization = driver.find_element(By.XPATH, '//span[@data-qa=resume-block-specialization-category]').text


			print(div_info_resume, gender, age, location, name_title, sallary, specialization)


		
			time.sleep(3)  # Пауза для загрузки страницы
	except Exception as error:
		print(f"Error {error}")
		break