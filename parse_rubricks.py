import json
import os
import time
import base64
from functools import wraps
import random
import logging
import os

from bs4 import BeautifulSoup as soup
import pandas as pd
import openpyxl
import requests
import asyncio
import aiohttp
from fake_useragent import UserAgent
import coloredlogs

from wutil import get_logger

coloredlogs.install(level='INFO')
module_logger = get_logger("parse_links")

# Учетные данные для прокси
with open(os.path.join(os.getcwd(), f"config/proxy_settings.json")) as f:
    proxy_settings = json.load(f)
proxy_username, proxy_password = proxy_settings['proxy_username'], proxy_settings['proxy_password']
proxy_auth = {
    f"Proxy-Authorization": f"""Basic {base64.b64encode(f"{proxy_username}:{proxy_password}".encode()).decode()}"""}


def timer(f):
    @wraps(f)
    def wrapper_time(*args, **kwargs):
        start_script_time = time.time()  # Начало отсчета времени скрипта
        f(*args, **kwargs)
        module_logger.info(f"Общее время выполнения скрипта: {time.time() - start_script_time:.2f} сек.")

    return wrapper_time


class AmazonParseLinks:
    def __init__(self, department: str = "fashion-womens-clothing", min_price: float = 1, step: float = 0.2,
                 max_price: float = 5000, rubric: str = "bbn=7141123011&rh=n%3A2368365011%2Cp_36%3A",
                 file_path_save: str = None):
        self.file_path_save = file_path_save
        self.department = department
        self.min_price = min_price
        self.step = step
        self.max_price = max_price
        self.rubric = rubric
        self.proxy_list = self.get_proxy_list()
        with open(os.path.join(os.getcwd(), f"config/material_json.json")) as f:
            self.materials = json.load(f)
        self.missed_data = pd.DataFrame(columns=['department', 'link'])
        self.main_data = pd.DataFrame(
            columns=['department', 'min_price', 'max_price', 'page_count', 'prod_count', 'link',
                     'material'])

    def get_proxy_list(self):
        # Создание списка прокси, возможно не понадобиться если использовать ротационный ip
        proxy_list = []
        with open("proxy_http_20.txt", "r") as file:
            for line in file.readlines():
                ip, port = line.strip().split(':')
                auth = base64.b64encode(f"{proxy_username}:{proxy_password}".encode()).decode()
                proxy_url = f"http://{auth}@{ip}:{port}"
                proxy_list.append(proxy_url)
        return proxy_list

    def get_proxy(self):
        proxy = self.proxy_list[random.randint(1, len(self.proxy_list) - 1)]
        return proxy

    # Функция для асинхронного запроса к Amazon с прокси
    async def fetch_page_count(self, session, url, pages: bool = True, try_get: int = 5):
        with open(os.path.join(os.getcwd(), f"config/headers_config.json")) as f:
            headers = json.load(f) | proxy_auth | {"user-agent": UserAgent().random}
        proxy_url = self.get_proxy()

        if not pages:
            url = url.split('&page')[0]
        try:
            while try_get != 0:
                async with session.get(url, proxy=proxy_url, headers=headers) as response:
                    if response.status == 200:
                        text = await response.text()
                        bs = soup(text, 'html.parser')

                        last_page_span = bs.find('span', {'class': 's-pagination-item s-pagination-disabled'})
                        if last_page_span:
                            page_count = int(last_page_span.get_text())
                            return page_count, bs
                        else:
                            url = url.split('&page')[0]
                            proxy_url = self.get_proxy()
                            try_get -= 1
                    elif response.status == 503:
                        url = url.split('&page')[0]
                        proxy_url = self.get_proxy()
                        try_get -= 1
                    else:
                        module_logger.warning(f"Возникла ошибка при запросе запросе: {response.status}|{response.json}")
                        proxy_url = self.get_proxy()
                        try_get -= 1

            module_logger.warning(f"Данные не были найдены при запросе по урлу: {url}")
            return None, url
        except Exception as e:
            module_logger.info(f"Ошибка при запросе: {e}")
            return None, url

    async def add_data_to_df(self, min_price: float = 0, max_price: float = 0, page_count: int = 0, prod_count: int = 0,
                             link: str = None, material: bool = False, missed_data: bool = False):
        if missed_data:
            self.missed_data.loc[self.missed_data.shape[0]] = [self.department, link]
        if link in self.main_data.loc[:, 'link'] or (link + '&page=3') in self.main_data.loc[:, 'link']:
            self.main_data.loc[self.main_data.loc['link'] == link, ['page_count', 'prod_count', 'link', 'material']] = [
                page_count, prod_count, link, material]
        else:
            self.main_data.loc[self.main_data.shape[0]] = [
                self.department, min_price, max_price, page_count, prod_count, link, material]

    def create_urls(self):
        urls_list = []
        price, subtraction, page = 0, 0.01, 3

        while price <= self.max_price:
            price = round(self.min_price + self.step, 2)
            if str(price - subtraction)[-3:] == '.99' and 5 <= price < 40:
                def_price = price - subtraction
                url1 = f"https://www.amazon.com/s?i={self.department}&{self.rubric}{int(def_price * 100)}-{int(def_price * 100)}&s=price-asc-rank&fs=true&page={page}"
                url2 = f"https://www.amazon.com/s?i={self.department}&{self.rubric}{int(self.min_price * 100)}-{int((price - subtraction * 2) * 100)}&s=price-asc-rank&fs=true&page={page}"
                urls_list.append(url1)
                self.main_data.loc[self.main_data.shape[0]] = [self.department, def_price, def_price, 0, 0, url1, None]
                urls_list.append(url2)
                self.main_data.loc[self.main_data.shape[0]] = [self.department, self.min_price, price - subtraction * 2,
                                                               0, 0, url2, None]
                self.min_price = price
                continue
            elif 1 <= price // 100 < 10:
                self.step = 100
                url = f"https://www.amazon.com/s?i={self.department}&{self.rubric}{int(self.min_price * 100)}-{int((price - subtraction) * 100)}&s=price-asc-rank&fs=true"
            elif 10 <= price // 100 <= 100:
                self.step = 1000
                url = f"https://www.amazon.com/s?i={self.department}&{self.rubric}{int(self.min_price * 100)}-{int((price - subtraction) * 100)}&s=price-asc-rank&fs=true"
            else:
                url = f"https://www.amazon.com/s?i={self.department}&{self.rubric}{int(self.min_price * 100)}-{int((price - subtraction) * 100)}&s=price-asc-rank&fs=true&page={page}"
            urls_list.append(url)
            self.main_data.loc[self.main_data.shape[0]] = [self.department, self.min_price, price - subtraction, 0, 0,
                                                           url, None]
            self.min_price = price

        module_logger.info(len(urls_list))
        return urls_list

    async def process_links(self, max_concurrent_requests=1, missed_data=False):
        # Собираем все URL для запроса
        if missed_data:
            urls = pd.read_excel(f'missed_links_data_{self.department}.xlsx').loc[:, 'link'].to_list()
        else:
            urls = self.create_urls()
        try:
            # Создание списка прокси, возможно не понадобиться если использовать ротационный ip
            async with aiohttp.ClientSession() as session:
                semaphore = asyncio.Semaphore(max_concurrent_requests)
                tasks = []

                async def fetch_with_semaphore(url):
                    async with semaphore:
                        page_count, soup = await self.fetch_page_count(session, url=url)
                        if page_count is not None:
                            if page_count < 400:
                                min_price = float(
                                    soup.find('label', {'class': 'sf-lower-bound-label'}).text.strip().replace(',',
                                                                                                    '.').replace('$', ''))
                                max_price = float(
                                    soup.find('label', {'class': 'sf-upper-bound-label'}).text.strip().replace(',',
                                                                                                    '.').replace('$', ''))
                                prod_count = 48 * page_count
                                await self.add_data_to_df(min_price=min_price, max_price=max_price,
                                                          page_count=page_count, prod_count=prod_count,
                                                          link=url.split('&page')[0])
                            elif page_count == 400:
                                for key, value in self.materials.items():
                                    new_url = url.replace("&s=", f"%2Cp_n_material_browse%3A{value}&s=")
                                    page_count, soup = await self.fetch_page_count(session, new_url)
                                    if page_count is not None:
                                        min_price = float(
                                            soup.find('label', {'class': 'sf-lower-bound-label'}).text.strip().replace(',',
                                                                                                   '.').replace('$', ''))
                                        max_price = float(
                                            soup.find('label', {'class': 'sf-upper-bound-label'}).text.strip().replace(',',
                                                                                                   '.').replace('$', ''))
                                        prod_count = 48 * page_count
                                        await self.add_data_to_df(min_price=min_price, max_price=max_price,
                                                                  page_count=page_count, prod_count=prod_count,
                                                                  link=url.split('&page')[0], material=key)
                        else:
                            await self.add_data_to_df(link=url)

                for url in urls:
                    task = asyncio.create_task(fetch_with_semaphore(url))
                    tasks.append(task)

                await asyncio.gather(*tasks)
        except Exception as exc:
            module_logger.error(str(exc))
        finally:
            self.main_data = self.main_data.drop((self.main_data.loc[(self.main_data.prod_count == 0)].index))
            # Сохранение изменений в файл Excel
            self.main_data.to_excel(self.file_path_save, index=False)
            self.missed_data.to_excel(f"excel_parsed_files/missed_links_data_{self.department}.xlsx", index=False)


if __name__ == '__main__':
    # Запуск асинхронной функции
    start_script_time = time.time()  # Начало отсчета времени скрипта

    amaz_parse = AmazonParseLinks(file_path_save='excel_parsed_files/test_amazon.xlsx')
    asyncio.run(amaz_parse.process_links())

    total_script_duration = time.time() - start_script_time
    print(f"Общее время выполнения скрипта: {total_script_duration:.2f} сек.")
