import json
import os
import random
import time
import base64
from functools import wraps
import os
import logging

from bs4 import BeautifulSoup as soup
import pandas as pd
import openpyxl
import requests
import asyncio
import aiohttp
from fake_useragent import UserAgent
import coloredlogs

from wutil import get_logger
from soup_parser import SoupContentParser

coloredlogs.install(level='INFO')
module_logger = get_logger("parse_information")

# Учетные данные для прокси
with open(os.path.join(os.getcwd(), f"config/proxy_settings.json")) as f:
    proxy_settings = json.load(f)
proxy_username, proxy_password = proxy_settings['proxy_username'], proxy_settings['proxy_password']
proxy_auth = {
    f"Proxy-Authorization": f"""Basic {base64.b64encode(f"{proxy_username}:{proxy_password}".encode()).decode()}"""}


class AmazonParserInfo:
    def __init__(self, file_path: str = None, json_file_path: str = None, goods: bool = False):
        if '.xlsx' in file_path:
            self.main_data = pd.read_excel(file_path)
            self.urls_pages = self.main_data.loc[:1, ['page_count', 'link']].drop_duplicates(subset=['link'], keep='first')
        elif '.json' in file_path:
            with open(os.path.join(os.getcwd(), file_path)) as file:
                self.main_data = json.load(file)
            self.urls_pages = self.main_data

        self.goods = goods
        self.soup_parser = SoupContentParser()
        self.json_file_path = json_file_path
        self.products_json = []
        self.proxy_list = self.get_proxy_list()
        with open(os.path.join(os.getcwd(), f"config/headers_config.json")) as f:
            self.headers = json.load(f) | proxy_auth | {"user-agent": UserAgent().random}

    def get_proxy_list(self, file_path_to_proxy='proxy_http_20.txt'):
        # Создание списка прокси, возможно не понадобиться если использовать ротационный ip
        proxy_list = []
        with open(file_path_to_proxy, "r") as file:
            for line in file.readlines():
                ip, port = line.strip().split(':')
                auth = base64.b64encode(f"{proxy_username}:{proxy_password}".encode()).decode()
                proxy_url = f"http://{auth}@{ip}:{port}"
                proxy_list.append(proxy_url)
        return proxy_list

    async def append_product_to_json(self, title, price, rating, reviews, sales_volume, link, asin, image_link,
                                     color_variants_count):
        product_data = {
            'title': title,
            'price': price,
            'rating': rating,
            'reviews': reviews,
            'sales_volume': sales_volume,
            'link': f'https://www.amazon.com{link}' if link else None,
            'asin': asin,
            'image_link': image_link,
            'color_variants_count': color_variants_count
        }

        self.products_json.append(product_data)

    async def append_good_to_json(self, title, price, rating, brand, reviews, sales_volume, link, asin, image_link,
                                  color_variants_count, details):
        product_data = {
            'title': title,
            'price': price,
            'rating': rating,
            'brand': brand,
            'reviews': reviews,
            'sales_volume': sales_volume,
            'link': f'https://www.amazon.com{link}' if link else None,
            'asin': asin,
            'image_link': image_link,
            'color_variants_count': color_variants_count,
            'details': details
        }

        self.products_json.append(product_data)

    async def fetching_pages(self, session, url: str = None, pages: int = 1):
        pages_done = 0
        proxy_url = self.get_proxy()
        time_start_count = time.time()
        for page in range(1, pages + 1):
            try:
                next_page_url = url.split("&page=")[0] + f"&page={page}"
                # module_logger.info(f"Парсинг страницы номер {page} при запросе на страницу: {next_page_url}")
                async with session.get(next_page_url, proxy=proxy_url, headers=self.headers) as response:
                    if response.status == 200:
                        text = await response.text()
                        bs = soup(text, 'html.parser')
                        if 'Sorry! Something went wrong!' in bs.text:
                            module_logger.warning(f"Something going wrong with request to {next_page_url}, "
                                                  f"changing proxy...")
                            proxy_url = self.get_proxy()
                            continue

                        product_blocks = bs.find_all('div', {'data-component-type': 's-search-result'})
                        for product in product_blocks:
                            title = self.soup_parser.get_title(product)
                            price = self.soup_parser.get_price(product)
                            rating = self.soup_parser.get_rating(product)
                            reviews = self.soup_parser.get_reviews(product)
                            sales_volume = self.soup_parser.get_sales_volume(product)
                            link = self.soup_parser.get_website(product)
                            asin = self.soup_parser.get_asin(product)
                            image_link = self.soup_parser.get_image_link(product)
                            color_variants_count = self.soup_parser.get_color(product)
                            await self.append_product_to_json(title=title, price=price, rating=rating, reviews=reviews,
                                                              sales_volume=sales_volume, link=link, asin=asin,
                                                              image_link=image_link,
                                                              color_variants_count=color_variants_count)
                        pages_done += 1
                    else:
                        module_logger.warning(f"Данные не были найдены при запросе на страницу: {next_page_url}, "
                                              f"response text and code: {response.status}:{response.text}")
                        proxy_url = self.get_proxy()
                        raise "EXCEPTION"
            except Exception as e:
                module_logger.warning(f"Ошибка {e}, при запросе на страницу: {url}")
                proxy_url = self.get_proxy()
                break
        # module_logger.info(f"Парсинг {pages_done} страниц за: {(time.time() - time_start_count):.2f} sec")
        return pages_done

    async def fetching_goods(self, session, url: str = None, item: dict = None):
        pages_done = 0
        proxy_url = self.get_proxy()
        try_count = 5
        while try_count != 0:
            try:
                async with session.get(url, proxy=proxy_url, headers=self.headers) as response:
                    if response.status == 200:
                        text = await response.text()
                        bs = soup(text, 'html.parser')
                        if 'Sorry! Something went wrong!' in bs.text:
                            module_logger.warning(f"Something going wrong with request to {next_page_url}, "
                                                  f"changing proxy...")
                            proxy_url = self.get_proxy()
                            try_count -= 1
                            continue

                        product = bs.find('div', {'cel_widget_id': 'dpx-ppd_csm_instrumentation_wrapper'})
                        if product is None:
                            product = bs.find('div', {'id': 'dp'})
                        if product is None:
                            product = bs

                        title = self.soup_parser.get_title(product)
                        if not title:
                            title = item['title']

                        price = self.soup_parser.get_price(product)
                        if not price:
                            price = item['price']

                        rating = self.soup_parser.get_rating(product)
                        if not rating:
                            rating = item['rating']

                        sales_volume = self.soup_parser.get_sales_volume_of_good(product)
                        if not sales_volume:
                            sales_volume = item['sales_volume']

                        reviews = self.soup_parser.amount_reviws(product)
                        if not reviews:
                            reviews = item['reviews']

                        asin = self.soup_parser.get_asin(product)
                        if not asin:
                            asin = item['asin']

                        image_link = self.soup_parser.get_image_link_good(product)
                        if not image_link:
                            image_link = item['image_link']

                        color_variants_count = self.soup_parser.get_colors_goods(product)
                        if not color_variants_count:
                            color_variants_count = item['color_variants_count']

                        brand = self.soup_parser.get_brand(product)
                        link = url

                        details = self.soup_parser.get_details(product)
                        module_logger.info(f"Данные были найдены при запросе {url}")
                        await self.append_good_to_json(title=title, price=price, rating=rating, reviews=reviews,
                                                       sales_volume=sales_volume, link=link, asin=asin,
                                                       image_link=image_link,
                                                       color_variants_count=color_variants_count, brand=brand,
                                                       details=details)
                        try_count = 0
                    else:
                        module_logger.warning(f"Данные не были найдены при "  # запросе на страницу: {url}, "
                                              f"response text and code: {response.status}:{response.text}")

                        try_count -= 1
                        proxy_url = self.get_proxy()
            except Exception as e:
                module_logger.warning(f"Ошибка {e}, при запросе на страницу: {url}")
                proxy_url = self.get_proxy()
        return pages_done

    def get_proxy(self):
        try:
            proxy = self.proxy_list[random.randint(1, len(self.proxy_list) - 1)]
            return proxy
        except Exception:
            return None

    async def process_links(self, max_concurrent_requests=10):
        start_script_time = time.time()  # Начало отсчета времени скрипта
        try:
            async with aiohttp.ClientSession() as session:
                semaphore = asyncio.Semaphore(max_concurrent_requests)
                tasks = []

                async def fetch_with_semaphore(url, page_count: int = 0, item: dict = None):
                    pages_ = 0
                    second_script_time = time.time()  # Начало отсчета времени скрипта
                    async with semaphore:
                        if not self.goods:
                            pages_done = await self.fetching_pages(session=session, url=url, pages=page_count)
                            pages_ += pages_done
                            if pages_done:
                                module_logger.info(f"Amount parsed pages {pages_done} out of {page_count} for url {url}")
                            else:
                                module_logger.warning(f"No one of page wasn't parsed from {url}")
                        else:
                            await self.fetching_goods(session, url=url, item=item)
                        module_logger.info(f"Время выполнения {pages_} за: {(time.time() - second_script_time):.2f} сек.")

                if not self.goods and type(self.main_data) != dict:
                    for ind, row in self.urls_pages.iterrows():
                        task = asyncio.create_task(fetch_with_semaphore(url=row['link'], page_count=row['page_count']))
                        tasks.append(task)
                elif type(self.main_data) == dict:
                    for key, value in self.main_data.items():
                        task = asyncio.create_task(fetch_with_semaphore(url=value['url'], page_count=value['page_count']))
                        tasks.append(task)

                else:
                    for item in self.main_data:
                        task = asyncio.create_task(fetch_with_semaphore(url=item['link'], item=item))
                        tasks.append(task)
                await asyncio.gather(*tasks)
        except Exception as exc:
            module_logger.error(exc)
        finally:
            total_script_duration = time.time() - start_script_time
            module_logger.info(f"Общее время выполнения скрипта: {total_script_duration:.2f} сек.")
            num_of_files = len(os.listdir(''.join(self.json_file_path.split('/')[:-1])))
            # Сохранение изменений в файл Excel
            with open(self.json_file_path.split('.json')[0] + '_' + str(num_of_files) + '.json', 'w',
                      encoding='utf-8') as json_file:
                json.dump(self.products_json, json_file, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    # Запуск асинхронной функции
    amaz_parse = AmazonParserInfo(file_path='json_parsed_files/2_count_page_sum.json',
                                  # file_path='Amazon_links_data.xlsx',
                                  json_file_path='json_parsed_files/amazon_products.json')
    asyncio.run(amaz_parse.process_links())
