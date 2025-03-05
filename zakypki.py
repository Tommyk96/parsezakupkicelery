from __future__ import absolute_import, unicode_literals
import asyncio
import json
import logging
import os

import aiofiles
import aiohttp
import xmltodict
from bs4 import BeautifulSoup
from celery import Celery, group
from typing import Optional, Set, Dict, List

# Настройка Celery
app = Celery('zakypki', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')
app.conf.broker_connection_retry_on_startup = True

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Константы
BASE_URL = "https://zakupki.gov.ru"
SEARCH_URL = "https://zakupki.gov.ru/epz/order/extendedsearch/results.html?fz44=on&pageNumber={}"
PRINT_FORM_URL = "https://zakupki.gov.ru/epz/order/notice/printForm/viewXml.html?regNumber={}"

OUTPUT_FILE = "tenders.json"
PROCESSED_FILE = "processed_tenders.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Referer": BASE_URL,
    "Connection": "keep-alive",
}


async def fetch(url: str) -> Optional[str]:
    """Асинхронная функция для запроса по URL"""
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        try:
            async with session.get(url, timeout=10) as response:
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientError as exc:
            logger.error(f"Ошибка запроса {url}: {exc}")
            return None


def run_async_task(async_func, *args):
    """Запускает асинхронную функцию в синхронном коде"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(async_func(*args))
    loop.close()
    return result


async def read_json_file(file_path: str) -> list:
    """Читает данные из JSON-файла."""
    if os.path.exists(file_path):
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            try:
                return json.loads(await f.read())
            except json.JSONDecodeError:
                return []
    return []


async def write_json_file(file_path: str, data: list) -> None:
    """Записывает данные в JSON-файл."""
    async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(data, ensure_ascii=False, indent=4))


@app.task(bind=True, max_retries=3)
def fetch_tender_links(self, page_number: int) -> List[str]:
    """Извлекает ссылки на тендеры с указанной страницы"""
    url = SEARCH_URL.format(page_number)
    logger.info(f"Получение списка тендеров со страницы {page_number}")
    
    page_content = run_async_task(fetch, url)
    if not page_content:
        raise self.retry(exc=Exception("Ошибка загрузки страницы"), countdown=5)
    
    soup = BeautifulSoup(page_content, 'html.parser')
    tenders = {link["href"].split("=")[-1] for link in soup.find_all("a", href=True) if "regNumber=" in link["href"]}
    
    logger.info(f"Найдено {len(tenders)} тендеров на странице {page_number}")
    return list(tenders)


@app.task(bind=True, max_retries=3)
def fetch_tender_data(self, reg_number: str) -> Optional[Dict]:
    """Получает и парсит XML-данные тендера"""
    url = PRINT_FORM_URL.format(reg_number)
    logger.info(f"Загрузка XML для тендера {reg_number}")
    
    xml_content = run_async_task(fetch, url)
    if not xml_content:
        raise self.retry(exc=Exception("Ошибка загрузки XML"), countdown=5)
    
    try:
        data = xmltodict.parse(xml_content)
        notice = data.get("export", {}).get("contractNotice", {})
        
        tender_info = {
            "reg_number": reg_number,
            "publish_date": notice.get("publishDTInEIS", "Не указана"),
            "initial_price": notice.get("initialPrice", 0),
            "customer": notice.get("customer", {}).get("fullName", "Не указан"),
            "contract_type": notice.get("contractType", "Не указано"),
            "procurement_object": notice.get("procurementObject", "Не указано"),
            "deadline": notice.get("deadline", "Не указано"),
            "print_form_url": url
        }

        if tender_info["customer"] == "Не указан":
            logger.warning(f"Не найден заказчик для тендера {reg_number}")
        
        run_async_task(save_to_json, tender_info)
        processed_tenders = run_async_task(load_processed_tenders)
        processed_tenders.add(reg_number)
        run_async_task(save_processed_tenders, processed_tenders)
        
        logger.info(f"Тендер {reg_number} обработан: {tender_info}")
        return tender_info
    except (xmltodict.ParsingInterrupted, KeyError, TypeError) as e:
        logger.error(f"Ошибка парсинга XML {url}: {e}")
        return None


async def load_processed_tenders() -> Set[str]:
    """Загружает список обработанных тендеров"""
    processed_tenders = await read_json_file(PROCESSED_FILE)
    return set(processed_tenders)


async def save_processed_tenders(processed_tenders: Set[str]) -> None:
    """Сохраняет обработанные тендеры"""
    await write_json_file(PROCESSED_FILE, list(processed_tenders))


async def save_to_json(tender: Dict) -> None:
    """Сохраняет тендер в JSON-файл"""
    tenders = await read_json_file(OUTPUT_FILE)
    tenders.append(tender)
    await write_json_file(OUTPUT_FILE, tenders)


def main():
    """Основной процесс парсинга тендеров"""
    logger.info("Запуск парсинга тендеров...")
    processed_tenders = run_async_task(load_processed_tenders)

    fetch_links_result = group(fetch_tender_links.s(page) for page in range(1, 3)).apply_async()
    logger.info("Ожидаем завершения сбора ссылок...")
    
    tender_lists = fetch_links_result.get()
    if not tender_lists:
        logger.warning("Не получено ни одной ссылки на тендеры!")
        return

    tenders = {reg for tenders_page in tender_lists if tenders_page for reg in tenders_page}
    tenders -= processed_tenders  

    logger.info(f"Найдено {len(tenders)} новых тендеров: {tenders}")
    if not tenders:
        logger.info("Нет новых тендеров для обработки.")
        return

    fetch_data_tasks_result = group(fetch_tender_data.s(reg) for reg in tenders).apply_async()
    logger.info("Ожидаем завершения парсинга XML...")
    fetch_data_tasks_result.get()
    logger.info("Парсинг завершен!")


if __name__ == "__main__":
    main()
