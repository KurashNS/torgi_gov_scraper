import ua_generator
from aiohttp_socks import ProxyConnector, ProxyType, ProxyError, ProxyConnectionError, ProxyTimeoutError
from aiohttp import ClientSession, ClientError, TCPConnector

from tenacity import retry, retry_if_exception_type, wait_random, stop_after_attempt

from bs4 import BeautifulSoup

from excel.xlsx_io import output_check_result

from datetime import datetime

from logging import Logger, INFO, ERROR

import asyncio


class TorgiScraper:
	def __init__(self, logger: Logger):
		self._url = 'https://torgi.gov.ru/new/api/public/lotcards/rss'

		self._logger = logger

		self._semaphore = asyncio.Semaphore(value=50)

		self._output_file = f'excel/output/check_results_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.xlsx'

	@property
	def _headers(self) -> dict[str: str]:
		ua = ua_generator.generate(device='desktop')
		return {
			'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
			'Accept-Language': 'ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7',
			'Connection': 'keep-alive',
			'Referer': 'https://torgi.gov.ru/new/public/lots/reg',
			'Sec-Fetch-Dest': 'document',
			'Sec-Fetch-Mode': 'navigate',
			'Sec-Fetch-Site': 'same-origin',
			'Sec-Fetch-User': '?1',
			'Upgrade-Insecure-Requests': '1',
			'User-Agent': ua.text,
			'sec-ch-ua': ua.ch.brands,
			'sec-ch-ua-mobile': ua.ch.mobile,
			'sec-ch-ua-platform': ua.ch.platform,
		}

	@property
	def _check_result_template(self) -> dict[str: str]:
		return {
			'VIN': '',
			'Статус проверки': 'Успешно',
			'Номер лота': '',
			'Наименование лота': '',
			'Вид торгов': '',
			'Форма проведения торгов': '',
			'Статус лота': '',
			'Электронная площадка': '',
			'Дата публикации': '',
			'Дата изменения': '',
			'Начальная цена': '',
			'Номер извещения': '',
			'Категория имущества': '',
			'Марка': '',
			'Модель': '',
			'Год выпуска': '',
			'Государственный регистрационный знак': '',
			'Дата государственного регистрационного знака': '',
			'Пробег': '',
			'Вид транспорта': '',
			'Объем двигателя': '',
			'Мощность двигателя': '',
			'Коробка передач': '',
			'Привод': '',
			'Экологический класс': ''
		}

	@retry(retry=retry_if_exception_type((ProxyError, ProxyConnectionError, ProxyTimeoutError, ClientError)),
	       sleep=asyncio.sleep, wait=wait_random(min=0, max=1), stop=stop_after_attempt(10), reraise=True)
	async def _make_check_request(self, vin: str) -> str:
		search_params = {
			'biddEndFrom': '',
			'biddEndTo': '',
			'pubFrom': '',
			'pubTo': '',
			'aucStartFrom': '',
			'aucStartTo': '',
			'text': vin,
			'amoOrgCode': '',
			'npa': '',
			'byFirstVersion': 'true',
		}
	        async with ProxyConnector(proxy_type=ProxyType.HTTP, host='185.82.126.71', port='13518',
	                                  username='yfy5n4', password='s4SsUv') as proxy_conn:
			async with ClientSession(connector=proxy_conn, headers=self._headers, raise_for_status=True) as session:
				async with session.get(url=self._url, params=search_params) as check_response:
					return await check_response.text()

	@staticmethod
	def _process_item_description(item_description: BeautifulSoup) -> dict[str: str]:
		item_description_dict = {}
		columns_flag = False
		for bold_element in item_description.find_all(name='b'):
			element_text = bold_element.get_text(strip=True)
			if columns_flag and 'Характеристики' not in element_text:
				key = element_text[:-1]
				item_description_dict[key] = ''

			if 'Список лотов' in element_text:
				columns_flag = True

		item_desc_strings = list(item_description.stripped_strings)
		for key in item_description_dict:
			for idx, string in enumerate(item_desc_strings):
				if key in string:
					key_value = item_desc_strings[idx + 1]
					item_description_dict[key] = key_value if key_value[:-1] not in item_description_dict else ''
					break

		characteristics_flag = False
		for string in item_desc_strings:
			if characteristics_flag:
				key, value = string.split(':', 1)
				item_description_dict[key.strip()] = value.strip()
			if 'Характеристики' in string:
				characteristics_flag = True

		for key in item_description_dict:
			if 'Дата' in key:
				if item_description_dict[key] and isinstance(item_description_dict[key], str):
					try:
						datetime_obj = datetime.strptime(item_description_dict[key], '%Y-%m-%dT%H:%M:%S.%fZ')
						item_description_dict[key] = datetime_obj.strftime('%d.%m.%Y %H:%M:%S')
					except ValueError:
						pass

		return item_description_dict

	def _handle_no_vehicle_found(self, vin: str, status: str, log_msg: str, log_level: int):
		self._logger.log(level=log_level, msg=log_msg)
		check_results = self._check_result_template.copy()
		check_results['VIN'] = vin
		check_results['Статус проверки'] = status
		return check_results

	def _process_check_response(self, vin: str, check_response: str) -> dict[str: str]:
		check_response_xml = BeautifulSoup(markup=check_response, features='lxml-xml')
		item = check_response_xml.find(name='item')
		if not item:
			return self._handle_no_vehicle_found(
				vin=vin,
				status='Нет данных',
				log_msg=f'VIN: {vin} | Vehicle was not found at the auction',
				log_level=INFO
			)

		item_desc_element = item.find(name='description')
		if not item_desc_element:
			raise ValueError('No description for item')

		item_desc = item_desc_element.get_text()
		item_desc_html = BeautifulSoup(markup=item_desc, features='html.parser')

		item_desc_dict = self._process_item_description(item_description=item_desc_html)
		check_results = self._check_result_template.copy()
		for results_dict_key in check_results:
			for item_desc_dict_key in item_desc_dict:
				if results_dict_key in item_desc_dict_key and item_desc_dict[item_desc_dict_key]:
					check_results[results_dict_key] = item_desc_dict[item_desc_dict_key]

		if vin != check_results.get('VIN'):
			return self._handle_no_vehicle_found(
				vin=vin,
				status='Нет данных',
				log_msg=f'VIN: {vin} | Vehicle was not found at the auction',
				log_level=INFO
			)
		else:
			self._logger.info(f'VIN: {vin} | Item description: {item_desc}')
			self._logger.info(f'VIN: {vin} | Check result: {check_results}')
			print(f'VIN: {vin} | Check result: {check_results}')

		return check_results

	async def check_vehicle(self, vin: str):
		async with self._semaphore:
			try:
				check_response = await self._make_check_request(vin=vin)
				check_results = await asyncio.to_thread(
					self._process_check_response,
					vin=vin,
					check_response=check_response
				)
			except Exception as e:
				check_results = self._handle_no_vehicle_found(
					vin=vin,
					status='Ошибка',
					log_msg=f'VIN: {vin} | Error: {type(e)} - {e}',
					log_level=ERROR
				)
			finally:
				await asyncio.to_thread(output_check_result, output_file=self._output_file, check_result=check_results)


if __name__ == '__main__':
	from log import TorgiLogger


	async def main():
		logger = TorgiLogger()
		torgi_scraper = TorgiScraper(logger=logger)

		vin_list = [
			'XTAKS045LK1178313',
			'X9W64408MJ0002729',
			'Z8PFF3A5XBA014831',
			'Z8NBAABD0L0108892',
			'Z0X219079M0767640',
			'KNAKU811DA5005300',
			'XWEGU411BL0021018',
			'Z8NBAABD0K0083816'
		]
		check_tasks = [torgi_scraper.check_vehicle(vin=vin) for vin in vin_list]
		for check_task in asyncio.as_completed(check_tasks):
			await check_task


	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
