from scraper import TorgiScraper
from excel.xlsx_io import get_vin_list

import asyncio


INPUT_FILE = 'excel/input/vin_list.xlsx'


async def main():
	torgi_scraper = TorgiScraper()
	vin_list = get_vin_list(input_excel_file=INPUT_FILE)

	check_tasks = [torgi_scraper.check_vehicle(vin=vin) for vin in vin_list]
	for check_task in asyncio.as_completed(check_tasks):
		await check_task


if __name__ == '__main__':
	import time

	start_time = time.time()
	print('--------------------------- START ---------------------------')

	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())

	print('--------------------------- FINISH ---------------------------')
	print(f'Time: {time.time() - start_time}')
