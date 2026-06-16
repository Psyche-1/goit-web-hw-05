import asyncio
import logging
import sys
from datetime import datetime, timedelta
import aiohttp

# Налаштування логування для відстеження помилок мережі
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class DateProvider:
    """Відповідає за генерацію списку дат."""
    @staticmethod
    def get_past_dates(days_count: int) -> list[str]:
        if not 1 <= days_count <= 10:
            raise ValueError("Кількість днів повинна бути від 1 до 10.")
        
        dates = []
        current_date = datetime.now()
        for i in range(days_count):
            past_date = current_date - timedelta(days=i)
            dates.append(past_date.strftime("%d.%m.%Y"))
        return dates


class PrivatBankAPIClient:
    """Відповідає за мережеву взаємодію з АПІ."""
    BASE_URL = "https://api.privatbank.ua/p24api/exchange_rates?json&date="

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def fetch_rates_for_date(self, date: str) -> dict | None:
        url = f"{self.BASE_URL}{date}"
        try:
            async with self.session.get(url, timeout=10) as response:
                if response.status != 200:
                    logging.error(f"Помилка сервера для дати {date}: статус {response.status}")
                    return None
                return await response.json()
        except aiohttp.ClientError as e:
            logging.error(f"Мережева помилка при запиті за дату {date}: {e}")
            return None
        except asyncio.TimeoutError:
            logging.error(f"Таймаут запиту для дати {date}")
            return None


class CurrencyDataParser:
    """Відповідає за фільтрацію та парсинг сирих даних від АПІ."""
    TARGET_CURRENCIES = {"USD", "EUR"}

    @classmethod
    def parse_response(cls, data: dict | None, date: str) -> dict:
        result = {date: {}}
        if not data or "exchangeRate" not in data:
            return result

        for rate in data["exchangeRate"]:
            currency = rate.get("currency")
            if currency in cls.TARGET_CURRENCIES:
                # Використовуємо готівковий курс (saleRate/purchaseRate). 
                # Якщо його немає, беремо безготівковий (saleRateNB/purchaseRateNB)
                sale = rate.get("saleRate", rate.get("saleRateNB"))
                purchase = rate.get("purchaseRate", rate.get("purchaseRateNB"))
                
                result[date][currency] = {
                    "sale": float(sale) if sale else None,
                    "purchase": float(purchase) if purchase else None
                }
        return result


class CurrencyFetcherService:
    """Оркестратор, який об'єднує клієнт та парсер для збору даних."""
    def __init__(self, api_client: PrivatBankAPIClient):
        self.api_client = api_client

    async def get_rates_for_period(self, dates: list[str]) -> list[dict]:
        tasks = [self.api_client.fetch_rates_for_date(date) for date in dates]
        # Запускаємо всі запити паралельно
        raw_results = await asyncio.gather(*tasks)
        
        formatted_results = []
        for date, raw_data in zip(dates, raw_results):
            parsed = CurrencyDataParser.parse_response(raw_data, date)
            formatted_results.append(parsed)
            
        return formatted_results


async def main():
    # 1. Валідація аргументів командного рядка
    if len(sys.argv) < 2:
        print("Помилка: Передайте кількість днів. Наприклад: py main.py 2")
        return

    try:
        days_count = int(sys.argv[1])
        dates = DateProvider.get_past_dates(days_count)
    except ValueError as e:
        print(f"Помилка аргументів: {e}")
        return

    # 2. Ініціалізація асинхронної сесії та збір даних
    async with aiohttp.ClientSession() as session:
        api_client = PrivatBankAPIClient(session)
        service = CurrencyFetcherService(api_client)
        
        import pprint
        results = await service.get_rates_for_period(dates)
        
        # 3. Вивід результату в консоль у красивому форматі
        pprint.pprint(results, sort_dicts=False)


if __name__ == "__main__":
    # Налаштування для коректної роботи asyncio на Windows
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(main())
