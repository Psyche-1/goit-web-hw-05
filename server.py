import asyncio
import logging
import sys
from datetime import datetime, timedelta
import aiohttp
import websockets
import names
from websockets.asyncio.server import ServerConnection
from websockets.exceptions import ConnectionClosedOK
from aiofile import async_open
from aiopath import AsyncPath

logging.basicConfig(level=logging.INFO)

# ==================== БЛОК ЛОГІКИ ПРИВАТБАНКУ ====================

class DateProvider:
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
    BASE_URL = "https://api.privatbank.ua/p24api/exchange_rates?json&date="

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def fetch_rates_for_date(self, date: str) -> dict | None:
        url = f"{self.BASE_URL}{date}"
        try:
            async with self.session.get(url, timeout=10) as response:
                if response.status != 200:
                    return None
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return None


class CurrencyDataParser:
    @classmethod
    def parse_response(cls, data: dict | None, date: str, currencies: set[str]) -> dict:
        result = {date: {}}
        if not data or "exchangeRate" not in data:
            return result

        for rate in data["exchangeRate"]:
            currency = rate.get("currency")
            if currency in currencies:
                sale = rate.get("saleRate", rate.get("saleRateNB"))
                purchase = rate.get("purchaseRate", rate.get("purchaseRateNB"))
                
                result[date][currency] = {
                    "sale": float(sale) if sale else None,
                    "purchase": float(purchase) if purchase else None
                }
        return result


class CurrencyFetcherService:
    def __init__(self, api_client: PrivatBankAPIClient):
        self.api_client = api_client

    async def get_rates_for_period(self, dates: list[str], currencies: set[str]) -> list[dict]:
        tasks = [self.api_client.fetch_rates_for_date(date) for date in dates]
        raw_results = await asyncio.gather(*tasks)
        
        formatted_results = []
        for date, raw_data in zip(dates, raw_results):
            parsed = CurrencyDataParser.parse_response(raw_data, date, currencies)
            formatted_results.append(parsed)
            
        return formatted_results


def format_chat_output(data_list: list[dict]) -> str:
    lines = ["--- Курс валют від ПриватБанку ---"]
    for item in data_list:
        for date, currencies in item.items():
            lines.append(f"Дата: {date}")
            if not currencies:
                lines.append("  Немає даних за цей день.")
                continue
            for curr, rates in currencies.items():
                sale = rates['sale'] if rates['sale'] else '-'
                purchase = rates['purchase'] if rates['purchase'] else '-'
                lines.append(f"  {curr} -> Продаж: {sale} | Купівля: {purchase}")
    return "\n".join(lines)


# ==================== БЛОК ЛОГУВАННЯ ФАЙЛІВ ====================

async def log_exchange_command(user_name: str, message_text: str):
    log_dir = AsyncPath("logs")
    await log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / "exchange_commands.log"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] User '{user_name}' executed: '{message_text}'\n"
    
    async with async_open(log_file, "a", encoding="utf-8") as afp:
        await afp.write(log_entry)


# ==================== БЛОК СЕРВЕРУ ВЕБ-СОКЕТІВ ====================

class Server:
    clients = set()

    async def register(self, ws: ServerConnection):
        ws.name = names.get_full_name()
        self.clients.add(ws)
        logging.info(f'{ws.remote_address} connects')

    async def unregister(self, ws: ServerConnection):
        self.clients.remove(ws)
        logging.info(f'{ws.remote_address} disconnects')

    async def send_to_clients(self, message: str):
        if self.clients:
            for client in self.clients:
                await client.send(message)

    async def ws_handler(self, ws: ServerConnection):
        await self.register(ws)
        try:
            await self.distrubute(ws)
        except ConnectionClosedOK:
            pass
        finally:
            await self.unregister(ws)

    async def distrubute(self, ws: ServerConnection):
        async for message in ws:
            cleaned_message = message.strip()
            
            if cleaned_message.startswith("exchange"):
                await self.handle_exchange_command(ws, cleaned_message)
            else:
                await self.send_to_clients(f"{ws.name}: {message}")

    async def handle_exchange_command(self, ws: ServerConnection, message: str):
        await log_exchange_command(ws.name, message)
        
        parts = message.split()
        days = 1
        
        # Правильна перевірка елемента масиву за індексом [1]
        if len(parts) > 1 and parts[1].isdigit():
            days = int(parts[1])
            
        try:
            dates = DateProvider.get_past_dates(days)
        except ValueError as e:
            await ws.send(f"Помилка: {str(e)}")
            return

        target_currencies = {"USD", "EUR"}
        if len(sys.argv) > 1:
            for extra_curr in sys.argv[1:]:
                target_currencies.add(extra_curr.upper())

        await ws.send("Отримую дані з ПриватБанку, будь ласка, зачекайте...")
        
        async with aiohttp.ClientSession() as session:
            api_client = PrivatBankAPIClient(session)
            service = CurrencyFetcherService(api_client)
            
            rates = await service.get_rates_for_period(dates, target_currencies)
            response_text = format_chat_output(rates)
            
            await ws.send(response_text)


async def main():
    server = Server()
    async with websockets.serve(server.ws_handler, 'localhost', 5000):
        await asyncio.Future()

if __name__ == '__main__':
    asyncio.run(main())
