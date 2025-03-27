import time
from time import perf_counter
import aiohttp
import asyncio
import json
import platform


# У меня была ощибка при запросе, возможно, из-за моего санкционного региона. Гпт подсказал сказал как решить)
# Ошибка: aiodns needs a SelectorEventLoop on Windows. See more: https://github.com/saghul/aiodns/issues/86
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def get_order_book(url: str) -> json:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                if response.status == 200:
                    return await response.json()
                return {"error": f"HTTP status {response.status}"}
    except Exception as error:
        return {"error": str(error)}


def write_to_file(api_responses: list, symbols: list):
    for i, symbol in enumerate(symbols):
        if i >= len(api_responses) or not api_responses[i]:
            print(f"Нет данных для биржи {symbol}")
            continue

        filename = f"{symbol}_data.json"
        try:
            with open(filename, 'w') as file:
                json.dump(api_responses[i], file, indent=4)
            print(f"Данные сохранены в {filename}")
        except Exception as error:
            print(f"Ошибка записи {symbol}: {error}")


class Bybit:
    @staticmethod
    def edit_data():
        try:
            with open('bybit_data.json', 'r') as file:
                data = json.load(file)
                return [{ticker['symbol']: ticker['bid1Price']}
                        for ticker in data.get('result', {}).get('list', [])]
        except Exception as error:
            print(f"Ошибка обработки Bybit: {error}")
            return []


class Binance:
    @staticmethod
    def edit_data():
        try:
            with open('binance_data.json', 'r') as file:
                data = json.load(file)
                return [{ticker['symbol']: ticker['price']}
                        for ticker in data if isinstance(data, list)]
        except Exception as error:
            print(f"Ошибка обработки Binance: {error}")
            return []


class Okx:
    @staticmethod
    def edit_data():
        try:
            with open('okx_data.json', 'r') as file:
                data = json.load(file)
                return [{ticker['instId'].replace("-", ""): ticker['last']}
                        for ticker in data.get('data', [])]
        except Exception as error:
            print(f"Ошибка обработки OKX: {error}")
            return []


def arbitrage(data_1, data_2, data_3, threshold=0.01):
    def to_dict(data):
        return {list(d.keys())[0]: float(list(d.values())[0])
                for d in data if d and isinstance(d, dict)}

    dict1 = to_dict(data_1)
    dict2 = to_dict(data_2)
    dict3 = to_dict(data_3)

    common_pairs = set(dict1.keys()) & set(dict2.keys()) & set(dict3.keys())

    opportunities = []
    for pair in common_pairs:
        prices = {
            'Bybit': dict1[pair],
            'Binance': dict2[pair],
            'OKX': dict3[pair]
        }

        max_p = max(prices.values())
        min_p = min(prices.values())

        if min_p > 0:
            spread_pct = ((max_p - min_p) / min_p) * 100

            if spread_pct > threshold:
                buy_exchange = min(prices, key=prices.get)
                sell_exchange = max(prices, key=prices.get)
                spread_value = max_p - min_p

                opportunities.append({
                    'pair': pair,
                    'buy_exchange': buy_exchange,
                    'buy_price': min_p,
                    'sell_exchange': sell_exchange,
                    'sell_price': max_p,
                    'spread_value': spread_value,
                    'spread_percent': spread_pct
                })

    return sorted(opportunities, key=lambda x: x['spread_percent'], reverse=True)


async def main():
    exchange_names = ['bybit', 'binance', 'okx']
    exchange_urls = [
        'https://api.bybit.com/v5/market/tickers?category=spot',
        'https://api.binance.com/api/v3/ticker/price',
        'https://www.okx.com/api/v5/market/tickers?instType=SPOT'
    ]

    # Получаем и сохраняем данные
    responses = await asyncio.gather(*[get_order_book(url) for url in exchange_urls])
    write_to_file(responses, exchange_names)

    # Даем время на запись файлов
    await asyncio.sleep(1)

    # Обрабатываем данные
    data_bybit = Bybit.edit_data()
    data_binance = Binance.edit_data()
    data_okx = Okx.edit_data()

    # Находим арбитраж
    opportunities = arbitrage(data_bybit, data_binance, data_okx, threshold)

    # Красивый вывод результатов
    print("\nАрбитражные возможности:")
    print(f"Порог спреда: {threshold}%\n")

    for opp in opportunities:
        print(f"Нашел спред на монете {opp['pair']} между {opp['buy_exchange']} и {opp['sell_exchange']}.")
        print(f"Покупка: {opp['buy_price']:.2f}$")
        print(f"Продажа: {opp['sell_price']:.2f}$")
        print(f"Профит: {opp['spread_value']:.2f}$ ({opp['spread_percent']:.2f}%)")
        print("-" * 50)

    print(f"\nВсего найдено возможностей: {len(opportunities)}")


if __name__ == "__main__":
    while True:
        start = perf_counter()
        threshold = 0.50  # Минимальный спред %
        asyncio.run(main())
        print(f"Время выполнения скрипта: {(perf_counter() - start):.02f}")
        time.sleep(120)
