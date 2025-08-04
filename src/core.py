import asyncio
import aiohttp
import csv
import json
from typing import List, Callable, Optional

# HTTP constants
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; wb-seller-crawler/1.0)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# Desired CSV columns (order matters)
COLUMNS = [
    "ID",
    "Продавец",
    "Полное название",
    "Торговая марка",
    "Ссылка",
    "ИНН",
    "КПП",
    "ОГРН",
    "Адрес",
    "Регион",
    "Город",
    "Индекс",
    "Страна",
    "Категорий",
    "Топ категория 1",
    "Топ категория 2",
    "Топ категория 3",
    "Топ бренд 1",
    "Топ бренд 2",
    "Топ бренд 3",
    "Цена мин",
    "Цена средн",
    "Цена макс",
    "Ср. рейтинг товаров",
    "Сумма отзывов товаров",
    "Ср. скидка %",
    "Макс. скидка %",
    "Кол-во акций",
    "Типы акций",
    "СкидТовар1",
    "Скид%1",
    "Акция1",
    "СкидТовар2",
    "Скид%2",
    "Акция2",
    "СкидТовар3",
    "Скид%3",
    "Акция3",
]

# -------------------------------------------------------------
# Helper HTTP --------------------------------------------------
async def _get_json(session: aiohttp.ClientSession, url: str):
    try:
        async with session.get(url, headers=HEADERS) as r:
            if r.status != 200:
                return None
            
            # Читаем текст ответа
            text = await r.text()
            
            if not text.strip():
                return None
                
            # Парсим JSON
            try:
                data = json.loads(text)
                return data
            except json.JSONDecodeError:
                return None
            
    except Exception:
        return None

# -------------------------------------------------------------
# Data fetchers ------------------------------------------------
async def fetch_passport(session: aiohttp.ClientSession, sid: int):
    """Возвращает базовую информацию о продавце.

    У Wildberries встречаются разные схемы хранения supplier-by-id. Чаще всего это
    vol0, но для части продавцов встречаются vol, вычисляемые как sid // 1000
    (историческая схема) или другие варианты. Чтобы гарантированно получить
    данные, пробуем несколько вариантов, останавливаясь на первом успешном.
    """

    possible_vols = [0]
    # Схема «/vol{id//1000}/» встречается в старых примерах
    possible_vols.append(sid // 1000)
    # На всякий случай пытаемся и //10000 – замечено у части новых ID
    possible_vols.append(sid // 10000)

    data = None
    for vol in dict.fromkeys(possible_vols):  # preserve order, remove dups
        url = f"https://static-basket-01.wbbasket.ru/vol{vol}/data/supplier-by-id/{sid}.json"
        try:
            data = await _get_json(session, url)
                
            # Проверяем что получили валидные данные - как в рабочем локальном коде
            if data and data.get("supplierName"):
                break
        except Exception as e:
            print(f"ERROR: Exception fetching passport for seller {sid} from vol{vol}: {e}")
            continue

    # Если не получили валидные данные, возвращаем None
    if not data or not data.get("supplierName"):
        return None

    # extract address-related fields if present
    legal = data.get("legalAddress")
    address_block = legal if isinstance(legal, dict) else {}
    address_value = (address_block.get("address") if isinstance(legal, dict) else legal) or data.get("address")

    result = {
        "supplierName": data.get("supplierName"),
        "supplierFullName": data.get("supplierFullName"),
        "trademark": data.get("trademark"),
        "inn": data.get("inn"),
        "kpp": data.get("kpp"),
        "ogrn": data.get("ogrn") or data.get("ogrnip") or data.get("ogrnIp"),
        "address": address_value,
        "region": address_block.get("region"),
        "city": address_block.get("city"),
        "zip": address_block.get("zip") or address_block.get("postCode"),
        "country": address_block.get("country"),
    }
    
    return result

async def fetch_cards_info(session: aiohttp.ClientSession, sid: int):
    vol = sid // 1000
    part = sid // 100
    url = f"https://basket-07.wb.ru/vol{vol}/part{part}/info/sellers/{sid}.json"
    js = await _get_json(session, url)
    if js:
        return {"cardsCount": js.get("cardsCount"), "brandsCount": js.get("brandsCount")}
    return None

async def fetch_goods_sample(session: aiohttp.ClientSession, sid: int, pages: int = 3):
    cats, brands = {}, {}
    prices, ratings, feedbacks = [], [], []
    disc_list = []
    promo_types_set = set()
    discount_items = []  # (id, disc, promo)
    goods_seen = 0
    for page in range(1, pages + 1):
        url = (
            "https://catalog.wb.ru/sellers/v2/catalog?ab_testing=false&appType=1&curr=rub&dest=-1184644"
            f"&page={page}&sort=popular&spp=30&supplier={sid}&uclusters=2"
        )
        js = await _get_json(session, url)
        products = (js or {}).get("data", {}).get("products", [])
        if not products:
            break
        for g in products:
            goods_seen += 1
            cat_name = g.get("subjectName") or g.get("entity") or ""
            cats[cat_name] = cats.get(cat_name, 0) + 1
            brand_name = g.get("brand") or ""
            brands[brand_name] = brands.get(brand_name, 0) + 1
            # prices
            price_block = g.get("price") if isinstance(g.get("price"), dict) else {}
            basic = price_block.get("basic") or g.get("priceU")
            promo = price_block.get("product") or g.get("salePriceU")
            if (promo or basic):
                prices.append((promo or basic) / 100)
            # discount
            disc = 0
            if basic and promo and basic > 0:
                disc = round((basic - promo) / basic * 100, 1)
            disc_list.append(disc)
            # promos
            if g.get("promoTextCard"):
                promo_types_set.add(g.get("promoTextCard"))
            discount_items.append((g.get("id"), disc, g.get("promoTextCard")))
            # ratings
            if g.get("rating"):
                ratings.append(g.get("rating"))
            if g.get("feedbacks"):
                feedbacks.append(g.get("feedbacks"))
    # aggregates
    def top_n(d, n=3):
        return sorted(d.items(), key=lambda x: -x[1])[:n]
    top_cats = top_n(cats)
    top_brands = top_n(brands)
    # Ensure exactly 3 discount tuples (nmId, disc%, promoText)
    disc_items_sorted = sorted(discount_items, key=lambda x: -x[1])[:3]
    disc_items_padded = disc_items_sorted + [(None, None, None)] * (3 - len(disc_items_sorted))

    return {
        "subjectsCount": len(cats),
        "topSubjects": [c[0] for c in top_cats] + [None] * (3 - len(top_cats)),
        "topBrands": [b[0] for b in top_brands] + [None] * (3 - len(top_brands)),
        "priceMin": min(prices) if prices else None,
        "priceAvg": round(sum(prices) / len(prices), 2) if prices else None,
        "priceMax": max(prices) if prices else None,
        "ratingAvgProd": round(sum(ratings) / len(ratings), 2) if ratings else None,
        "feedbacksSum": sum(feedbacks) if feedbacks else None,
        "discountAvg": round(sum(disc_list) / len(disc_list), 1) if disc_list else None,
        "discountMax": max(disc_list) if disc_list else None,
        "promoCount": len(promo_types_set),
        "promoTypes": "|".join(promo_types_set) if promo_types_set else None,
        "topDiscountItems": disc_items_padded,
    }

# -------------------------------------------------------------
# Core export --------------------------------------------------
async def export_data(
    seller_ids: List[int],
    output_path: str,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    concurrency: int = 10,
):
    """Fetch data for given seller ids and save to CSV or Excel based on output_path extension."""
    import pandas as pd

    q = asyncio.Queue()
    for sid in seller_ids:
        q.put_nowait(sid)

    rows: List[List] = []
    lock = asyncio.Lock()

    async def worker(session: aiohttp.ClientSession):
        nonlocal rows
        while True:
            try:
                sid = await q.get()
                try:
                    passport_raw = await fetch_passport(session, sid)
                    sample_raw = await fetch_goods_sample(session, sid)

                    # Use empty dicts if any of the fetches failed so that we still output the row
                    passport = passport_raw or {}
                    sample = sample_raw or {}
                    
                    # Helper to ensure lists always have exactly 3 elements
                    def pad(lst, size=3, fill=None):
                        return (list(lst) + [fill] * size)[:size]

                    # Top subjects / brands always length 3
                    top_subjects = pad(sample.get("topSubjects", []))
                    top_brands = pad(sample.get("topBrands", []))

                    # Discounts list – each item is (id, disc, promo). Ensure exactly 3 tuples
                    disc_items = sample.get("topDiscountItems", [])
                    disc_items = disc_items + [(None, None, None)] * (3 - len(disc_items))
                    flat_disc = [item for triple in disc_items[:3] for item in triple]

                    row = [
                        sid,
                        passport.get("supplierName"),
                        passport.get("supplierFullName"),
                        passport.get("trademark"),
                        f"https://www.wildberries.ru/seller/{sid}",
                        passport.get("inn"),
                        passport.get("kpp"),
                        passport.get("ogrn"),
                        passport.get("address"),
                        passport.get("region"),
                        passport.get("city"),
                        passport.get("zip"),
                        passport.get("country"),
                        sample.get("subjectsCount"),
                        *top_subjects,
                        *top_brands,
                        sample.get("priceMin"),
                        sample.get("priceAvg"),
                        sample.get("priceMax"),
                        sample.get("ratingAvgProd"),
                        sample.get("feedbacksSum"),
                        sample.get("discountAvg"),
                        sample.get("discountMax"),
                        sample.get("promoCount"),
                        sample.get("promoTypes"),
                        *flat_disc,
                    ]
                    
                    async with lock:
                        rows.append(row)
                except Exception as exc:
                    # log and continue processing other IDs
                    print(f"Error processing {sid}: {exc}")
                finally:
                    if progress_cb:
                        progress_cb(len(rows), len(seller_ids))
                    q.task_done()
            except asyncio.CancelledError:
                break

    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as session:
        tasks = [asyncio.create_task(worker(session)) for _ in range(concurrency)]
        await q.join()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    # Save
    df = pd.DataFrame(rows, columns=COLUMNS)
    if output_path.lower().endswith(".xlsx"):
        df.to_excel(output_path, index=False)
    else:
        df.to_csv(output_path, index=False, sep=";", encoding="utf-8")

    return len(rows)

# Convenience sync wrapper

def export_data_sync(seller_ids: List[int], output_path: str, progress_cb: Optional[Callable[[int, int], None]] = None):
    """Blocking convenience wrapper for export_data inside non-async contexts."""
    return asyncio.run(export_data(seller_ids, output_path, progress_cb))