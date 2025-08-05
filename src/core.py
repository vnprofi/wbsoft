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
    "Цена basic мин",
    "Цена basic средн",
    "Цена basic макс",
    "Цена product мин",
    "Цена product средн",
    "Цена product макс",
    "Цена total мин",
    "Цена total средн",
    "Цена total макс",
    "Цена logistics средн",
    "Ср. рейтинг товаров",
    "Сумма отзывов товаров",
    "Ср. скидка %",
    "Макс. скидка %",
    "Кол-во акций",
    "Типы акций",
    "СкидТовар1",
    "СсылкаТовар1",
    "Скид%1",
    "Акция1",
    "Цена basic 1",
    "Цена product 1",
    "Цена total 1",
    "Цена logistics 1",
    "Рейтинг1",
    "СкидТовар2",
    "СсылкаТовар2",
    "Скид%2",
    "Акция2",
    "Цена basic 2",
    "Цена product 2",
    "Цена total 2",
    "Цена logistics 2",
    "Рейтинг2",
    "СкидТовар3",
    "СсылкаТовар3",
    "Скид%3",
    "Акция3",
    "Цена basic 3",
    "Цена product 3",
    "Цена total 3",
    "Цена logistics 3",
    "Рейтинг3",
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
    basic_prices, product_prices, total_prices, logistics_prices = [], [], [], []
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
            # Fallback: WB часто кладёт price только внутри sizes[0].price
            if not price_block:
                sizes_list = g.get("sizes")
                if isinstance(sizes_list, list) and sizes_list:
                    first_price = sizes_list[0].get("price") if isinstance(sizes_list[0], dict) else None
                    if isinstance(first_price, dict):
                        price_block = first_price

            basic_raw = price_block.get("basic") or g.get("priceU")
            product_raw = price_block.get("product") or g.get("salePriceU")
            total_raw = price_block.get("total")
            logistics_raw = price_block.get("logistics")

            # Convert to ₽
            basic = basic_raw / 100 if basic_raw else None
            product = product_raw / 100 if product_raw else None
            total = total_raw / 100 if total_raw else None
            logistics = logistics_raw / 100 if logistics_raw else None

            # legacy for priceMin/Max (promo or basic)
            promo_price = product if product is not None else basic
            if promo_price is not None:
                prices.append(promo_price)

            # accumulate per-component lists
            if basic is not None:
                basic_prices.append(basic)
            if product is not None:
                product_prices.append(product)
            if total is not None:
                total_prices.append(total)
            if logistics is not None:
                logistics_prices.append(logistics)

            # discount
            disc = 0
            if basic is not None and product is not None and basic > 0:
                disc = round((basic - product) / basic * 100, 1)
            disc_list.append(disc)
            # promos
            if g.get("promoTextCard"):
                promo_types_set.add(g.get("promoTextCard"))
            discount_items.append(
                (
                    g.get("id"),
                    disc,
                    g.get("promoTextCard"),
                    basic,
                    product,
                    total,
                    logistics,
                    g.get("rating"),
                )
            )
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
    disc_items_padded = disc_items_sorted + [(None,) * 8] * (3 - len(disc_items_sorted))

    return {
        "subjectsCount": len(cats),
        "topSubjects": [c[0] for c in top_cats] + [None] * (3 - len(top_cats)),
        "topBrands": [b[0] for b in top_brands] + [None] * (3 - len(top_brands)),
        "priceMin": min(prices) if prices else None,
        "priceAvg": round(sum(prices) / len(prices), 2) if prices else None,
        "priceMax": max(prices) if prices else None,
        # per-component aggregates
        "basicMin": min(basic_prices) if basic_prices else None,
        "basicAvg": round(sum(basic_prices) / len(basic_prices), 2) if basic_prices else None,
        "basicMax": max(basic_prices) if basic_prices else None,

        "productMin": min(product_prices) if product_prices else None,
        "productAvg": round(sum(product_prices) / len(product_prices), 2) if product_prices else None,
        "productMax": max(product_prices) if product_prices else None,

        "totalMin": min(total_prices) if total_prices else None,
        "totalAvg": round(sum(total_prices) / len(total_prices), 2) if total_prices else None,
        "totalMax": max(total_prices) if total_prices else None,

        "logisticsAvg": round(sum(logistics_prices) / len(logistics_prices), 2) if logistics_prices else None,
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

                    # Discounts list – each item is 8-tuple (id, disc, promo, basic, product, total, logistics, rating)
                    disc_items = sample.get("topDiscountItems", [])
                    disc_items = disc_items + [(None,) * 8] * (3 - len(disc_items))

                    # Build flattened list for each of 3 items: id, link, disc, promo, basic, product, total, logistics, rating
                    flat_disc: List = []
                    for gid, disc, promo, basic_p, product_p, total_p, logistics_p, rating_p in disc_items[:3]:
                        link = f"https://www.wildberries.ru/catalog/{gid}/detail.aspx" if gid else None
                        flat_disc.extend([
                            gid,
                            link,
                            disc,
                            promo,
                            basic_p,
                            product_p,
                            total_p,
                            logistics_p,
                            rating_p,
                        ])

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
                        sample.get("subjectsCount"),
                        *top_subjects,
                        *top_brands,
                        sample.get("priceMin"),
                        sample.get("priceAvg"),
                        sample.get("priceMax"),
                        # per-component aggregates
                        sample.get("basicMin"),
                        sample.get("basicAvg"),
                        sample.get("basicMax"),
                        sample.get("productMin"),
                        sample.get("productAvg"),
                        sample.get("productMax"),
                        sample.get("totalMin"),
                        sample.get("totalAvg"),
                        sample.get("totalMax"),
                        sample.get("logisticsAvg"),
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