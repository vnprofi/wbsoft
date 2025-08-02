import asyncio
import aiohttp
import csv
import json
import time

# ----------- Параметры --------------------------------------
INPUT_PATH = r"C:\Users\User\Desktop\wb\active_seller.txt"
OUTPUT_PATH = r"C:\Users\User\Desktop\wb\active_seller_full.csv"
MAX_SELLERS = 300
CONCURRENCY = 5
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; wb-seller-crawler/0.5)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# ----------- HTTP helper ------------------------------------
async def get_json(session: aiohttp.ClientSession, url: str):
    try:
        async with session.get(url, headers=HEADERS) as r:
            if r.status != 200:
                return None
            return await r.json(loads=json.loads, content_type=None)
    except Exception:
        return None

# ----------- basic passport ---------------------------------
async def fetch_passport(session, sid: int):
    vol = 0 if sid < 1_000_000 else sid // 1000
    url = f"https://static-basket-01.wbbasket.ru/vol{vol}/data/supplier-by-id/{sid}.json"
    data = await get_json(session, url)
    if data and data.get("supplierName"):
        return {
            "supplierName": data.get("supplierName"),
            "supplierFullName": data.get("supplierFullName"),
            "inn": data.get("inn"),
            "kpp": data.get("kpp"),
            "ogrn": data.get("ogrn"),
            "registrationDate": data.get("registrationDate"),
            "legalType": data.get("legalType"),
            "siteUrl": data.get("siteUrl"),
        }
    return None

# ----------- rating / feedbacks -----------------------------
async def fetch_rating(session, sid: int):
    url = f"https://suppliers-rating.wildberries.ru/api/v1/suppliers/{sid}"
    js = await get_json(session, url)
    if js:
        return {"rating": js.get("rating"), "feedbacks": js.get("feedbacks")}
    return None

# ----------- cards & brands count ---------------------------
async def fetch_cards_info(session, sid: int):
    vol = sid // 1000
    part = sid // 100
    url = f"https://basket-07.wb.ru/vol{vol}/part{part}/info/sellers/{sid}.json"
    js = await get_json(session, url)
    if js:
        return {"cardsCount": js.get("cardsCount"), "brandsCount": js.get("brandsCount")}
    return None

# ----------- geographic data --------------------------------
async def fetch_geographic_data(session, sid: int):
    """Получение географических данных продавца"""
    vol = 0 if sid < 1_000_000 else sid // 1000
    url = f"https://static-basket-01.wbbasket.ru/vol{vol}/data/supplier-by-id/{sid}.json"
    data = await get_json(session, url)
    if data:
        return {
            "address": data.get("address"),
            "region": data.get("region"),
            "city": data.get("city"),
            "postalCode": data.get("postalCode"),
            "country": data.get("country"),
        }
    return None

# ----------- goods sample (categories, brands, price stats & promos) --
async def fetch_goods_sample(session, sid: int, pages: int = 3):
    cats, brands = {}, {}
    prices, ratings, feedbacks = [], [], []
    goods_seen = 0
    for page in range(1, pages+1):
        url = ("https://catalog.wb.ru/sellers/v2/catalog?ab_testing=false&appType=1&curr=rub&dest=-1184644"
               f"&page={page}&sort=popular&spp=30&supplier={sid}&uclusters=2")
        js = await get_json(session, url)
        products = (js or {}).get("data", {}).get("products", [])
        if not products:
            break
        for g in products:
            goods_seen += 1
            cats[g.get("subjectName") or g.get("subject") or ""] = cats.get(g.get("subjectName") or g.get("subject") or "", 0) + 1
            brands[g.get("brand") or ""] = brands.get(g.get("brand") or "", 0) + 1
            basic = g.get("price", {}).get("basic") if isinstance(g.get("price"), dict) else g.get("priceU")
            promo = g.get("price", {}).get("product") if isinstance(g.get("price"), dict) else g.get("salePriceU")
            price_val = (promo or basic)
            if price_val:
                prices.append(price_val / 100)
            if basic and promo and basic>0:
                discount = round((basic-promo)/basic*100,1) if promo else 0
            else:
                discount = 0
            g["_disc"] = discount
            if g.get("rating"):
                ratings.append(g.get("rating"))
            if g.get("feedbacks"):
                feedbacks.append(g.get("feedbacks"))
    def top_n(d, n=3):
        return sorted(d.items(), key=lambda x: -x[1])[:n]
    top_cats = top_n(cats)
    top_brands = top_n(brands)
    discountAvg = round(sum([g['_disc'] for g in products]) / len(products),1) if goods_seen else None
    discountMax = max([g['_disc'] for g in products]) if goods_seen else None
    promo_types = set([g.get('promoTextCard') for g in products if g.get('promoTextCard')])
    top_by_discount = sorted(products, key=lambda x: -x['_disc'])[:3]
    top_discount_items = [(t.get('id'), t.get('_disc'), t.get('promoTextCard')) for t in top_by_discount]
    return {
        "subjectsCount": len(cats),
        "topSubjects": top_cats,
        "brandsCountSample": len(brands),
        "topBrands": top_brands,
        "priceMin": min(prices) if prices else None,
        "priceMax": max(prices) if prices else None,
        "priceAvg": round(sum(prices) / len(prices), 2) if prices else None,
        "ratingAvgProd": round(sum(ratings) / len(ratings), 2) if ratings else None,
        "feedbacksSum": sum(feedbacks) if feedbacks else None,
        "discountAvg": discountAvg,
        "discountMax": discountMax,
        "promoCount": len(promo_types),
        "promoTypes": "|".join(promo_types) if promo_types else None,
        "topDiscountItems": top_discount_items,
        "goodsSample": goods_seen,
    }


# ----------- fixed goods sample --------------------------------------
async def fetch_goods_sample(session, sid: int, pages: int = 3):
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
        js = await get_json(session, url)
        products = (js or {}).get("data", {}).get("products", [])
        if not products:
            break
        for g in products:
            goods_seen += 1
            cats_name = g.get("subjectName") or g.get("entity") or ""
            cats[cats_name] = cats.get(cats_name, 0) + 1
            brand_name = g.get("brand") or ""
            brands[brand_name] = brands.get(brand_name, 0) + 1
            # цены
            price_block = g.get("price") if isinstance(g.get("price"), dict) else {}
            basic = price_block.get("basic") or g.get("priceU")
            promo = price_block.get("product") or g.get("salePriceU")
            price_val = promo or basic
            if price_val:
                prices.append(price_val / 100)
            # скидка
            disc = 0
            if basic and promo and basic > 0:
                disc = round((basic - promo) / basic * 100, 1)
            disc_list.append(disc)
            # акции
            if g.get("promoTextCard"):
                promo_types_set.add(g.get("promoTextCard"))
            discount_items.append((g.get("id"), disc, g.get("promoTextCard")))
            # рейтинги
            if g.get("rating"):
                ratings.append(g.get("rating"))
            if g.get("feedbacks"):
                feedbacks.append(g.get("feedbacks"))
    # агрегаты
    def top_n(d, n=3):
        return sorted(d.items(), key=lambda x: -x[1])[:n]
    top_cats = top_n(cats)
    top_brands = top_n(brands)
    discountAvg = round(sum(disc_list) / len(disc_list), 1) if disc_list else None
    discountMax = max(disc_list) if disc_list else None
    top_discount_items = sorted(discount_items, key=lambda x: -x[1])[:3]
    return {
        "subjectsCount": len(cats),
        "topSubjects": top_cats,
        "brandsCountSample": len(brands),
        "topBrands": top_brands,
        "priceMin": min(prices) if prices else None,
        "priceMax": max(prices) if prices else None,
        "priceAvg": round(sum(prices) / len(prices), 2) if prices else None,
        "ratingAvgProd": round(sum(ratings) / len(ratings), 2) if ratings else None,
        "feedbacksSum": sum(feedbacks) if feedbacks else None,
        "discountAvg": discountAvg,
        "discountMax": discountMax,
        "promoCount": len(promo_types_set),
        "promoTypes": "|".join(promo_types_set) if promo_types_set else None,
        "topDiscountItems": top_discount_items,
        "goodsSample": goods_seen,
    }

# ----------- top sales --------------------------------------
async def fetch_top_sales(session, sid: int, count: int = 5):
    vol = sid // 1000
    part = sid // 100
    url = f"https://basket-03.wb.ru/vol{vol}/part{part}/top/suppliers/{sid}.json"
    js = await get_json(session, url)
    top = js[:count] if isinstance(js, list) else []
    return top

# ----------- worker -----------------------------------------
async def worker(q: asyncio.Queue, session: aiohttp.ClientSession, writer: csv.writer, lock: asyncio.Lock):
    while True:
        try:
            sid = await q.get()
            try:
                passport = await fetch_passport(session, sid)
                if not passport:
                    q.task_done(); continue
                rating = await fetch_rating(session, sid)
                cards = await fetch_cards_info(session, sid)
                geographic = await fetch_geographic_data(session, sid)
                sample = await fetch_goods_sample(session, sid)
                top_sales = await fetch_top_sales(session, sid)

                top_subjects = [s[0] for s in sample["topSubjects"]] + [None, None, None]
                top_brands = [b[0] for b in sample["topBrands"]] + [None, None, None]
                flat_sales = []
                for itm in top_sales:
                    flat_sales.extend([itm.get("nm"), itm.get("sales")])
                while len(flat_sales) < 10:
                    flat_sales.extend([None, None])
                # топ-скидки
                disc_items = sample.get("topDiscountItems") or []
                disc_items += [(None, None, None)] * (3 - len(disc_items))
                flat_disc = []
                for d in disc_items[:3]:
                    flat_disc.extend(d)  # nmId, disc%, promoText

                row = [
                    sid,
                    passport.get("supplierName"),
                    passport.get("supplierFullName"),
                    passport.get("inn"),
                    passport.get("kpp"),
                    passport.get("ogrn"),
                    passport.get("registrationDate"),
                    passport.get("legalType"),
                    passport.get("siteUrl"),
                    geographic.get("address") if geographic else None,
                    geographic.get("region") if geographic else None,
                    geographic.get("city") if geographic else None,
                    geographic.get("postalCode") if geographic else None,
                    geographic.get("country") if geographic else None,
                    rating.get("rating") if rating else None,
                    rating.get("feedbacks") if rating else None,
                    cards.get("cardsCount") if cards else None,
                    cards.get("brandsCount") if cards else None,
                    sample.get("subjectsCount"),
                    *top_subjects[:3],
                    *top_brands[:3],
                    sample.get("priceMin"),
                    sample.get("priceAvg"),
                    sample.get("priceMax"),
                    sample.get("ratingAvgProd"),
                    sample.get("feedbacksSum"),
                    sample.get("discountAvg"), sample.get("discountMax"),
                    sample.get("promoCount"), sample.get("promoTypes"),
                    *flat_disc,
                    *flat_sales[:10],
                ]
                async with lock:
                    writer.writerow(row)
            finally:
                q.task_done()
        except asyncio.CancelledError:
            break

# ----------- main -------------------------------------------
async def main():
    import re
    with open(INPUT_PATH, encoding="utf-8-sig") as f:
        seller_ids = []
        for line in f:
            m = re.search(r"\d{3,}", line)
            if m:
                seller_ids.append(int(m.group()))
            if len(seller_ids) >= MAX_SELLERS:
                break
    if not seller_ids:
        print("Нет ID"); return

    q = asyncio.Queue()
    for sid in seller_ids:
        q.put_nowait(sid)

    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as session:
        with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow([
                "ID", "Продавец", "Полное название", "ИНН", "КПП", "ОГРН",
                "Дата рег.", "Тип юр.лица", "Сайт", "Адрес", "Регион", "Город", "Индекс", "Страна",
                "Рейтинг WB", "Отзывы WB",
                "Карточек", "Брендов",
                "Категорий", "Топ категория 1", "Топ категория 2", "Топ категория 3",
                "Топ бренд 1", "Топ бренд 2", "Топ бренд 3",
                "Цена мин", "Цена средн", "Цена макс", "Ср. рейтинг товаров", "Сумма отзывов товаров",
                "Ср. скидка %", "Макс. скидка %", "Кол-во акций", "Типы акций",
                "СкидТовар1", "Скид%1", "Акция1", "СкидТовар2", "Скид%2", "Акция2", "СкидТовар3", "Скид%3", "Акция3",
                "ТопТовар1", "Продажи1", "ТопТовар2", "Продажи2", "ТопТовар3", "Продажи3", "ТопТовар4", "Продажи4", "ТопТовар5", "Продажи5"
            ])
            lock = asyncio.Lock()
            tasks = [asyncio.create_task(worker(q, session, writer, lock)) for _ in range(CONCURRENCY)]
            await q.join()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    print("✓ Завершено. CSV обновлён.")

if __name__ == "__main__":
    asyncio.run(main())
