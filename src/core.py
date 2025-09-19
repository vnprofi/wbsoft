import asyncio
import aiohttp
import csv
import json
from typing import List, Callable, Optional
import os

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


# -------------------------------------------------------------
# HTML report --------------------------------------------------
def _escape_html(text: Optional[str]) -> str:
    if text is None:
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _render_html_report(df, html_path: str, download_csv: Optional[str], download_xlsx: Optional[str]):
    # Build HTML table body
    rows_html = []
    for _, row in df.iterrows():
        tds = []
        for col in COLUMNS:
            val = row.get(col)
            if col.startswith("Ссылка"):
                if isinstance(val, str) and val:
                    tds.append(f'<td><a href="{_escape_html(val)}" target="_blank">ссылка</a></td>')
                else:
                    tds.append("<td></td>")
            else:
                tds.append(f"<td>{_escape_html(val)}</td>")
        # add a select checkbox for compare
        tds.insert(0, '<td><input type="checkbox" class="row-select" /></td>')
        rows_html.append("<tr>" + "".join(tds) + "</tr>")

    # Headers
    thead_cols = ["<th></th>"] + [f"<th>{_escape_html(c)}</th>" for c in COLUMNS]
    thead_html = "<tr>" + "".join(thead_cols) + "</tr>"

    # Basic, dependency-free interactivity
    html = f"""
<!DOCTYPE html>
<html lang=\"ru\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Отчет по продавцам WB</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; }}
    header {{ padding: 14px 18px; background: #6a11cb; background: linear-gradient(90deg,#6a11cb,#2575fc); color: #fff; }}
    header h1 {{ margin: 0; font-size: 20px; }}
    .container {{ padding: 16px 18px 28px; }}
    .toolbar {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; margin-bottom: 12px; }}
    .toolbar input[type=text] {{ padding: 8px 10px; border: 1px solid #ccc; border-radius: 6px; min-width: 220px; }}
    .toolbar .range {{ display: flex; gap: 6px; align-items: center; }}
    .toolbar label {{ color: #333; font-size: 14px; }}
    .toolbar a.btn {{ text-decoration: none; padding: 8px 12px; border-radius: 6px; background: #f0f2f5; color: #333; border: 1px solid #d0d7de; }}
    .toolbar a.btn.primary {{ background: #2563eb; color: #fff; border-color: #1d4ed8; }}
    .grid {{ overflow: auto; border: 1px solid #e5e7eb; border-radius: 8px; }}
    table {{ border-collapse: separate; border-spacing: 0; width: 100%; }}
    thead th {{ position: sticky; top: 0; background: #f8fafc; z-index: 1; text-align: left; padding: 10px; border-bottom: 1px solid #e5e7eb; font-weight: 600; font-size: 13px; color: #111827; }}
    tbody td {{ padding: 10px; border-bottom: 1px solid #f1f5f9; font-size: 13px; color: #111827; white-space: nowrap; }}
    tbody tr:hover {{ background: #f8fafc; }}
    .panel {{ margin-top: 14px; padding: 12px; border: 1px solid #e5e7eb; border-radius: 8px; background: #fcfcfd; }}
    .panel h3 {{ margin: 0 0 10px; font-size: 16px; }}
    .muted {{ color: #6b7280; }}
    .chips {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .chip {{ background: #eef2ff; color: #3730a3; padding: 6px 10px; border-radius: 999px; font-size: 12px; border: 1px solid #c7d2fe; }}
    .sortable {{ cursor: pointer; }}
  </style>
</head>
<body>
  <header>
    <h1>Отчет по продавцам WB</h1>
  </header>
  <div class=\"container\">
    <div class=\"toolbar\">
      <input id=\"globalFilter\" type=\"text\" placeholder=\"Фильтр по любому полю…\" />
      <div class=\"range\">
        <label>Цена мин от</label>
        <input id=\"minPrice\" type=\"number\" step=\"0.01\" style=\"width:110px\" />
        <label>до</label>
        <input id=\"maxPrice\" type=\"number\" step=\"0.01\" style=\"width:110px\" />
      </div>
      { (f'<a class=\"btn\" href=\"{download_csv}\">Скачать CSV</a>') if download_csv else '' }
      { (f'<a class=\"btn\" href=\"{download_xlsx}\">Скачать Excel</a>') if download_xlsx else '' }
      <a id=\"clearSel\" class=\"btn\" href=\"#\">Снять выделение</a>
      <a id=\"sortByPrice\" class=\"btn\" href=\"#\">Сортировать по цене средн</a>
    </div>
    <div class=\"grid\">
      <table id=\"data\">\n        <thead>\n          {thead_html}\n        </thead>\n        <tbody>\n          {''.join(rows_html)}\n        </tbody>\n      </table>
    </div>
    <div class=\"panel\">
      <h3>Сравнение цен выбранных продавцов</h3>
      <div id=\"compareChips\" class=\"chips\"></div>
      <div class=\"muted\" style=\"margin-top:8px\">Выберите строки (чекбоксы) для сравнения. Сравниваются столбцы «Цена мин», «Цена средн», «Цена макс».</div>
      <div id=\"compareStats\" style=\"margin-top:10px\"></div>
    </div>
  </div>
  <script>
    (function() {{
      const table = document.getElementById('data');
      const globalFilter = document.getElementById('globalFilter');
      const minPrice = document.getElementById('minPrice');
      const maxPrice = document.getElementById('maxPrice');
      const clearSel = document.getElementById('clearSel');
      const sortByPrice = document.getElementById('sortByPrice');
      const tbody = table.querySelector('tbody');
      const rows = Array.from(tbody.querySelectorAll('tr'));

      function textOfRow(row) {{
        return row.innerText.toLowerCase();
      }}

      function getNum(cellText) {{
        const v = parseFloat(cellText.replace(',', '.'));
        return isNaN(v) ? null : v;
      }}

      function applyFilters() {{
        const q = globalFilter.value.trim().toLowerCase();
        const mn = parseFloat(minPrice.value.replace(',', '.'));
        const mx = parseFloat(maxPrice.value.replace(',', '.'));

        rows.forEach(row => {{
          let ok = true;
          if (q) ok = textOfRow(row).includes(q);

          // Цена мин находится в колонке с заголовком "Цена мин"
          if (ok && (minPrice.value || maxPrice.value)) {{
            const cells = row.querySelectorAll('td');
            // offset +1 из-за чекбокса в начале
            const priceMinCell = cells[{COLUMNS.index('Цена мин') + 1}];
            const priceMin = priceMinCell ? getNum(priceMinCell.textContent) : null;
            if (minPrice.value && (priceMin === null || priceMin < mn)) ok = false;
            if (maxPrice.value && (priceMin === null || priceMin > mx)) ok = false;
          }}

          row.style.display = ok ? '' : 'none';
        }});
      }}

      globalFilter.addEventListener('input', applyFilters);
      minPrice.addEventListener('input', applyFilters);
      maxPrice.addEventListener('input', applyFilters);

      clearSel.addEventListener('click', e => {{
        e.preventDefault();
        tbody.querySelectorAll('input.row-select').forEach(cb => cb.checked = false);
        updateCompare();
      }});

      sortByPrice.addEventListener('click', e => {{
        e.preventDefault();
        const idx = {COLUMNS.index('Цена средн') + 1};
        const sorted = rows.slice().sort((a,b) => {{
          const av = getNum(a.children[idx].textContent) ?? -Infinity;
          const bv = getNum(b.children[idx].textContent) ?? -Infinity;
          return bv - av;
        }});
        sorted.forEach(r => tbody.appendChild(r));
      }});

      // Column sorting on header click
      const ths = Array.from(table.querySelectorAll('thead th'));
      ths.forEach((th, i) => {{
        th.classList.add('sortable');
        let asc = true;
        th.addEventListener('click', () => {{
          if (i === 0) return; // checkbox column
          const idx = i;
          const sorted = rows.slice().sort((a,b) => {{
            const at = a.children[idx].textContent.trim();
            const bt = b.children[idx].textContent.trim();
            const an = parseFloat(at.replace(',', '.'));
            const bn = parseFloat(bt.replace(',', '.'));
            const aIsNum = !isNaN(an);
            const bIsNum = !isNaN(bn);
            let res;
            if (aIsNum && bIsNum) res = (an - bn);
            else res = at.localeCompare(bt, 'ru', {{ sensitivity: 'base' }});
            return asc ? res : -res;
          }});
          asc = !asc;
          sorted.forEach(r => tbody.appendChild(r));
        }});
      }});

      // Compare panel
      const chips = document.getElementById('compareChips');
      const stats = document.getElementById('compareStats');

      function updateCompare() {{
        const selected = rows.filter(r => r.querySelector('input.row-select').checked);
        chips.innerHTML = '';
        const nameIdx = {COLUMNS.index('Продавец') + 1};
        selected.forEach(r => {{
          const name = r.children[nameIdx].textContent || '—';
          const chip = document.createElement('span');
          chip.className = 'chip';
          chip.textContent = name;
          chips.appendChild(chip);
        }});

        const fields = [
          {{ key: 'Цена мин', idx: {COLUMNS.index('Цена мин') + 1} }},
          {{ key: 'Цена средн', idx: {COLUMNS.index('Цена средн') + 1} }},
          {{ key: 'Цена макс', idx: {COLUMNS.index('Цена макс') + 1} }},
        ];

        function numFrom(r, i) {{
          const t = r.children[i].textContent;
          const v = parseFloat(t.replace(',', '.'));
          return isNaN(v) ? null : v;
        }}

        let html = '';
        fields.forEach(f => {{
          const values = selected.map(r => numFrom(r, f.idx)).filter(v => v !== null);
          if (values.length === 0) return;
          const min = Math.min.apply(null, values);
          const max = Math.max.apply(null, values);
          const avg = values.reduce((a,b) => a+b, 0) / values.length;
          html += `<div><strong>${{f.key}}</strong>: мин ${{min}}, ср ${{avg.toFixed(2)}}, макс ${{max}}</div>`;
        }});
        stats.innerHTML = html || '<span class="muted">Ничего не выбрано.</span>';
      }}

      tbody.addEventListener('change', e => {{
        if (e.target.classList.contains('row-select')) updateCompare();
      }});

      // initial state
      applyFilters();
    }})();
  </script>
</body>
</html>
"""

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)


async def export_html_report(
    seller_ids: List[int],
    output_html_path: str,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    concurrency: int = 10,
):
    """Fetch data and build an interactive HTML report. Also saves CSV and XLSX next to HTML.

    Returns the path to the generated HTML file.
    """
    import pandas as pd

    # We reuse export_data logic to collect rows, but avoid saving a non-HTML file
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

                    passport = passport_raw or {}
                    sample = sample_raw or {}

                    def pad(lst, size=3, fill=None):
                        return (list(lst) + [fill] * size)[:size]

                    top_subjects = pad(sample.get("topSubjects", []))
                    top_brands = pad(sample.get("topBrands", []))

                    disc_items = sample.get("topDiscountItems", [])
                    disc_items = disc_items + [(None,) * 8] * (3 - len(disc_items))

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
                except Exception:
                    pass
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

    df = pd.DataFrame(rows, columns=COLUMNS)

    base, _ = os.path.splitext(output_html_path)
    csv_path = base + ".csv"
    xlsx_path = base + ".xlsx"

    # Save CSV and XLSX so user can download from report
    try:
        df.to_csv(csv_path, index=False, sep=";", encoding="utf-8")
    except Exception:
        csv_path = None
    try:
        df.to_excel(xlsx_path, index=False)
    except Exception:
        xlsx_path = None

    # Use relative links next to HTML
    csv_link = os.path.basename(csv_path) if csv_path else None
    xlsx_link = os.path.basename(xlsx_path) if xlsx_path else None
    _render_html_report(df, output_html_path, csv_link, xlsx_link)

    return output_html_path