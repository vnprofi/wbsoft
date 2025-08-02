#!/usr/bin/env python3

import sys
import os
import asyncio
import aiohttp

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from core import fetch_passport, HEADERS, HTTP_TIMEOUT

async def test_seller_893739():
    sid = 893739
    print(f"Testing seller {sid}...")
    
    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as session:
        passport = await fetch_passport(session, sid)
        
        if passport:
            print("✅ SUCCESS! Got passport data:")
            for key, value in passport.items():
                print(f"  {key}: {value}")
        else:
            print("❌ FAILED: No passport data returned")
            
            # Попробуем напрямую проверить эндпоинт
            print("\nTrying direct API call...")
            vol = 0 if sid < 1_000_000 else sid // 1000
            url = f"https://static-basket-01.wbbasket.ru/vol{vol}/data/supplier-by-id/{sid}.json"
            
            try:
                async with session.get(url, headers=HEADERS) as r:
                    print(f"Status: {r.status}")
                    if r.status == 200:
                        data = await r.json()
                        print(f"Raw data: {data}")
                    else:
                        print(f"HTTP error: {r.status}")
            except Exception as e:
                print(f"Exception: {e}")

if __name__ == "__main__":
    asyncio.run(test_seller_893739())