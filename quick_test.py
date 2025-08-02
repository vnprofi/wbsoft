#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from core import export_data_sync

def test_seller():
    seller_ids = [893739]  # Тестовый селлер из вашего примера
    output_path = "test_debug.csv"
    
    print(f"Testing seller {seller_ids[0]}...")
    
    try:
        count = export_data_sync(seller_ids, output_path)
        print(f"Successfully processed {count} sellers")
        
        # Читаем результат
        with open(output_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            print(f"Output file has {len(lines)} lines")
            if len(lines) >= 2:
                print("Header:", lines[0].strip())
                print("Data:", lines[1].strip())
                
                # Парсим данные для анализа
                data_parts = lines[1].strip().split(';')
                print(f"\nAnalysis for seller {seller_ids[0]}:")
                print(f"ID: '{data_parts[0] if len(data_parts) > 0 else 'EMPTY'}'")
                print(f"Продавец: '{data_parts[1] if len(data_parts) > 1 else 'EMPTY'}'")
                print(f"Полное название: '{data_parts[2] if len(data_parts) > 2 else 'EMPTY'}'")
                print(f"ИНН: '{data_parts[3] if len(data_parts) > 3 else 'EMPTY'}'")
                print(f"КПП: '{data_parts[4] if len(data_parts) > 4 else 'EMPTY'}'")
                print(f"ОГРН: '{data_parts[5] if len(data_parts) > 5 else 'EMPTY'}'")
                
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_seller()