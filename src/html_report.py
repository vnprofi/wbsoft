"""
Упрощённый HTML генератор отчётов для тестирования
"""

import json
import os
import tempfile
import webbrowser
from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd


class SimpleHTMLReportGenerator:
    """Простой генератор HTML отчётов для тестирования."""
    
    def __init__(self):
        self.data = []
        self.metadata = {}
    
    def set_data(self, data: List[Dict[str, Any]], metadata: Optional[Dict] = None):
        """Устанавливает данные для использования в отчёте."""
        self.data = data
        self.metadata = metadata or {}
    
    def generate_report(self, output_path: Optional[str] = None) -> str:
        """Генерирует простой HTML отчёт."""
        if not self.data:
            raise ValueError("Нет данных для создания отчёта")
        
        if output_path is None:
            temp_dir = tempfile.gettempdir()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(temp_dir, f"wb_seller_report_simple_{timestamp}.html")
        
        # Конвертируем данные в JSON для JavaScript
        data_json = json.dumps(self.data, ensure_ascii=False, default=str)
        
        # Генерируем статистику
        stats = self._calculate_statistics()
        stats_json = json.dumps(stats, ensure_ascii=False, default=str)
        
        # Текущее время
        generation_time = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        
        html_content = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>WB Seller Report - {generation_time}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdn.datatables.net/1.13.7/css/dataTables.bootstrap5.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    
    <style>
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .stat-card.success {{ background: linear-gradient(135deg, #4CAF50 0%, #45a049 100%); }}
        .stat-card.info {{ background: linear-gradient(135deg, #2196F3 0%, #1976D2 100%); }}
        .stat-card.warning {{ background: linear-gradient(135deg, #FF9800 0%, #F57C00 100%); }}
        .stat-value {{ font-size: 2rem; font-weight: bold; margin-bottom: 0; }}
        .stat-label {{ font-size: 0.9rem; opacity: 0.9; }}
    </style>
</head>
<body>
    <div class="container-fluid">
        <div class="row mb-4">
            <div class="col-12">
                <h1 class="h3"><i class="fas fa-chart-line me-2"></i>WB Seller Analytics</h1>
                <small class="text-muted">Отчёт создан: {generation_time}</small>
            </div>
        </div>
        
        <!-- Статистика -->
        <div class="row mb-4" id="statisticsCards"></div>
        
        <!-- Кнопки экспорта -->
        <div class="row mb-3">
            <div class="col-12">
                <button class="btn btn-success me-2" onclick="exportToCSV()">
                    <i class="fas fa-file-csv me-1"></i>Экспорт в CSV
                </button>
                <button class="btn btn-primary me-2" onclick="exportToExcel()">
                    <i class="fas fa-file-excel me-1"></i>Экспорт в Excel
                </button>
            </div>
        </div>
        
        <!-- Фильтры -->
        <div class="row mb-3">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5 class="mb-0">Фильтры</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-3">
                                <label>Поиск по названию:</label>
                                <input type="text" id="searchFilter" class="form-control" placeholder="Поиск...">
                            </div>
                            <div class="col-md-3">
                                <label>Мин. цена:</label>
                                <input type="number" id="priceMinFilter" class="form-control" placeholder="Мин. цена">
                            </div>
                            <div class="col-md-3">
                                <label>Макс. цена:</label>
                                <input type="number" id="priceMaxFilter" class="form-control" placeholder="Макс. цена">
                            </div>
                            <div class="col-md-3">
                                <button class="btn btn-primary mt-4" onclick="applyFilters()">Применить</button>
                                <button class="btn btn-secondary mt-4 ms-2" onclick="resetFilters()">Сбросить</button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Таблица -->
        <div class="row">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5 class="mb-0">Данные продавцов (<span id="recordsCount">0</span>)</h5>
                    </div>
                    <div class="card-body">
                        <div class="table-responsive">
                            <table id="sellersTable" class="table table-striped table-hover">
                                <thead>
                                    <tr>
                                        <th>ID</th>
                                        <th>Продавец</th>
                                        <th>Торговая марка</th>
                                        <th>ИНН</th>
                                        <th>Топ категория</th>
                                        <th>Цена мин</th>
                                        <th>Цена средн</th>
                                        <th>Цена макс</th>
                                        <th>Рейтинг</th>
                                        <th>Отзывы</th>
                                        <th>Скидка %</th>
                                        <th>Действия</th>
                                    </tr>
                                </thead>
                                <tbody id="sellersTableBody">
                                    <!-- Данные будут заполнены JavaScript -->
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <!-- Скрипты -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.7/js/dataTables.bootstrap5.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>
    
    <script>
        // Данные
        let sellersData = {data_json};
        let statistics = {stats_json};
        let sellersTable;
        let filteredData = [...sellersData];
        
        // Инициализация при загрузке страницы
        $(document).ready(function() {{
            console.log('Загружено продавцов:', sellersData.length);
            console.log('Статистика:', statistics);
            
            initializeStatistics();
            initializeDataTable();
            updateRecordsCount();
        }});
        
        function initializeStatistics() {{
            const statsContainer = document.getElementById('statisticsCards');
            const cardClasses = ['info', 'success', 'warning', 'danger'];
            const icons = ['users', 'ruble-sign', 'star', 'comments'];
            
            const statCards = [
                {{ label: 'Всего продавцов', value: statistics.total_sellers || 0 }},
                {{ label: 'Средняя цена', value: (statistics.avg_price || 0) + ' ₽' }},
                {{ label: 'Средний рейтинг', value: (statistics.avg_rating || 0) }},
                {{ label: 'Всего отзывов', value: (statistics.total_feedbacks || 0).toLocaleString() }}
            ];
            
            let cardsHTML = '';
            statCards.forEach((stat, index) => {{
                cardsHTML += `
                    <div class="col-md-3 col-sm-6">
                        <div class="stat-card ${{cardClasses[index]}}">
                            <div class="d-flex justify-content-between align-items-center">
                                <div>
                                    <div class="stat-value">${{stat.value}}</div>
                                    <div class="stat-label">${{stat.label}}</div>
                                </div>
                                <div>
                                    <i class="fas fa-${{icons[index]}} fa-2x opacity-50"></i>
                                </div>
                            </div>
                        </div>
                    </div>
                `;
            }});
            
            statsContainer.innerHTML = cardsHTML;
        }}
        
        function initializeDataTable() {{
            const tableBody = document.getElementById('sellersTableBody');
            let html = '';
            
            sellersData.forEach(seller => {{
                html += `
                    <tr>
                        <td>${{seller.ID}}</td>
                        <td>${{seller['Продавец'] || '-'}}</td>
                        <td>${{seller['Торговая марка'] || '-'}}</td>
                        <td>${{seller['ИНН'] || '-'}}</td>
                        <td>${{seller['Топ категория 1'] || '-'}}</td>
                        <td>${{seller['Цена мин'] ? seller['Цена мин'].toLocaleString() + ' ₽' : '-'}}</td>
                        <td>${{seller['Цена средн'] ? seller['Цена средн'].toLocaleString() + ' ₽' : '-'}}</td>
                        <td>${{seller['Цена макс'] ? seller['Цена макс'].toLocaleString() + ' ₽' : '-'}}</td>
                        <td>${{seller['Ср. рейтинг товаров'] ? seller['Ср. рейтинг товаров'] + ' ⭐' : '-'}}</td>
                        <td>${{seller['Сумма отзывов товаров'] ? seller['Сумма отзывов товаров'].toLocaleString() : '-'}}</td>
                        <td>${{seller['Ср. скидка %'] ? seller['Ср. скидка %'] + '%' : '-'}}</td>
                        <td>
                            <a href="${{seller['Ссылка']}}" target="_blank" class="btn btn-sm btn-outline-primary">
                                <i class="fas fa-external-link-alt"></i> WB
                            </a>
                        </td>
                    </tr>
                `;
            }});
            
            tableBody.innerHTML = html;
            
            // Инициализируем DataTable
            sellersTable = $('#sellersTable').DataTable({{
                pageLength: 25,
                responsive: true,
                language: {{
                    search: "Поиск:",
                    lengthMenu: "Показать _MENU_ записей",
                    info: "Показано _START_ до _END_ из _TOTAL_ записей",
                    infoEmpty: "Показано 0 до 0 из 0 записей",
                    paginate: {{
                        first: "Первая",
                        last: "Последняя",
                        next: "Следующая",
                        previous: "Предыдущая"
                    }},
                    emptyTable: "Нет данных в таблице",
                    zeroRecords: "Записи отсутствуют."
                }}
            }});
        }}
        
        function updateRecordsCount() {{
            document.getElementById('recordsCount').textContent = filteredData.length;
        }}
        
        function applyFilters() {{
            const searchTerm = document.getElementById('searchFilter').value.toLowerCase();
            const priceMin = parseFloat(document.getElementById('priceMinFilter').value) || 0;
            const priceMax = parseFloat(document.getElementById('priceMaxFilter').value) || Infinity;
            
            filteredData = sellersData.filter(seller => {{
                const nameMatch = !searchTerm || 
                    (seller['Продавец'] && seller['Продавец'].toLowerCase().includes(searchTerm)) ||
                    (seller['Торговая марка'] && seller['Торговая марка'].toLowerCase().includes(searchTerm));
                
                const priceMatch = (!seller['Цена средн'] || 
                    (seller['Цена средн'] >= priceMin && seller['Цена средн'] <= priceMax));
                
                return nameMatch && priceMatch;
            }});
            
            updateTable();
            updateRecordsCount();
        }}
        
        function resetFilters() {{
            document.getElementById('searchFilter').value = '';
            document.getElementById('priceMinFilter').value = '';
            document.getElementById('priceMaxFilter').value = '';
            
            filteredData = [...sellersData];
            updateTable();
            updateRecordsCount();
        }}
        
        function updateTable() {{
            sellersTable.clear();
            
            filteredData.forEach(seller => {{
                sellersTable.row.add([
                    seller.ID,
                    seller['Продавец'] || '-',
                    seller['Торговая марка'] || '-',
                    seller['ИНН'] || '-',
                    seller['Топ категория 1'] || '-',
                    seller['Цена мин'] ? seller['Цена мин'].toLocaleString() + ' ₽' : '-',
                    seller['Цена средн'] ? seller['Цена средн'].toLocaleString() + ' ₽' : '-',
                    seller['Цена макс'] ? seller['Цена макс'].toLocaleString() + ' ₽' : '-',
                    seller['Ср. рейтинг товаров'] ? seller['Ср. рейтинг товаров'] + ' ⭐' : '-',
                    seller['Сумма отзывов товаров'] ? seller['Сумма отзывов товаров'].toLocaleString() : '-',
                    seller['Ср. скидка %'] ? seller['Ср. скидка %'] + '%' : '-',
                    `<a href="${{seller['Ссылка']}}" target="_blank" class="btn btn-sm btn-outline-primary">
                        <i class="fas fa-external-link-alt"></i> WB
                    </a>`
                ]);
            }});
            
            sellersTable.draw();
        }}
        
        function exportToCSV() {{
            const csv = convertToCSV(filteredData);
            const filename = `wb_sellers_${{new Date().toISOString().split('T')[0]}}.csv`;
            downloadFile(csv, filename, 'text/csv');
        }}
        
        function exportToExcel() {{
            const ws = XLSX.utils.json_to_sheet(filteredData);
            const wb = XLSX.utils.book_new();
            XLSX.utils.book_append_sheet(wb, ws, 'Продавцы');
            const filename = `wb_sellers_${{new Date().toISOString().split('T')[0]}}.xlsx`;
            XLSX.writeFile(wb, filename);
        }}
        
        function convertToCSV(objArray) {{
            const array = typeof objArray !== 'object' ? JSON.parse(objArray) : objArray;
            let str = '';
            
            // Заголовки
            let line = '';
            for (let index in array[0]) {{
                if (line !== '') line += ';';
                line += index;
            }}
            str += line + '\\r\\n';
            
            // Данные
            for (let i = 0; i < array.length; i++) {{
                let line = '';
                for (let index in array[i]) {{
                    if (line !== '') line += ';';
                    line += array[i][index] || '';
                }}
                str += line + '\\r\\n';
            }}
            
            return str;
        }}
        
        function downloadFile(content, fileName, contentType) {{
            const a = document.createElement('a');
            const file = new Blob([content], {{ type: contentType }});
            a.href = URL.createObjectURL(file);
            a.download = fileName;
            a.click();
        }}
    </script>
</body>
</html>"""
        
        # Записываем в файл
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return output_path
    
    def open_report_in_browser(self, output_path: str):
        """Открывает отчёт в браузере."""
        webbrowser.open(f'file://{os.path.abspath(output_path)}')
    
    def _calculate_statistics(self) -> Dict[str, Any]:
        """Рассчитывает статистику из данных."""
        if not self.data:
            return {}
        
        df = pd.DataFrame(self.data)
        
        stats = {
            'total_sellers': len(df),
            'avg_price': round(df['Цена средн'].replace([None, 0], pd.NA).mean(), 2) if 'Цена средн' in df.columns else 0,
            'avg_rating': round(df['Ср. рейтинг товаров'].replace([None, 0], pd.NA).mean(), 2) if 'Ср. рейтинг товаров' in df.columns else 0,
            'total_feedbacks': int(df['Сумма отзывов товаров'].replace([None, 0], pd.NA).sum()) if 'Сумма отзывов товаров' in df.columns else 0,
        }
        
        return stats