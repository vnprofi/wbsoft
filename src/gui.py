from PyQt6.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QFileDialog,
    QComboBox,
    QProgressBar,
    QMessageBox,
)
from PyQt6.QtCore import Qt, QUrl
from PyQt6.QtGui import QDesktopServices
import sys
import re
import pathlib
from typing import List, Dict

from qasync import QEventLoop, asyncSlot
import asyncio

try:
    # when packaged as a standalone executable core will be importable as top-level module
    from core import export_data, get_sellers_data_sync
    from html_report import SimpleHTMLReportGenerator
except ImportError:
    # fallback to relative import when running from source tree
    from .core import export_data, get_sellers_data_sync
    from .html_report import SimpleHTMLReportGenerator


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("WB Seller Parser")
        self.resize(540, 280)
        self._build_ui()

        self.total_ids = 0
        self.last_data = []  # Сохраняем последние обработанные данные для HTML отчёта

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # INPUT
        in_row = QHBoxLayout()
        self.input_edit = QLineEdit()
        self.input_edit.setReadOnly(True)
        btn_in = QPushButton("Выбрать файл с ID…")
        btn_in.clicked.connect(self.choose_input)
        in_row.addWidget(QLabel("Файл ID:"))
        in_row.addWidget(self.input_edit, 1)
        in_row.addWidget(btn_in)
        layout.addLayout(in_row)

        # OUTPUT
        out_row = QHBoxLayout()
        self.output_edit = QLineEdit()
        self.output_edit.setReadOnly(True)
        btn_out = QPushButton("Куда сохранить…")
        btn_out.clicked.connect(self.choose_output)
        self.format_combo = QComboBox()
        self.format_combo.addItems(["CSV", "Excel (XLSX)"])
        out_row.addWidget(QLabel("Выходной файл:"))
        out_row.addWidget(self.output_edit, 1)
        out_row.addWidget(btn_out)
        out_row.addWidget(self.format_combo)
        layout.addLayout(out_row)

        # Progress
        self.progress = QProgressBar()
        self.progress.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.progress)

        # Buttons row
        btn_row = QHBoxLayout()
        self.start_btn = QPushButton("Старт")
        self.start_btn.clicked.connect(self.on_start)
        self.html_report_btn = QPushButton("📊HTML Отчёт")
        self.html_report_btn.clicked.connect(self.on_html_report)
        self.html_report_btn.setEnabled(False)  # Начально отключена
        suggest_btn = QPushButton("Предложить улучшение")
        suggest_btn.clicked.connect(lambda: QDesktopServices.openUrl(QUrl("https://t.me/Userspoi")))
        btn_row.addStretch(1)
        btn_row.addWidget(self.start_btn)
        btn_row.addWidget(self.html_report_btn)
        btn_row.addWidget(suggest_btn)
        layout.addLayout(btn_row)

    # ----------------------- UI helpers ------------------------
    def choose_input(self):
        path, _ = QFileDialog.getOpenFileName(self, "Выберите файл с ID", "", "Text files (*.txt);;CSV (*.csv);;All files (*)")
        if path:
            self.input_edit.setText(path)

    def choose_output(self):
        default_ext = "csv" if self.format_combo.currentIndex() == 0 else "xlsx"
        path, _ = QFileDialog.getSaveFileName(self, "Сохранить файл", f"result.{default_ext}", "CSV (*.csv);;Excel (*.xlsx)")
        if path:
            self.output_edit.setText(path)

    def parse_ids(self, file_path: str) -> List[int]:
        ids: List[int] = []
        try:
            with open(file_path, encoding="utf-8-sig") as f:
                for line in f:
                    m = re.search(r"\d{3,}", line)
                    if m:
                        ids.append(int(m.group()))
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось прочитать файл ID:\n{e}")
        return ids

    # ----------------------- Actions ---------------------------
    @asyncSlot()
    async def on_start(self):
        input_path = self.input_edit.text().strip()
        output_path = self.output_edit.text().strip()
        if not input_path or not pathlib.Path(input_path).exists():
            QMessageBox.warning(self, "Не выбран входной файл", "Укажите корректный файл с ID.")
            return
        if not output_path:
            QMessageBox.warning(self, "Не выбран выходной файл", "Укажите путь для сохранения результата.")
            return
        # Ensure extension matches format selection
        fmt_idx = self.format_combo.currentIndex()
        if fmt_idx == 0 and not output_path.lower().endswith(".csv"):
            output_path += ".csv"
        if fmt_idx == 1 and not output_path.lower().endswith(".xlsx"):
            output_path += ".xlsx"
        seller_ids = self.parse_ids(input_path)
        if not seller_ids:
            QMessageBox.information(self, "Нет ID", "Не найдено ни одного ID продавца в выбранном файле.")
            return

        self.total_ids = len(seller_ids)
        self.progress.setValue(0)
        self.start_btn.setEnabled(False)

        def progress_cb(done: int, total: int):
            percent = int(done / total * 100)
            self.progress.setValue(percent)

        try:
            await export_data(seller_ids, output_path, progress_cb)
            
            # Получаем данные для HTML отчёта
            self.last_data = await self._get_data_for_html(seller_ids)
            
            QMessageBox.information(self, "Готово", f"Файл успешно сохранён:\n{output_path}\n\nТеперь вы можете создать HTML отчёт для просмотра данных!")
            
            # Включаем кнопку HTML отчёта
            self.html_report_btn.setEnabled(True)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Во время обработки произошла ошибка:\n{e}")
        finally:
            self.start_btn.setEnabled(True)
            self.progress.setValue(0)
    
    async def _get_data_for_html(self, seller_ids: List[int]) -> List[Dict]:
        """Получает данные для HTML отчёта."""
        def progress_cb(done: int, total: int):
            percent = int(done / total * 100)
            self.progress.setValue(percent)
        
        try:
            # Используем новую функцию для получения данных в виде словарей
            from core import get_sellers_data
            return await get_sellers_data(seller_ids, progress_cb)
        except Exception as e:
            print(f"Error getting data for HTML: {e}")
            return []
    
    def on_html_report(self):
        """Обработчик кнопки HTML отчёта."""
        if not self.last_data:
            QMessageBox.warning(self, "Нет данных", "Сначала выполните парсинг данных.")
            return
        
        try:
            # Создаём HTML отчёт
            report_generator = SimpleHTMLReportGenerator()
            report_generator.set_data(self.last_data)
            
            # Генерируем отчёт
            report_path = report_generator.generate_report()
            
            # Открываем в браузере
            report_generator.open_report_in_browser(report_path)
            
            QMessageBox.information(
                self, 
                "Отчёт создан", 
                f"HTML отчёт создан и открыт в браузере!\n\nПуть: {report_path}\n\nВ отчёте вы можете:\n• Фильтровать данные\n• Сравнивать продавцов\n• Экспортировать в CSV/Excel/PDF"
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при создании HTML отчёта:\n{e}")


# ----------------------- Entry point -------------------------

def main():
    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)
    window = MainWindow()
    window.show()
    with loop:
        loop.run_forever()


if __name__ == "__main__":
    main()