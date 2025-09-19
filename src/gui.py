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
from typing import List

from qasync import QEventLoop, asyncSlot
import asyncio

try:
    # when packaged as a standalone executable core will be importable as top-level module
    from core import export_data, export_html_report
except ImportError:
    # fallback to relative import when running from source tree
    from .core import export_data, export_html_report


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("WB Seller Parser")
        self.resize(540, 280)
        self._build_ui()

        self.total_ids = 0

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
        self.format_combo.addItems(["CSV", "Excel (XLSX)", "HTML отчет"])
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
        suggest_btn = QPushButton("Предложить улучшение")
        suggest_btn.clicked.connect(lambda: QDesktopServices.openUrl(QUrl("https://t.me/Userspoi")))
        btn_row.addStretch(1)
        btn_row.addWidget(self.start_btn)
        btn_row.addWidget(suggest_btn)
        layout.addLayout(btn_row)

    # ----------------------- UI helpers ------------------------
    def choose_input(self):
        path, _ = QFileDialog.getOpenFileName(self, "Выберите файл с ID", "", "Text files (*.txt);;CSV (*.csv);;All files (*)")
        if path:
            self.input_edit.setText(path)

    def choose_output(self):
        idx = self.format_combo.currentIndex()
        default_ext = "csv" if idx == 0 else ("xlsx" if idx == 1 else "html")
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
        if fmt_idx == 2 and not output_path.lower().endswith(".html"):
            output_path += ".html"
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
            if fmt_idx == 2:
                html_path = await export_html_report(seller_ids, output_path, progress_cb)
                QMessageBox.information(self, "Готово", f"HTML отчёт сохранён:\n{html_path}\n\nРядом лежат ссылки для CSV/Excel.")
                # auto-open in default browser
                QDesktopServices.openUrl(QUrl.fromLocalFile(html_path))
            else:
                await export_data(seller_ids, output_path, progress_cb)
                QMessageBox.information(self, "Готово", f"Файл успешно сохранён:\n{output_path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Во время обработки произошла ошибка:\n{e}")
        finally:
            self.start_btn.setEnabled(True)
            self.progress.setValue(0)


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