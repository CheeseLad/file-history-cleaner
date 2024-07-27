import sys
import sqlite3
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QTableWidget, QTableWidgetItem

class App(QWidget):
    def __init__(self):
        super().__init__()
        self.title = 'SQLite Data Viewer'
        self.left = 100
        self.top = 100
        self.width = 640
        self.height = 400

        self.initUI()

    def initUI(self):
        self.setWindowTitle(self.title)
        self.setGeometry(self.left, self.top, self.width, self.height)

        self.layout = QVBoxLayout()

        self.button = QPushButton('Load Data', self)
        self.button.clicked.connect(self.loadData)
        self.layout.addWidget(self.button)

        self.tableWidget = QTableWidget()
        self.layout.addWidget(self.tableWidget)

        self.setLayout(self.layout)
        self.show()

    def loadData(self):
        conn = sqlite3.connect('files2.db')
        cursor = conn.cursor()
        cursor.execute("SELECT folder, file_path, file_name, date_string, extension, duplicate_count, keep FROM files")

        rows = cursor.fetchall()
        self.tableWidget.setRowCount(len(rows))
        self.tableWidget.setColumnCount(7)
        self.tableWidget.setHorizontalHeaderLabels(['Folder', 'File Path', 'Filename', 'Date String', 'Extension', 'Duplicate Count', 'Keep'])

        for i, row in enumerate(rows):
            for j, val in enumerate(row):
                self.tableWidget.setItem(i, j, QTableWidgetItem(str(val)))

        conn.close()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = App()
    sys.exit(app.exec_())
