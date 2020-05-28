import sys

from PyQt5 import uic
from PyQt5.QtWidgets import QApplication, QMainWindow
from PyQt5.QtCore import Qt, QAbstractTableModel

from db_connection import DataBase
from db_changer_dialog import DialogWindow
from logger import logger


class TableModel(QAbstractTableModel):
    def __init__(self, data):
        super(TableModel, self).__init__()
        self._data = data

    def data(self, index, role):
        if role == Qt.DisplayRole:
            value = self._data.iloc[index.row(), index.column()]
            return str(value)

    def rowCount(self, index):
        return self._data.shape[0]

    def columnCount(self, index):
        return self._data.shape[1]

    def headerData(self, section, orientation, role):
        # section is the index of the column/row.
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return str(self._data.columns[section])

            if orientation == Qt.Vertical:
                return str(self._data.index[section])


class MyWindow(QMainWindow):
    """
    Qt Widgets:

    label
    comboBox
    label_2
    tableViewSqlResult
    label_3
    lineEditSqlRequest
    pushButton

    """

    def __init__(self, parent=None):

        QMainWindow.__init__(self)
        uic.loadUi('mainwindow.ui', self)
        logger.info('App has started')
        self.data_base_obj = DataBase()
        self.pushButton.pressed.connect(self.execute_sql_request)
        self.comboBox.currentIndexChanged[str].connect(self.change_db)

    def execute_sql_request(self) -> None:

        sql_request = self.lineEditSqlRequest.text()

        data = self.data_base_obj.execute_sql(sql_statement=sql_request)
        self.fill_table_view(data=data)

    def fill_table_view(self, data=None, status=None):
        if status is not None and data is None:
            data = [[status]]
        elif data is not None:
            data = data
        else:
            logger.error('Invalid data in table_view')
            raise Exception('Specify data')
        model = TableModel(data)
        self.tableViewSqlResult.setModel(model)

    def change_db(self):
        db_type = self.comboBox.itemText(self.comboBox.currentIndex())
        if db_type != ':memory:':
            self.dialog = DialogWindow(
                database_obj=self.data_base_obj,
                selected_db_type=db_type)
            self.dialog.show()
            return
        self.data_base_obj.capture_database(database_type=db_type)
        self.data_base_obj.create_db_in_memory()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MyWindow()
    window.show()
    sys.exit(app.exec_())
