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
            # See below for the nested-list data structure.
            # .row() indexes into the outer list,
            # .column() indexes into the sub-list
            return self._data[index.row()][index.column()]

    def rowCount(self, index):
        # The length of the outer list.
        return len(self._data)

    def columnCount(self, index):
        # The following takes the first sub-list, and returns
        # the length (only works if all rows are an equal length)
        return len(self._data[0])


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
        uic.loadUi("mainwindow.ui", self)
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
            self.dialog = DialogWindow(database_obj=self.data_base_obj, selected_db_type=db_type)
            self.dialog.show()
            return
        self.data_base_obj.capture_database(database_type=db_type)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MyWindow()
    window.show()
    sys.exit(app.exec_())
