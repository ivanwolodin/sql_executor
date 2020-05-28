from PyQt5 import uic
from PyQt5.QtCore import QTimer

form_2, base_2 = uic.loadUiType('dialog.ui')


class DialogWindow(base_2, form_2):
    """
        lineEditUser
        lineEditPass
        lineEditHost
        lineEditPort
        lineEditDb
        lineEditResultConnection

    """

    def __init__(self, database_obj, selected_db_type):
        super(base_2, self).__init__()
        self.setupUi(self)
        self.pushButton.pressed.connect(self._get_credentials)

        self.selected_db_type = selected_db_type

        self.user = ''
        self.host = ''
        self.port = ''
        self.database = ''
        self.password = ''

        self._database_obj = database_obj

    def _get_credentials(self):
        self.user = self.lineEditUser.text()
        self.password = self.lineEditPass.text()
        self.host = self.lineEditHost.text()
        self.port = self.lineEditPort.text()
        self.database = self.lineEditDb.text()

        if not all([self.user,
                    self.password,
                    self.host,
                    self.port,
                    self.database]):
            self.lineEditResultConnection.setText('Fill empty fields!')
            return

        if self._change_database():
            self.lineEditResultConnection.setText(
                'Connected to {}'.format(self.host))
            # in case of successful connect close the window in 30 secs
            QTimer.singleShot(10000, self.close)
        else:
            self.lineEditResultConnection.setText(
                'Cant connect to {}'.format(self.host))

    def _change_database(self):
        return self._database_obj.capture_database(
            database_type=self.selected_db_type,
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
