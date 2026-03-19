import sys
import os
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QLabel, QLineEdit, QPushButton, QComboBox, QFileDialog, QMessageBox, QTextEdit, QGridLayout, QGroupBox, QDesktopWidget

class DBApp(QWidget):
    def __init__(self):
        super().__init__()
        self.have_file = False
        self.lst_fileid = {}
        self.is_need_fileID = False
        self.initUI()

    def center(self):
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
    
    def initUI(self):
        self.setWindowTitle('DB Export Tool')
        self.setGeometry(100, 50, 400, 500)
        
        self.center()

        layout = QGridLayout()

        # Group Box A
        group_box_a = QGroupBox("")
        group_layout_a = QGridLayout()

        self.system_id_label = QLabel('Old System ID:')
        group_layout_a.addWidget(self.system_id_label, 0, 0)
        self.system_id_input = QLineEdit()
        self.system_id_input.textChanged.connect(self.check_inputs)
        group_layout_a.addWidget(self.system_id_input, 0, 1)
        
        self.new_system_id_label = QLabel('New System ID:')
        group_layout_a.addWidget(self.new_system_id_label, 1, 0)
        self.new_system_id_input = QLineEdit()
        self.new_system_id_input.textChanged.connect(self.check_inputs)
        group_layout_a.addWidget(self.new_system_id_input, 1, 1)
        
        self.current_max_file_id_label = QLabel('Current Max File ID:')
        group_layout_a.addWidget(self.current_max_file_id_label, 2, 0)
        self.current_max_file_id_input = QLineEdit()
        self.current_max_file_id_input.setEnabled(False)
        group_layout_a.addWidget(self.current_max_file_id_input, 2, 1)
        
        self.table_list_label = QLabel('Tables with matching System ID')
        group_layout_a.addWidget(self.table_list_label, 3, 0, 1, 2)
        self.table_list_display = QTextEdit()
        self.table_list_display.setReadOnly(True)
        group_layout_a.addWidget(self.table_list_display, 4, 0, 1, 3)
        
        self.directory_label = QLabel('Save Directory:')
        group_layout_a.addWidget(self.directory_label, 6, 0)
        self.directory_input = QLineEdit()
        group_layout_a.addWidget(self.directory_input, 6, 1)
        
        self.browse_button = QPushButton('Browse')
        self.browse_button.clicked.connect(self.browse_directory)
        group_layout_a.addWidget(self.browse_button, 6, 2)
        
        
        group_box_a.setLayout(group_layout_a)
        layout.addWidget(group_box_a, 0, 0, 1, 4)
        
        
        self.connect_button = QPushButton('Connect to Database')
        self.connect_button.clicked.connect(self.connect_to_db)
        self.connect_button.setEnabled(False)
        layout.addWidget(self.connect_button, 1, 1, 1, 2)
        
        self.export_button_multi = QPushButton('Export Data multi files')
        self.export_button_multi.clicked.connect(self.export_data_multi)
        self.export_button_multi.setEnabled(False)
        layout.addWidget(self.export_button_multi, 2, 1, 1, 2)
        
        self.export_button_single = QPushButton('Export Data single file')
        self.export_button_single.clicked.connect(self.export_data_single)
        self.export_button_single.setEnabled(False)
        layout.addWidget(self.export_button_single, 3, 1, 1, 2)
        
        self.setLayout(layout)

    def check_inputs(self):
        if self.system_id_input.text().strip() and self.new_system_id_input.text().strip():
            self.connect_button.setEnabled(True)
        else:
            self.connect_button.setEnabled(False)
        
    def browse_directory(self):
        options = QFileDialog.Options()
        directory = QFileDialog.getExistingDirectory(self, "Select Directory", options=options)
        if directory:
            self.directory_input.setText(directory)

    def connect_to_db(self):
        old_system_id = self.system_id_input.text()
        if not old_system_id:
            QMessageBox.warning(self, 'Input Required', 'Please enter the Old System ID before connecting to the database.')
            return

        try:
            # Đọc danh sách bảng từ file table.txt với mã hóa UTF-8
            script_dir = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(script_dir, 'table_logic.txt')
            with open('table_logic.txt', 'r', encoding='utf-8') as file:
                table_names = [line.strip() for line in file.readlines()]

            matching_tables = []

            # Kết nối đến cơ sở dữ liệu
            #conn_str = os.path.join(script_dir, 'data_string.txt')
            with open('data_string.txt', 'r', encoding='utf-8') as file:
                conn_str = file.read().strip()
            self.conn = pyodbc.connect(conn_str)
            
            with open('file_string.txt', 'r', encoding='utf-8') as file:
                conn_str_file = file.read().strip()
            self.conn_file = pyodbc.connect(conn_str_file)
            
            cursor = self.conn.cursor()
            
            for table_name in table_names:
                query = f"SELECT COUNT(*) FROM {table_name} WHERE SYSTEM_ID = ?"
                cursor.execute(query, old_system_id)
                count = cursor.fetchone()[0]
                if count > 0:
                    matching_tables.append(table_name)

            matching_tables = sorted(matching_tables)  # Sắp xếp tên các bảng theo thứ tự tăng dần
            self.table_list_display.clear()
            if not matching_tables:
                QMessageBox.warning(self, 'No Data', 'No data found for the given System ID in any table.')
            else:
                for table_name in matching_tables:
                    self.table_list_display.append(table_name)

                if 'T_FILE_LINK_KIHON_PJ_GAMEN' in matching_tables:
                    self.is_need_fileID = True
                    self.current_max_file_id_input.setEnabled(True)
                else:
                    self.current_max_file_id_input.setEnabled(False)
        
                self.export_button_multi.setEnabled(True)
                self.export_button_single.setEnabled(True)
                QMessageBox.information(self, 'Success', 'Loaded matching table names successfully!')
        except Exception as e:
            QMessageBox.critical(self, 'Error', f'Failed to load table names: {e}')        
    
    def export_data_file(self):
        file_id_keys = list(self.lst_fileid.keys())
        query = f"SELECT * FROM T_FILE_DATA WHERE FILE_ID IN ({', '.join(['?' for _ in file_id_keys])})"
        df = pd.read_sql(query, self.conn_file, params=file_id_keys)

        if not df.empty:
            current_max_file_id = int(self.current_max_file_id_input.text()) if self.current_max_file_id_input.text().strip() else 0
            insert_queries = []
            for index, row in df.iterrows():
                values = []
                columns = []
                for col, value in row.items():
                    if col == 'TIME_STAMP':
                        continue  # Bỏ qua cột TIME_STAMP
                    columns.append(col)
                    if pd.isnull(value):
                        values.append('NULL')
                    elif isinstance(value, bool):  # Kiểm tra kiểu bool cho cột kiểu bit
                        values.append('1' if value else '0')
                    elif col == 'FILE_ID':
                        current_max_file_id += 1
                        values.append(f"'{self.lst_fileid.get(value)}'")
                    elif isinstance(value, (int, float)):
                        values.append(f"'{int(value)}'")
                    elif isinstance(value, pd.Timestamp):
                        values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                    elif isinstance(value, str):
                        values.append(f"N'{value}'")
                    elif isinstance(value, bytes):
                        values.append(f"0x{value.hex()}")
                    
                    else:
                        values.append(f"'{str(value)}'")
                query = f"INSERT INTO T_FILE_DATA ({', '.join(columns)}) VALUES ({', '.join(values)});"
                insert_queries.append(query)
                        
            file_path = os.path.join(self.save_directory, "T_FILE_DATA.sql")
            with open(file_path, 'w', encoding='utf-8') as file:
                for query in insert_queries:
                    file.write(query + '\n')        
            
            query = f"SELECT * FROM S_NUMBER_FILE"
            df = pd.read_sql(query, self.conn_file)
                
            set_clause = []
            insert_queries = []
            for col, value in df.iloc[0].items():
                if col == 'CREATE_USER' or col == 'CREATE_DATE' or col == 'CREATE_PC' or col == 'TIME_STAMP' or col == 'DELETE_FLG':
                    continue
                if isinstance(value, str):
                    set_clause.append(f"{col} = N'{value}'")
                elif ('ID' in col or 'KBN' in col or 'CD' in col) and isinstance(value, (int, float)):
                    set_clause.append(f"{col} = {int(value)}")
                elif col == 'CURRENT_NUMBER':
                    set_clause.append(f"{col} = {current_max_file_id}")
                elif pd.isnull(value):
                    set_clause.append(f"{col} = NULL")
                elif isinstance(value, pd.Timestamp):
                    set_clause.append(f"{col} = '{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                else:
                    set_clause.append(f"{col} = {repr(value)}")
            update_query = f"UPDATE S_NUMBER_FILE SET {', '.join(set_clause)};"
            insert_queries.append(update_query)        
            
            file_path = os.path.join(self.save_directory, "S_NUMBER_FILE.sql")
            with open(file_path, 'w', encoding='utf-8') as file:
                for query in insert_queries:
                    file.write(query + '\n')  

    def export_data_multi(self):
        old_system_id = self.system_id_input.text()
        new_system_id = self.new_system_id_input.text()
        self.save_directory = self.directory_input.text()
        if self.is_need_fileID:
            if not self.current_max_file_id_input.text():
                QMessageBox.warning(self, 'Input Required', 'Please enter current max file ID.')
                return
            else:
                current_max_file_id = int(self.current_max_file_id_input.text())
        

        if not old_system_id or not new_system_id:
            QMessageBox.warning(self, 'Input Required', 'Please enter both Old System ID and New System ID.')
            return

        if not self.save_directory:
            QMessageBox.warning(self, 'Input Required', 'Please select a directory to save the SQL files.')
            return

        try:
            matching_tables = self.table_list_display.toPlainText().split('\n')
            if not matching_tables:
                QMessageBox.warning(self, 'No Tables', 'No tables found to export.')
                return

            columns_to_convert = ['ZENKAKU_MOJI_SU', 'HANKAKU_MOJI_SU', 'SEISU_KETA', 'SYOUSU_KETA']

            for table_name in matching_tables:
                insert_queries = []

                if not table_name.strip():
                    continue

                query = f"SELECT * FROM {table_name} WHERE SYSTEM_ID = ?"
                df = pd.read_sql(query, self.conn, params=[old_system_id])

                if df.empty:
                    QMessageBox.warning(self, 'No Data', f'No data found for the given System ID in table {table_name}.')
                    continue

                if table_name == 'T_KIHON_PJ':
                    set_clause = []
                    for col, value in df.iloc[0].items():
                        if col == 'SYSTEM_ID' or col == 'TIME_STAMP' or col == 'DELETE_FLG' or col == 'KOUSHIN_FUKA_FLG':
                            continue
                        if isinstance(value, str):
                            set_clause.append(f"{col} = N'{value}'")
                        elif ('ID' in col or 'KBN' in col or 'CD' in col or 'SEQ' in col) and isinstance(value, (int, float)):
                            set_clause.append(f"{col} = {int(value)}")
                        elif pd.isnull(value):
                            set_clause.append(f"{col} = NULL")
                        elif isinstance(value, pd.Timestamp):
                            set_clause.append(f"{col} = '{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                        else:
                            set_clause.append(f"{col} = {repr(value)}")
                    update_query = f"UPDATE {table_name} SET {', '.join(set_clause)} WHERE SYSTEM_ID = '{new_system_id}';"
                    insert_queries.append(update_query)
                else:
                    delete_query = f"DELETE FROM {table_name} WHERE SYSTEM_ID = '{new_system_id}';"
                    insert_queries.append(delete_query)

                    for index, row in df.iterrows():
                        values = []
                        columns = []
                        for col, value in row.items():
                            if col == 'TIME_STAMP':
                                continue  # Bỏ qua cột TIME_STAMP
                            columns.append(col)
                            if pd.isnull(value):
                                values.append('NULL')
                            elif col == 'SYSTEM_ID':
                                values.append(f"'{new_system_id}'")
                            elif table_name == 'T_FILE_LINK_KIHON_PJ_GAMEN' and col == 'FILE_ID':
                                self.have_file = True
                                current_max_file_id += 1
                                self.lst_fileid[value] = current_max_file_id
                                values.append(f"'{current_max_file_id}'")
                            elif table_name == 'T_FILE_LINK_KIHON_PJ_GAMEN' and isinstance(value, pd.Timestamp):
                                values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                            elif ('ID' in col or 'KBN' in col or 'CD' in col or 'SEQ' in col) and isinstance(value, (int, float)):
                                values.append(f"'{int(value)}'")
                            elif col in columns_to_convert and isinstance(value, (int, float)):
                                values.append(f"'{int(value)}'")
                            elif isinstance(value, str):
                                values.append(f"N'{value}'")
                            else:
                                values.append(f"'{str(value)}'")
                        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
                        insert_queries.append(query)

                file_path = os.path.join(self.save_directory, f"{table_name}.sql")
                with open(file_path, 'w', encoding='utf-8') as file:
                    for query in insert_queries:
                        file.write(query + '\n')
                
            if self.have_file:
                self.export_data_file()  
                
            QMessageBox.information(self, 'Success', 'INSERT and UPDATE statements have been generated and saved to a single file.')
            
            self.is_need_fileID = False
            self.export_button_multi.setEnabled(False)
            self.export_button_single.setEnabled(False)
            self.table_list_display.clear()
            self.system_id_input.setText("")
            self.new_system_id_input.setText("")
            self.current_max_file_id_input.setText("")
        except Exception as e:
            QMessageBox.critical(self, 'Error', f'An error occurred: {str(e)}')
                        
    def export_data_single(self):
        old_system_id = self.system_id_input.text()
        new_system_id = self.new_system_id_input.text()
        self.save_directory = self.directory_input.text()
        if self.is_need_fileID:
            if not self.current_max_file_id_input.text():
                QMessageBox.warning(self, 'Input Required', 'Please enter current max file ID.')
                return
            else:
                current_max_file_id = int(self.current_max_file_id_input.text())

        if not old_system_id or not new_system_id:
            QMessageBox.warning(self, 'Input Required', 'Please enter both Old System ID and New System ID.')
            return

        if not self.save_directory:
            QMessageBox.warning(self, 'Input Required', 'Please select a directory to save the SQL files.')
            return

        try:
            matching_tables = self.table_list_display.toPlainText().split('\n')
            if not matching_tables:
                QMessageBox.warning(self, 'No Tables', 'No tables found to export.')
                return

            columns_to_convert = ['ZENKAKU_MOJI_SU', 'HANKAKU_MOJI_SU', 'SEISU_KETA', 'SYOUSU_KETA']
            all_queries = []

            for table_name in matching_tables:
                insert_queries = []

                if not table_name.strip():
                    continue

                query = f"SELECT * FROM {table_name} WHERE SYSTEM_ID = ?"
                df = pd.read_sql(query, self.conn, params=[old_system_id])

                if df.empty:
                    QMessageBox.warning(self, 'No Data', f'No data found for the given System ID in table {table_name}.')
                    continue

                if table_name == 'T_KIHON_PJ':
                    set_clause = []
                    for col, value in df.iloc[0].items():
                        if col == 'SYSTEM_ID' or col == 'TIME_STAMP' or col == 'DELETE_FLG' or col == 'KOUSHIN_FUKA_FLG':
                            continue
                        if isinstance(value, str):
                            set_clause.append(f"{col} = N'{value}'")                        
                        elif ('ID' in col or 'KBN' in col or 'CD' in col or 'SEQ' in col) and isinstance(value, (int, float)):
                            set_clause.append(f"{col} = {int(value)}")
                        elif pd.isnull(value):
                            set_clause.append(f"{col} = NULL")
                        elif isinstance(value, pd.Timestamp):
                            set_clause.append(f"{col} = '{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                        else:
                            set_clause.append(f"{col} = {repr(value)}")
                    update_query = f"UPDATE {table_name} SET {', '.join(set_clause)} WHERE SYSTEM_ID = '{new_system_id}';"
                    insert_queries.append(update_query)
                else:
                    delete_query = f"DELETE FROM {table_name} WHERE SYSTEM_ID = '{new_system_id}';"
                    insert_queries.append(delete_query)

                    for index, row in df.iterrows():
                        values = []
                        columns = []
                        for col, value in row.items():
                            if col == 'TIME_STAMP':
                                continue  # Bỏ qua cột TIME_STAMP
                            columns.append(col)
                            if pd.isnull(value):
                                values.append('NULL')
                            elif col == 'SYSTEM_ID':
                                values.append(f"'{new_system_id}'")
                            elif table_name == 'T_FILE_LINK_KIHON_PJ_GAMEN' and col == 'FILE_ID':
                                self.have_file = True
                                current_max_file_id += 1
                                self.lst_fileid[value] = current_max_file_id
                                values.append(f"'{current_max_file_id}'")
                            elif table_name == 'T_FILE_LINK_KIHON_PJ_GAMEN' and isinstance(value, pd.Timestamp):
                                values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                            elif ('ID' in col or 'KBN' in col or 'CD' in col or 'SEQ' in col) and isinstance(value, (int, float)):
                                values.append(f"'{int(value)}'")
                            elif col in columns_to_convert and isinstance(value, (int, float)):
                                values.append(f"'{int(value)}'")
                            elif isinstance(value, str):
                                values.append(f"N'{value}'")
                            else:
                                values.append(f"'{str(value)}'")
                        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
                        insert_queries.append(query)

                all_queries.extend(insert_queries)
                all_queries.append("GO")

            file_path = os.path.join(self.save_directory, "all_tables.sql")
            with open(file_path, 'w', encoding='utf-8') as file:
                for query in all_queries:
                    file.write(query + '\n')

            if self.have_file:
                self.export_data_file()  
                
            QMessageBox.information(self, 'Success', 'INSERT and UPDATE statements have been generated and saved to a single file.')
            
            self.is_need_fileID = False
            self.export_button_multi.setEnabled(False)
            self.export_button_single.setEnabled(False)
            self.table_list_display.clear()
            self.system_id_input.setText("")
            self.new_system_id_input.setText("")
            self.current_max_file_id_input.setText("")
        except Exception as e:
            QMessageBox.critical(self, 'Error', f'An error occurred: {str(e)}')


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = DBApp()
    ex.show()
    sys.exit(app.exec_())
