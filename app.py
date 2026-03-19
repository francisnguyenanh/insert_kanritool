import os
import pyodbc
import pandas as pd
from flask import Flask, render_template, request, jsonify, session

app = Flask(__name__)
app.secret_key = 'db_export_tool_secret_key_flask'


def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))


def get_main_conn():
    script_dir = get_script_dir()
    with open(os.path.join(script_dir, 'data_string.txt'), 'r', encoding='utf-8') as f:
        conn_str = f.read().strip()
    return pyodbc.connect(conn_str)


def get_file_conn():
    script_dir = get_script_dir()
    with open(os.path.join(script_dir, 'file_string.txt'), 'r', encoding='utf-8') as f:
        conn_str = f.read().strip()
    return pyodbc.connect(conn_str)


def export_data_file_helper(conn_file, lst_fileid, original_max_file_id, save_directory):
    """Exports T_FILE_DATA and S_NUMBER_FILE – mirrors export_data_file() in main.py."""
    file_id_keys = list(lst_fileid.keys())
    query = f"SELECT * FROM T_FILE_DATA WHERE FILE_ID IN ({', '.join(['?' for _ in file_id_keys])})"
    df = pd.read_sql(query, conn_file, params=file_id_keys)

    if not df.empty:
        current_max_file_id = original_max_file_id
        insert_queries = []
        for _index, row in df.iterrows():
            values = []
            columns = []
            for col, value in row.items():
                if col == 'TIME_STAMP':
                    continue
                columns.append(col)
                if pd.isnull(value):
                    values.append('NULL')
                elif isinstance(value, bool):
                    values.append('1' if value else '0')
                elif col == 'FILE_ID':
                    current_max_file_id += 1
                    values.append(f"'{lst_fileid.get(value)}'")
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
            insert_queries.append(
                f"INSERT INTO T_FILE_DATA ({', '.join(columns)}) VALUES ({', '.join(values)});"
            )

        file_path = os.path.join(save_directory, 'T_FILE_DATA.sql')
        with open(file_path, 'w', encoding='utf-8') as f:
            for q in insert_queries:
                f.write(q + '\n')

        # S_NUMBER_FILE
        df2 = pd.read_sql("SELECT * FROM S_NUMBER_FILE", conn_file)
        set_clause = []
        for col, value in df2.iloc[0].items():
            if col in ('CREATE_USER', 'CREATE_DATE', 'CREATE_PC', 'TIME_STAMP', 'DELETE_FLG'):
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

        file_path = os.path.join(save_directory, 'S_NUMBER_FILE.sql')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"UPDATE S_NUMBER_FILE SET {', '.join(set_clause)};\n")


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/connect', methods=['POST'])
def connect_to_db():
    data = request.get_json()
    old_system_id = (data.get('old_system_id') or '').strip()

    if not old_system_id:
        return jsonify({'status': 'error', 'message': 'Please enter the Old System ID before connecting to the database.'})

    try:
        script_dir = get_script_dir()
        with open(os.path.join(script_dir, 'table_logic.txt'), 'r', encoding='utf-8') as f:
            table_names = [line.strip() for line in f if line.strip()]

        conn = get_main_conn()
        cursor = conn.cursor()

        matching_tables = []
        for table_name in table_names:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE SYSTEM_ID = ?", old_system_id)
            if cursor.fetchone()[0] > 0:
                matching_tables.append(table_name)

        conn.close()
        matching_tables = sorted(matching_tables)
        is_need_fileID = 'T_FILE_LINK_KIHON_PJ_GAMEN' in matching_tables

        session['matching_tables'] = matching_tables
        session['is_need_fileID'] = is_need_fileID

        if not matching_tables:
            return jsonify({'status': 'warning', 'message': 'No data found for the given System ID in any table.'})

        return jsonify({
            'status': 'success',
            'message': 'Loaded matching table names successfully!',
            'matching_tables': matching_tables,
            'is_need_fileID': is_need_fileID,
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Failed to load table names: {e}'})


@app.route('/export_multi', methods=['POST'])
def export_data_multi():
    data = request.get_json()
    old_system_id = (data.get('old_system_id') or '').strip()
    new_system_id = (data.get('new_system_id') or '').strip()
    save_directory = (data.get('save_directory') or '').strip()
    current_max_file_id_str = (data.get('current_max_file_id') or '').strip()
    matching_tables = data.get('matching_tables', [])
    is_need_fileID = data.get('is_need_fileID', False)

    if is_need_fileID:
        if not current_max_file_id_str:
            return jsonify({'status': 'error', 'message': 'Please enter current max file ID.'})
        current_max_file_id = int(current_max_file_id_str)
    else:
        current_max_file_id = 0

    original_max_file_id = current_max_file_id

    if not old_system_id or not new_system_id:
        return jsonify({'status': 'error', 'message': 'Please enter both Old System ID and New System ID.'})
    if not save_directory:
        return jsonify({'status': 'error', 'message': 'Please select a directory to save the SQL files.'})
    if not os.path.isdir(save_directory):
        return jsonify({'status': 'error', 'message': f'Directory does not exist: {save_directory}'})
    if not matching_tables:
        return jsonify({'status': 'error', 'message': 'No tables found to export.'})

    columns_to_convert = ['ZENKAKU_MOJI_SU', 'HANKAKU_MOJI_SU', 'SEISU_KETA', 'SYOUSU_KETA']

    try:
        conn = get_main_conn()
        lst_fileid = {}
        have_file = False

        for table_name in matching_tables:
            if not table_name.strip():
                continue

            df = pd.read_sql(f"SELECT * FROM {table_name} WHERE SYSTEM_ID = ?", conn, params=[old_system_id])
            if df.empty:
                continue

            insert_queries = []

            if table_name == 'T_KIHON_PJ':
                set_clause = []
                for col, value in df.iloc[0].items():
                    if col in ('SYSTEM_ID', 'TIME_STAMP', 'DELETE_FLG', 'KOUSHIN_FUKA_FLG'):
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
                insert_queries.append(
                    f"UPDATE {table_name} SET {', '.join(set_clause)} WHERE SYSTEM_ID = '{new_system_id}';"
                )
            else:
                insert_queries.append(f"DELETE FROM {table_name} WHERE SYSTEM_ID = '{new_system_id}';")
                for _index, row in df.iterrows():
                    values = []
                    columns = []
                    for col, value in row.items():
                        if col == 'TIME_STAMP':
                            continue
                        columns.append(col)
                        if pd.isnull(value):
                            values.append('NULL')
                        elif col == 'SYSTEM_ID':
                            values.append(f"'{new_system_id}'")
                        elif table_name == 'T_FILE_LINK_KIHON_PJ_GAMEN' and col == 'FILE_ID':
                            have_file = True
                            current_max_file_id += 1
                            lst_fileid[value] = current_max_file_id
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
                    insert_queries.append(
                        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
                    )

            file_path = os.path.join(save_directory, f"{table_name}.sql")
            with open(file_path, 'w', encoding='utf-8') as f:
                for q in insert_queries:
                    f.write(q + '\n')

        conn.close()

        if have_file:
            conn_file = get_file_conn()
            export_data_file_helper(conn_file, lst_fileid, original_max_file_id, save_directory)
            conn_file.close()

        return jsonify({'status': 'success', 'message': 'INSERT and UPDATE statements have been generated and saved to multiple files.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'An error occurred: {str(e)}'})


@app.route('/export_single', methods=['POST'])
def export_data_single():
    data = request.get_json()
    old_system_id = (data.get('old_system_id') or '').strip()
    new_system_id = (data.get('new_system_id') or '').strip()
    save_directory = (data.get('save_directory') or '').strip()
    current_max_file_id_str = (data.get('current_max_file_id') or '').strip()
    matching_tables = data.get('matching_tables', [])
    is_need_fileID = data.get('is_need_fileID', False)

    if is_need_fileID:
        if not current_max_file_id_str:
            return jsonify({'status': 'error', 'message': 'Please enter current max file ID.'})
        current_max_file_id = int(current_max_file_id_str)
    else:
        current_max_file_id = 0

    original_max_file_id = current_max_file_id

    if not old_system_id or not new_system_id:
        return jsonify({'status': 'error', 'message': 'Please enter both Old System ID and New System ID.'})
    if not save_directory:
        return jsonify({'status': 'error', 'message': 'Please select a directory to save the SQL files.'})
    if not os.path.isdir(save_directory):
        return jsonify({'status': 'error', 'message': f'Directory does not exist: {save_directory}'})
    if not matching_tables:
        return jsonify({'status': 'error', 'message': 'No tables found to export.'})

    columns_to_convert = ['ZENKAKU_MOJI_SU', 'HANKAKU_MOJI_SU', 'SEISU_KETA', 'SYOUSU_KETA']

    try:
        conn = get_main_conn()
        all_queries = []
        lst_fileid = {}
        have_file = False

        for table_name in matching_tables:
            if not table_name.strip():
                continue

            df = pd.read_sql(f"SELECT * FROM {table_name} WHERE SYSTEM_ID = ?", conn, params=[old_system_id])
            if df.empty:
                continue

            insert_queries = []

            if table_name == 'T_KIHON_PJ':
                set_clause = []
                for col, value in df.iloc[0].items():
                    if col in ('SYSTEM_ID', 'TIME_STAMP', 'DELETE_FLG', 'KOUSHIN_FUKA_FLG'):
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
                insert_queries.append(
                    f"UPDATE {table_name} SET {', '.join(set_clause)} WHERE SYSTEM_ID = '{new_system_id}';"
                )
            else:
                insert_queries.append(f"DELETE FROM {table_name} WHERE SYSTEM_ID = '{new_system_id}';")
                for _index, row in df.iterrows():
                    values = []
                    columns = []
                    for col, value in row.items():
                        if col == 'TIME_STAMP':
                            continue
                        columns.append(col)
                        if pd.isnull(value):
                            values.append('NULL')
                        elif col == 'SYSTEM_ID':
                            values.append(f"'{new_system_id}'")
                        elif table_name == 'T_FILE_LINK_KIHON_PJ_GAMEN' and col == 'FILE_ID':
                            have_file = True
                            current_max_file_id += 1
                            lst_fileid[value] = current_max_file_id
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
                    insert_queries.append(
                        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
                    )

            all_queries.extend(insert_queries)
            all_queries.append("GO")

        conn.close()

        file_path = os.path.join(save_directory, 'all_tables.sql')
        with open(file_path, 'w', encoding='utf-8') as f:
            for q in all_queries:
                f.write(q + '\n')

        if have_file:
            conn_file = get_file_conn()
            export_data_file_helper(conn_file, lst_fileid, original_max_file_id, save_directory)
            conn_file.close()

        return jsonify({'status': 'success', 'message': 'INSERT and UPDATE statements have been generated and saved to a single file.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'An error occurred: {str(e)}'})


if __name__ == '__main__':
    app.run(debug=False, host='127.0.0.1', port=5021)
