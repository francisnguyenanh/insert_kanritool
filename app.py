import os
import io
import json
import zipfile
import tempfile
import datetime
import pyodbc
import pandas as pd
from flask import Flask, render_template, request, jsonify, session, send_file
import genScriptFromExcel

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


def export_data_file_helper(conn_file, lst_fileid, original_max_file_id):
    """Exports T_FILE_DATA and S_NUMBER_FILE – returns content as string."""
    file_id_keys = list(lst_fileid.keys())
    query = f"SELECT * FROM T_FILE_DATA WHERE FILE_ID IN ({', '.join(['?' for _ in file_id_keys])})"
    df = pd.read_sql(query, conn_file, params=file_id_keys)
    result = []
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
        result.extend(insert_queries)
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
        result.append(f"UPDATE S_NUMBER_FILE SET {', '.join(set_clause)};")
    return '\n'.join(result)


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
    if not matching_tables:
        return jsonify({'status': 'error', 'message': 'No tables found to export.'})

    columns_to_convert = ['ZENKAKU_MOJI_SU', 'HANKAKU_MOJI_SU', 'SEISU_KETA', 'SYOUSU_KETA']

    try:
        conn = get_main_conn()
        lst_fileid = {}
        have_file = False
        all_files = {}

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
            all_files[table_name + '.sql'] = '\n'.join(insert_queries)

        conn.close()

        if have_file:
            conn_file = get_file_conn()
            filedata = export_data_file_helper(conn_file, lst_fileid, original_max_file_id)
            all_files['T_FILE_DATA_and_S_NUMBER_FILE.sql'] = filedata
            conn_file.close()

        # Create a zip file in memory
        import io, zipfile
        mem_zip = io.BytesIO()
        with zipfile.ZipFile(mem_zip, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            for fname, content in all_files.items():
                zf.writestr(fname, content)
        mem_zip.seek(0)
        return send_file(mem_zip, as_attachment=True, download_name='exported_sql_files.zip', mimetype='application/zip')
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'An error occurred: {str(e)}'})


@app.route('/export_single', methods=['POST'])
def export_data_single():
    data = request.get_json()
    old_system_id = (data.get('old_system_id') or '').strip()
    new_system_id = (data.get('new_system_id') or '').strip()
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

        if have_file:
            conn_file = get_file_conn()
            filedata = export_data_file_helper(conn_file, lst_fileid, original_max_file_id)
            all_queries.append(filedata)
            conn_file.close()

        sql_bytes = io.BytesIO()
        sql_bytes.write('\n'.join(all_queries).encode('utf-8'))
        sql_bytes.seek(0)
        return send_file(sql_bytes, as_attachment=True, download_name='all_tables.sql', mimetype='text/plain')
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'An error occurred: {str(e)}'})


# ──────────────────────────────────────────────
#  Config API
# ──────────────────────────────────────────────

CONFIG_FILES = {
    'data_string':       'data_string.txt',
    'file_string':       'file_string.txt',
    'dataconnectstring': 'dataconnectstring.txt',
    'table':             'table.txt',
    'table_logic':       'table_logic.txt',
}


@app.route('/get_config', methods=['GET'])
def get_config():
    script_dir = get_script_dir()
    result = {}
    for key, filename in CONFIG_FILES.items():
        path = os.path.join(script_dir, filename)
        try:
            with open(path, 'r', encoding='utf-8') as f:
                result[key] = f.read()
        except Exception:
            result[key] = ''
    return jsonify({'status': 'success', 'config': result})


@app.route('/save_config', methods=['POST'])
def save_config():
    data = request.get_json()
    if not data:
        return jsonify({'status': 'error', 'message': 'No data provided.'})
    script_dir = get_script_dir()
    try:
        for key, filename in CONFIG_FILES.items():
            if key in data:
                path = os.path.join(script_dir, filename)
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(data[key])
        return jsonify({'status': 'success', 'message': 'Configuration saved successfully.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Failed to save configuration: {e}'})


# ──────────────────────────────────────────────
#  Tab 2: Gen Script from Excel
# ──────────────────────────────────────────────


# ──── Gen Script Config API ────

EXCEL_CONFIG_FILES = {
    'username_id': 'usernameID.txt',
    'table_info':  'TABLE_INFO.txt',
}


@app.route('/get_excel_config', methods=['GET'])
def get_excel_config():
    script_dir = get_script_dir()
    result = {}
    # Plain text files
    for key, filename in EXCEL_CONFIG_FILES.items():
        path = os.path.join(script_dir, filename)
        try:
            with open(path, 'r', encoding='utf-8') as f:
                result[key] = f.read()
        except Exception:
            result[key] = ''
    # genscript_config.json
    config_path = os.path.join(script_dir, 'genscript_config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            result['genscript_config'] = json.load(f)
    except Exception:
        result['genscript_config'] = {}
    return jsonify({'status': 'success', 'config': result})


@app.route('/save_excel_config', methods=['POST'])
def save_excel_config():
    data = request.get_json()
    if not data:
        return jsonify({'status': 'error', 'message': 'No data provided.'})
    script_dir = get_script_dir()
    try:
        # Save usernameID.txt
        if 'username_id' in data:
            with open(os.path.join(script_dir, 'usernameID.txt'), 'w', encoding='utf-8') as f:
                f.write(str(data['username_id']).strip())
        # Save TABLE_INFO.txt — validate JSON first
        if 'table_info' in data:
            try:
                json.loads(data['table_info'])  # validate
            except Exception as e:
                return jsonify({'status': 'error', 'message': f'TABLE_INFO.txt: invalid JSON — {e}'})
            with open(os.path.join(script_dir, 'TABLE_INFO.txt'), 'w', encoding='utf-8') as f:
                f.write(data['table_info'])
        # Save genscript_config.json
        if 'genscript_config' in data:
            with open(os.path.join(script_dir, 'genscript_config.json'), 'w', encoding='utf-8') as f:
                json.dump(data['genscript_config'], f, ensure_ascii=False, indent=2)
        # Reload constants in the genScriptFromExcel module
        genScriptFromExcel.reload_config()
        # Reset username ID counter so it re-reads the updated file
        genScriptFromExcel._username_id_counter = None
        return jsonify({'status': 'success', 'message': 'Configuration saved successfully.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Failed to save configuration: {e}'})


@app.route('/gen_excel', methods=['POST'])
def gen_excel():
    if 'excel_file' not in request.files:
        return jsonify({'status': 'error', 'message': 'No Excel file uploaded.'})

    excel_file = request.files['excel_file']
    if not excel_file.filename:
        return jsonify({'status': 'error', 'message': 'No Excel file selected.'})

    system_id   = request.form.get('system_id', '').strip()
    system_date = request.form.get('system_date', '').strip()

    # Save uploaded Excel to a temp file
    tmp_excel = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
    try:
        excel_file.save(tmp_excel.name)
        tmp_excel.close()
    except Exception:
        tmp_excel.close()
        try:
            os.unlink(tmp_excel.name)
        except OSError:
            pass
        return jsonify({'status': 'error', 'message': 'Failed to save uploaded file.'})

    tmp_output = tempfile.NamedTemporaryFile(delete=False, suffix='.sql', mode='w', encoding='utf-8')
    tmp_output.close()

    try:
        # Reload config before each run so any saved changes take effect
        genScriptFromExcel.reload_config()
        # Override appX globals per-request
        now = datetime.datetime.now()
        genScriptFromExcel.systemid_value    = system_id  if system_id   else f"{now.hour:02d}{now.minute:02d}{now.second:02d}"
        genScriptFromExcel.system_date_value = system_date if system_date else now.strftime('%Y-%m-%d')

        # Path to TABLE_INFO.txt stored next to app.py
        table_info_path = os.path.join(get_script_dir(), 'TABLE_INFO.txt')

        genScriptFromExcel.all_tables_in_sequence(tmp_excel.name, table_info_path, tmp_output.name)

        with open(tmp_output.name, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        sql_bytes = io.BytesIO(sql_content.encode('utf-8'))
        sql_bytes.seek(0)
        return send_file(sql_bytes, as_attachment=True, download_name='insert_all.sql', mimetype='text/plain')
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'An error occurred: {str(e)}'})
    finally:
        for path in (tmp_excel.name, tmp_output.name):
            try:
                os.unlink(path)
            except OSError:
                pass


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5021)
