import os
import io
import json
import re
import zipfile
import tempfile
import datetime
import threading
import pyodbc
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
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


import base64


def validate_insert_columns(sql_content):
    """Parse INSERT statements, compare columns against DB, return list of warning strings."""
    import re

    # Load skip_check_columns from genscript_config.json
    skip_cols = set()
    try:
        config_path = os.path.join(get_script_dir(), 'genscript_config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        skip_cols = {c.upper() for c in cfg.get('skip_check_columns', [])}
    except Exception:
        pass

    # Collect unique set of columns per table from INSERT statements
    table_cols = {}  # {table_name: set_of_columns}
    pattern = re.compile(
        r'INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)',
        re.IGNORECASE
    )
    for m in pattern.finditer(sql_content):
        tname = m.group(1).upper()
        cols = {c.strip().upper() for c in m.group(2).split(',')}
        if tname not in table_cols:
            table_cols[tname] = cols
        else:
            table_cols[tname] |= cols  # union: accumulate all cols seen

    if not table_cols:
        return []

    try:
        conn = get_main_conn()
    except Exception as e:
        return [f'[DB connect failed — column check skipped] {e}']

    warnings = []
    try:
        cursor = conn.cursor()
        for tname, insert_cols in sorted(table_cols.items()):
            try:
                cursor.execute(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                    tname
                )
                db_cols = {row[0].upper() for row in cursor.fetchall()} - skip_cols
                if not db_cols:
                    warnings.append(f"⚠️ Table {tname}: not found in DB (skipped).")
                    continue
                effective_insert = insert_cols - skip_cols
                missing = sorted(db_cols - effective_insert)
                extra   = sorted(effective_insert - db_cols)
                if missing and extra:
                    warnings.append(
                        f"⚠️ Table {tname}: INSERT に欠けているカラム: {', '.join(missing)} / "
                        f"余分なカラム: {', '.join(extra)}"
                    )
                elif missing:
                    warnings.append(
                        f"⚠️ Table {tname}: INSERT に欠けているカラム: {', '.join(missing)}"
                    )
                elif extra:
                    warnings.append(
                        f"⚠️ Table {tname}: INSERT にテーブル内にないカラム: {', '.join(extra)}"
                    )
                else:
                    warnings.append(f"✅ Table {tname}: OK (全カラム一致)")
            except Exception as e:
                warnings.append(f"⚠️ Table {tname}: check failed — {e}")
    finally:
        conn.close()

    return warnings


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

        # Validate columns against DB
        warnings = validate_insert_columns(sql_content)

        sql_b64 = base64.b64encode(sql_content.encode('utf-8')).decode('ascii')
        return jsonify({
            'status': 'success',
            'sql_b64': sql_b64,
            'warnings': warnings,
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'An error occurred: {str(e)}'})
    finally:
        for path in (tmp_excel.name, tmp_output.name):
            try:
                os.unlink(path)
            except OSError:
                pass


# ─────────────────────────────────────────────────────────────
#  Reproduce DB — config helpers
# ─────────────────────────────────────────────────────────────

_REPRODUCE_CONFIG_FILE = 'reproduce_config.json'
_REPRODUCE_CONFIG_DEFAULTS = {
    'src_conn_str':   '',
    'dst_conn_str':   '',
    'src_db_name':    '',
    'dst_db_name':    '',
    'rows_per_table': 1000,
}


def _get_reproduce_config():
    path = os.path.join(get_script_dir(), _REPRODUCE_CONFIG_FILE)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return {**_REPRODUCE_CONFIG_DEFAULTS, **data}
    except Exception:
        return dict(_REPRODUCE_CONFIG_DEFAULTS)


def _save_reproduce_config(data):
    path = os.path.join(get_script_dir(), _REPRODUCE_CONFIG_FILE)
    cfg = {k: data.get(k, v) for k, v in _REPRODUCE_CONFIG_DEFAULTS.items()}
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    return cfg


# ─────────────────────────────────────────────────────────────
#  Reproduce DB — background migration job state
# ─────────────────────────────────────────────────────────────

_migration_lock = threading.Lock()
_migration_state = {
    'running':  False,
    'done':     False,
    'success':  False,
    'progress': 0,
    'label':    '',
    'logs':     [],
}


def _run_migration(cfg):
    """Background thread: copies tables from a remote SQL Server to a new local DB."""
    global _migration_state

    def log(msg, level='INFO'):
        prefix = {
            'INFO':    '[INFO]',
            'ERROR':   '[ERROR]',
            'WARN':    '[WARN]',
            'SUCCESS': '[SUCCESS]',
        }.get(level, '[INFO]')
        with _migration_lock:
            _migration_state['logs'].append(f"{prefix} {msg}")

    def set_progress(pct, label=''):
        with _migration_lock:
            _migration_state['progress'] = pct
            if label:
                _migration_state['label'] = label

    def finish(success):
        with _migration_lock:
            _migration_state['done']    = True
            _migration_state['running'] = False
            _migration_state['success'] = success

    try:
        src_conn_str = (cfg.get('src_conn_str') or '').strip()
        dst_conn_str = (cfg.get('dst_conn_str') or '').strip()
        src_db_name  = (cfg.get('src_db_name')  or '').strip()
        dst_db_name  = (cfg.get('dst_db_name')  or '').strip()
        rows         = int(cfg.get('rows_per_table') or 1000)

        if not src_conn_str or not dst_conn_str or not src_db_name or not dst_db_name:
            log('Missing required configuration (connection strings or DB names).', 'ERROR')
            finish(False)
            return

        # Validate destination DB name to prevent injection in CREATE DATABASE
        if not re.match(r'^[A-Za-z0-9_\-]+$', dst_db_name):
            log(
                f"Invalid destination DB name '{dst_db_name}'. "
                "Use letters, digits, underscores, or hyphens only.", 'ERROR'
            )
            finish(False)
            return

        # ── Step 1: Create destination DB if not exists ──
        log(f"Connecting to destination server to create database '{dst_db_name}'…")
        set_progress(2, 'Creating destination database…')
        try:
            conn_master = pyodbc.connect(dst_conn_str, autocommit=True)
            cur = conn_master.cursor()
            cur.execute("SELECT COUNT(*) FROM sys.databases WHERE name = ?", dst_db_name)
            if cur.fetchone()[0] == 0:
                cur.execute(f"CREATE DATABASE [{dst_db_name}]")
                log(f"Database '{dst_db_name}' created.", 'SUCCESS')
            else:
                log(f"Database '{dst_db_name}' already exists — skipping creation.")
            conn_master.close()
        except Exception as e:
            log(f"Failed to create destination database: {e}", 'ERROR')
            finish(False)
            return

        # ── Step 2: Connect to source DB and fetch table list ──
        log(f"Connecting to source database '{src_db_name}'…")
        set_progress(5, 'Fetching table list…')
        try:
            upper = src_conn_str.upper()
            if 'DATABASE=' not in upper and 'INITIAL CATALOG=' not in upper:
                src_full = src_conn_str + f";DATABASE={src_db_name}"
            else:
                src_full = src_conn_str
            source_conn = pyodbc.connect(src_full)
            tables_df = pd.read_sql(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
                source_conn
            )
            table_list = tables_df['TABLE_NAME'].tolist()
            log(f"Found {len(table_list)} table(s) to copy.")
        except Exception as e:
            log(f"Failed to connect to source database: {e}", 'ERROR')
            finish(False)
            return

        # ── Step 3: Build SQLAlchemy engine for destination ──
        try:
            dst_full = dst_conn_str + f";DATABASE={dst_db_name}"
            dest_engine = create_engine(
                f"mssql+pyodbc:///?odbc_connect={quote_plus(dst_full)}",
                fast_executemany=True,
            )
        except Exception as e:
            log(f"Failed to create destination engine: {e}", 'ERROR')
            source_conn.close()
            finish(False)
            return

        # ── Step 4: Copy each table ──
        total         = len(table_list)
        success_count = 0
        error_count   = 0

        for i, table in enumerate(table_list):
            pct = 5 + int((i / total) * 90) if total else 95
            set_progress(pct, f"Đang copy bảng {i + 1}/{total}: {table}…")
            log(f"({i + 1}/{total}) Đang copy bảng [{table}]…")
            try:
                if rows > 0:
                    query = f"SELECT TOP {rows} * FROM [{table}] ORDER BY 1 DESC"
                else:
                    query = f"SELECT * FROM [{table}]"
                df = pd.read_sql(query, source_conn)
                row_count = len(df)

                # Use a single SQLAlchemy connection per table so that
                # SET IDENTITY_INSERT stays in scope during the actual INSERT.
                with dest_engine.begin() as dest_conn:
                    # ── IDENTITY INSERT ───────────────────────────────────────
                    # Attempt to preserve original ID values from the source.
                    # When if_exists='replace' recreates the table from the
                    # DataFrame schema (no IDENTITY column), this statement will
                    # raise an error and be silently skipped — the values are
                    # still written correctly because the new column is a plain
                    # integer without the IDENTITY constraint.
                    identity_enabled = False
                    try:
                        dest_conn.execute(text(f"SET IDENTITY_INSERT [{table}] ON"))
                        identity_enabled = True
                    except Exception:
                        pass  # table has no identity column — insert proceeds normally

                    try:
                        # method='multi' → multi-row INSERT VALUES (bulk insert).
                        # chunksize=1000 keeps each statement within SQL Server
                        # parameter limits (~2100 params per batch).
                        df.to_sql(
                            table, dest_conn,
                            if_exists='replace',
                            index=False,
                            method='multi',
                            chunksize=1000,
                        )
                    finally:
                        # Always turn IDENTITY INSERT back off after the write.
                        if identity_enabled:
                            try:
                                dest_conn.execute(text(f"SET IDENTITY_INSERT [{table}] OFF"))
                            except Exception:
                                pass

                log(f"Đang copy bảng [{table}]… Thành công ({row_count} dòng)", 'SUCCESS')
                success_count += 1
            except Exception as e:
                log(f"Bảng [{table}] thất bại: {e}", 'ERROR')
                error_count += 1

        source_conn.close()
        dest_engine.dispose()
        set_progress(100, 'Done')

        if error_count == 0:
            log(f"Migration complete! {success_count} table(s) copied successfully.", 'SUCCESS')
        else:
            log(f"Migration finished with errors. OK: {success_count} / Failed: {error_count}.", 'WARN')
        finish(error_count == 0)

    except Exception as exc:
        with _migration_lock:
            _migration_state['logs'].append(f'[ERROR] Unexpected error: {exc}')
        finish(False)


# ─────────────────────────────────────────────────────────────
#  Reproduce DB — Routes
# ─────────────────────────────────────────────────────────────

@app.route('/reproduce-db')
def reproduce_db_page():
    return render_template('reproduce_db.html')


@app.route('/reproduce/config', methods=['GET', 'POST'])
def reproduce_config():
    if request.method == 'GET':
        return jsonify({'status': 'success', 'config': _get_reproduce_config()})

    data = request.get_json()
    if not data:
        return jsonify({'status': 'error', 'message': 'No data provided.'})
    try:
        _save_reproduce_config(data)
        return jsonify({'status': 'success', 'message': 'Configuration saved successfully.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Failed to save configuration: {e}'})


@app.route('/reproduce/start', methods=['POST'])
def reproduce_start():
    global _migration_state

    with _migration_lock:
        if _migration_state.get('running'):
            return jsonify({'status': 'error', 'message': 'A migration is already in progress.'})
        _migration_state = {
            'running':  True,
            'done':     False,
            'success':  False,
            'progress': 0,
            'label':    'Starting…',
            'logs':     [],
        }

    # Merge posted values on top of saved config so UI changes take effect immediately
    posted = request.get_json() or {}
    cfg = _get_reproduce_config()
    for k in _REPRODUCE_CONFIG_DEFAULTS:
        if posted.get(k) not in (None, ''):
            cfg[k] = posted[k]

    threading.Thread(target=_run_migration, args=(cfg,), daemon=True).start()
    return jsonify({'status': 'success', 'message': 'Migration started.'})


@app.route('/reproduce/status', methods=['GET'])
def reproduce_status():
    with _migration_lock:
        done     = _migration_state['done']
        success  = _migration_state['success']
        progress = _migration_state['progress']
        label    = _migration_state['label']
        logs     = list(_migration_state['logs'])
        _migration_state['logs'] = []   # clear: each poll returns only new lines
    return jsonify({
        'done':     done,
        'success':  success,
        'progress': progress,
        'label':    label,
        'logs':     logs,
    })


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5021)
