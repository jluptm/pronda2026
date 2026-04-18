import os
import sys
import asyncio
import tempfile
from datetime import datetime
import pandas as pd
import streamlit as st
import streamlit_antd_components as sac
import libsql_client as libsql
import boto3
import altair as alt

try:
    import tomllib
except ImportError:
    import tomli as tomllib

# -------------------------------------------------------------------
# CONFIGURACION PRINCIPAL Y SECRETOS
# -------------------------------------------------------------------
st.set_page_config(page_title="Prondamin 2026", layout="wide", page_icon="📝")

secrets = {}
try:
    with open("secrets.toml", "rb") as f:
        secrets = tomllib.load(f)
except Exception as e:
    pass

def get_secret(group, key, env_key):
    val = os.getenv(env_key)
    if val: return val
    try:
        if group in st.secrets and key in st.secrets[group]:
            return st.secrets[group][key]
    except Exception: pass
    if group in secrets and key in secrets[group]:
        return secrets[group][key]
    return None

TURSO_URL = get_secret("turso", "url", "TURSO_URL")
TURSO_AUTH_TOKEN = get_secret("turso", "auth_token", "TURSO_AUTH_TOKEN")

# Cloudflare R2 Config
R2_ACCESS_KEY = get_secret("aws", "access_key", "R2_ACCESS_KEY_ID")
R2_SECRET_KEY = get_secret("aws", "secret_key", "R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = get_secret("aws", "endpoint_url", "R2_ENDPOINT_URL")
R2_PUBLIC_URL = get_secret("aws", "public_url", "R2_PUBLIC_URL")
R2_BUCKET_NAME = get_secret("aws", "bucket_name", "R2_BUCKET_NAME") or "prondamin-captures"

# Listas Base
CATEGORIAS_BASE = ["Ministro Ordenado", "Ministro Licenciado", "Ministro Cristiano", "Ministro Distrital"]
DISTRITOS_BASE = ["Andino", "Centro",  "Centro Llanos", "Falcón", "Lara", "Llanos Occidentales",  "Metropolitano", "Nor Oriente", "Sur Oriente",  "Yaracuy", "Zulia" ]
BANCOS_OPCIONES = [
    "0001-Banco Central de Venezuela",
    "0102-Banco de Venezuela (BDV)",
    "0104-Banco Venezolano de Crédito (BVC)",
    "0105-Banco Mercantil",
    "0108-BBVA Provincial (antes Banco Provincial)",
    "0114-Bancaribe",
    "0115-Banco Exterior",
    "0128-Banco Caroní",
    "0134-Banesco",
    "0137-Banco Sofitasa",
    "0138-Banco Plaza",
    "0146-Bangente",
    "0151-Banco Fondo Común (BFC)",
    "0156-100% Banco",
    "0157-DelSur Banco Universal",
    "0163-Banco del Tesoro",
    "0166-Banco Agrícola de Venezuela",
    "0168-Bancrecer",
    "0169-Mi Banco / R4 Banco Microfinanciero",
    "0171-Banco Activo",
    "0172-Bancamiga",
    "0173-Banco Internacional de Desarrollo",
    "0174-Banplus",
    "0175-Banco Digital de los Trabajadores (Bicentenario digital)",
    "0177-BANFANB (Banco de la Fuerza Armada Nacional Bolivariana)",
    "0178-N58 Banco Digital Microfinanciero",
    "0191-Banco Nacional de Crédito (BNC)"
]
CURSOS = [
    "Ministro Cristiano",
    "Ministro Licenciado",
    "Ministro Ordenado"]


# Inicialización S3/R2
@st.cache_resource
def get_s3_client():
    if R2_ACCESS_KEY and R2_SECRET_KEY and R2_ENDPOINT_URL:
        return boto3.client(
            's3',
            endpoint_url=R2_ENDPOINT_URL,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name='auto'
        )
    return None

s3_client = get_s3_client()

def run_async(coro):
    """Ejecuta una corrutina en el event loop síncronamente"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

# -------------------------------------------------------------------
# LÓGICA DE BASE DE DATOS MIGRADA DE TURSO SQL
# -------------------------------------------------------------------
async def _get_df_from_turso(table_name):
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        result = await client.execute(f"SELECT * FROM {table_name}")
        return pd.DataFrame(result.rows, columns=result.columns)

async def _busca_en_turso_pronda26(cedula: str):
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        result = await client.execute("SELECT * FROM prondamin2026BB WHERE CEDULA = ?", [cedula])
        if len(result.rows) > 0:
            return pd.DataFrame(result.rows, columns=result.columns)
        return "Cedula No encontrada"

async def _busca_en_turso_pronda25(cedula: str):
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        result = await client.execute("SELECT * FROM pronda_2025 WHERE CEDULA = ?", [cedula])
        if len(result.rows) > 0:
            cols = result.columns
            return dict(zip(cols, result.rows[0]))
        return None

async def _login_admin(username, password):
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        result = await client.execute("SELECT * FROM usuarios_sistema WHERE Usuario = ? AND Clave = ?", [username, password])
        if len(result.rows) > 0:
            return dict(zip(result.columns, result.rows[0]))
        return None

async def _verifica_clave_admin_f(clave):
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        result = await client.execute("SELECT * FROM usuarios_sistema WHERE Clave = ?", [clave])
        if len(result.rows) > 0:
            user = dict(zip(result.columns, result.rows[0]))
            tipo = user.get("TipoDeAcceso", "")
            if "Financiero" in tipo or "Develop" in tipo or "Total" in tipo:
                return True
        return False

async def _insert_registro(values_dict):
    keys = list(values_dict.keys())
    placeholders = ", ".join(["?"] * len(keys))
    fields = ", ".join(keys)
    values = list(values_dict.values())
    
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        sql = f"INSERT INTO prondamin2026BB ({fields}) VALUES ({placeholders})"
        await client.execute(sql, values)

async def _check_and_create_databank_table():
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        # 1. Crear tabla si no existe
        await client.execute("""
            CREATE TABLE IF NOT EXISTS databank (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Tipo TEXT,
                Fecha TEXT,
                Referencia TEXT,
                Descripcion TEXT,
                Monto REAL,
                FECHA_CARGA TEXT,
                'CEDULA-U' TEXT,
                UNIQUE(Fecha, Referencia, Descripcion)
            )
        """)
        
        # 2. Asegurar que la columna 'CEDULA-U' existe (por si la tabla ya existía previamente)
        try:
            # Consultamos una fila para ver las columnas
            res = await client.execute("SELECT * FROM databank LIMIT 1")
            if 'CEDULA-U' not in res.columns:
                await client.execute("ALTER TABLE databank ADD COLUMN 'CEDULA-U' TEXT")
        except Exception:
            pass

async def _process_pagos_2026():
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        # 1. Obtener registros de prondamin2026BB
        res_reg = await client.execute("SELECT CEDULA, REFERENCIA FROM prondamin2026BB")
        # Diccionario de reg_ref -> CEDULA (para búsqueda rápida)
        # Limpiamos reg_ref: solo números, mínimo 4 dígitos
        registros = {}
        for row in res_reg.rows:
            cedula = str(row[0]).strip()
            ref_raw = ''.join(filter(str.isdigit, str(row[1])))
            if len(ref_raw) >= 4:
                registros[ref_raw] = cedula

        # 2. Obtener registros de databank que no tengan CEDULA-U o sea No Asignado
        res_bank = await client.execute("SELECT id, Referencia FROM databank")
        
        statements = []
        count_assigned = 0
        
        for row in res_bank.rows:
            bid = row[0]
            bref_raw = ''.join(filter(str.isdigit, str(row[1])))
            
            cedula_found = "No Asignado"
            # Criterio: buscar si alguna reg_ref (4 a 8 dígitos) coincide con el final de bref_raw
            # Optimizamos: probamos longitudes de 4, 5, 6, 7, 8 al final de bref_raw
            for r_ref, r_ced in registros.items():
                if len(r_ref) >= 4 and bref_raw.endswith(r_ref):
                    cedula_found = r_ced
                    count_assigned += 1
                    break
            
            statements.append(libsql.Statement(
                "UPDATE databank SET 'CEDULA-U' = ? WHERE id = ?",
                [cedula_found, bid]
            ))
            
        if statements:
            await client.batch(statements)
        
        return count_assigned, len(res_bank.rows)

def process_pagos_2026(): return run_async(_process_pagos_2026())

async def _get_databank_df():
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        try:
            # Cruzamos databank con prondamin2026BB para traer los datos del usuario
            sql = """
                SELECT 
                    d.*, 
                    p.NOMBRES, 
                    p.APELLIDOS, 
                    p.DISTRITO, 
                    p.MONTO_PAGO, 
                    p.MONTO_A_PAGAR,
                    p.Status 
                FROM databank d
                LEFT JOIN prondamin2026BB p ON d."CEDULA-U" = p.CEDULA
            """
            res = await client.execute(sql)
            return pd.DataFrame(res.rows, columns=res.columns)
        except Exception:
            return pd.DataFrame()

def get_databank_df(): return run_async(_get_databank_df())

async def _bulk_update_status_verificado(cedulas):
    if not cedulas: return 0
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        statements = []
        for c in cedulas:
            statements.append(libsql.Statement(
                "UPDATE prondamin2026BB SET Status = 'Verificado' WHERE CEDULA = ? AND Status = 'Pendiente'", 
                [c]
            ))
        if statements:
            await client.batch(statements)
        return len(statements)

def bulk_update_status_verificado(cedulas): return run_async(_bulk_update_status_verificado(cedulas))

async def _get_databank_keys():
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        try:
            res = await client.execute("SELECT Fecha, Referencia, Descripcion FROM databank")
            return set((str(r[0]).strip(), str(r[1]).strip(), str(r[2]).strip()) for r in res.rows)
        except Exception:
            return set()

def clean_val(val):
    if pd.isna(val): return ""
    v = str(val).strip()
    if v.endswith(".0"): v = v[:-2]
    return v

async def _insert_bank_data(df_rows):
    existing_keys = await _get_databank_keys()
    new_rows = []
    
    for _, row in df_rows.iterrows():
        # Limpieza de datos
        fecha = clean_val(row['Fecha'])
        ref = clean_val(row['Referencia'])
        desc = clean_val(row['Descripción'])
        
        key = (fecha, ref, desc)
        if key not in existing_keys:
            new_rows.append(row)
    
    if not new_rows:
        return 0

    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        insert_sql = """
            INSERT OR IGNORE INTO databank (Tipo, Fecha, Referencia, Descripcion, Monto, FECHA_CARGA)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        now_ts = datetime.now().isoformat()
        statements = []
        for row in new_rows:
            # Asegurar monto numérico
            try:
                monto_str = str(row['Monto Bs.']).replace(".", "").replace(",", ".") if isinstance(row['Monto Bs.'], str) else row['Monto Bs.']
                monto = float(monto_str)
            except:
                monto = 0.0
                
            statements.append(libsql.Statement(insert_sql, [
                clean_val(row['Tipo']), clean_val(row['Fecha']), clean_val(row['Referencia']), 
                clean_val(row['Descripción']), monto, now_ts
            ]))
        
        # Batch insert
        if statements:
            await client.batch(statements)
        return len(statements)

def check_and_create_databank_table(): return run_async(_check_and_create_databank_table())
def insert_bank_data(df): return run_async(_insert_bank_data(df))

async def _load_merged_data(districts):
    common_cols = ['CEDULA', 'NOMBRES', 'APELLIDOS', 'DISTRITO', 'CATEGORIA', 'EMAIL', 'TELEFONOS', 'Status', 'REFERENCIA', 'CURSO_INSCRITO', '_source']
    
    df_2025 = await _get_df_from_turso("pronda_2025")
    if not df_2025.empty:
        df_2025['CEDULA'] = df_2025['CEDULA'].astype(str).str.strip()
        df_2025 = df_2025.rename(columns={'NOMBRES2025': 'NOMBRES', 'APELLIDOS2025': 'APELLIDOS', 'DISTRITO2025': 'DISTRITO', 'CATEGORIA2025': 'CATEGORIA', 'EMAIL2025': 'EMAIL', 'TELEFONOS2025': 'TELEFONOS'})
        df_2025['_source'] = '2025'
        if 'Status' not in df_2025.columns: df_2025['Status'] = "No Inscrito"
        for col in common_cols:
            if col not in df_2025.columns: df_2025[col] = None
        df_2025 = df_2025[common_cols]

    df_2026 = await _get_df_from_turso("prondamin2026BB")
    if not df_2026.empty:
        df_2026['CEDULA'] = df_2026['CEDULA'].astype(str).str.strip()
        df_2026['_source'] = '2026'
        for col in common_cols:
            if col not in df_2026.columns: df_2026[col] = None
        df_2026 = df_2026[common_cols]

    if df_2025.empty and df_2026.empty: return pd.DataFrame()

    cedulas_2025 = set(df_2025['CEDULA'].unique()) if not df_2025.empty else set()
    cedulas_2026 = set(df_2026['CEDULA'].unique()) if not df_2026.empty else set()

    merged = pd.concat([df_2025, df_2026], ignore_index=True)
    mask_dup = (merged['_source'] == '2025') & (merged['CEDULA'].isin(cedulas_2026))
    merged = merged[~mask_dup].copy()
    
    merged['is_new'] = merged['CEDULA'].isin(cedulas_2026 - cedulas_2025)
    merged['Status'] = merged['Status'].fillna('No Inscrito').replace('', 'No Inscrito')
    merged['inscrito'] = merged['Status'].apply(lambda x: "✅" if x in ["Pendiente", "Verificado"] else "❌")
    
    merged['distrito_final'] = merged['DISTRITO'].astype(str).str.strip()
    
    if districts:
        merged = merged[merged['distrito_final'].isin(districts)]
        
    return merged

def load_merged_data(districts): return run_async(_load_merged_data(districts))

def render_admin_charts(df_input):
    if df_input.empty:
        st.write("No hay datos suficientes para graficar.")
        return

    # 1. Gráfico de Dona - Estatus
    st.write("#### 🥯 Distribución por Estatus")
    status_counts = df_input['Status'].value_counts().reset_index()
    status_counts.columns = ['Status', 'Cantidad']
    
    donut = alt.Chart(status_counts).mark_arc(innerRadius=60).encode(
        theta=alt.Theta(field="Cantidad", type="quantitative"),
        color=alt.Color(field="Status", type="nominal", scale=alt.Scale(scheme='tableau10')),
        tooltip=['Status', 'Cantidad']
    ).properties(height=300)
    st.altair_chart(donut, use_container_width=True)

    col1, col2 = st.columns(2)
    
    # 2. Status X Categoría (Grouped Bar)
    with col1:
        st.write("#### 📊 Estatus por Categoría")
        df_cat_stat = df_input[df_input['Status'].isin(['Pendiente', 'Verificado'])]
        if not df_cat_stat.empty:
            df_grouped = df_cat_stat.groupby(['CATEGORIA', 'Status']).size().reset_index(name='Total')
            bar_cat_stat = alt.Chart(df_grouped).mark_bar().encode(
                x=alt.X('Status:N', title=None),
                y=alt.Y('Total:Q', title='Registros'),
                color=alt.Color('Status:N', scale=alt.Scale(scheme='set2')),
                column=alt.Column('CATEGORIA:N', title='Categoría', header=alt.Header(labelOrient='bottom'))
            ).properties(height=300, width=60)
            st.altair_chart(bar_cat_stat)
        else:
            st.info("No hay inscritos confirmados.")

    # 3. Inscritos X Categoría (Proporción Inscritos vs Faltantes) - Mejorado con sub-estatus
    with col2:
        st.write("#### 🎯 Meta: Progreso de Registro")
        df_target = df_input.copy()
        
        def set_state_detail(row):
            if row['_source'] == '2025': return '⏳ Faltante'
            if row['Status'] == 'Verificado': return '🟢 Verificado'
            return '🟡 Pendiente'
            
        df_target['Estado'] = df_target.apply(set_state_detail, axis=1)
        
        target_chart = alt.Chart(df_target).mark_bar().encode(
            x=alt.X('CATEGORIA:N', title='Categoría'),
            y=alt.Y('count():Q', stack='normalize', title='Progreso (%)'),
            color=alt.Color('Estado:N', title='Estado', scale=alt.Scale(
                domain=['🟢 Verificado', '🟡 Pendiente', '⏳ Faltante'], 
                range=['#4CAF50', '#FFC107', '#E0E0E0']
            )),
            tooltip=['CATEGORIA', 'Estado', 'count()']
        ).properties(height=300)
        
        st.altair_chart(target_chart, use_container_width=True)

def render_databank_table(df_db):
    if df_db.empty:
        st.write("La tabla databank está vacía o no existe aún.")
        return None
        
    df_show = df_db.copy()
    if 'FECHA_CARGA' in df_show.columns: df_show = df_show.drop(columns=['FECHA_CARGA'])
    if 'id' in df_show.columns: df_show = df_show.drop(columns=['id'])
    
    if 'CEDULA-U' in df_show.columns:
        df_show = df_show.sort_values(by='CEDULA-U', ascending=True)
    
    df_show['MONTO_PAGO'] = pd.to_numeric(df_show['MONTO_PAGO'], errors='coerce').fillna(0)
    df_show['MONTO_A_PAGAR'] = pd.to_numeric(df_show['MONTO_A_PAGAR'], errors='coerce').fillna(0)
    df_show['Monto'] = pd.to_numeric(df_show['Monto'], errors='coerce').fillna(0)
    
    df_show['difUpagos'] = 0.0
    df_show['difRec'] = 0.0
    df_show['difReal'] = 0.0
    mask = df_show['MONTO_PAGO'] > 0
    df_show.loc[mask, 'difUpagos'] = df_show.loc[mask, 'MONTO_PAGO'] - df_show.loc[mask, 'MONTO_A_PAGAR']
    df_show.loc[mask, 'difRec'] = df_show.loc[mask, 'Monto'] - df_show.loc[mask, 'MONTO_PAGO']
    df_show.loc[mask, 'difReal'] = df_show.loc[mask, 'Monto'] - df_show.loc[mask, 'MONTO_A_PAGAR']
    
    # Un registro es Verificado si la diferencia es <= 100 O si ya fue marcado como Verificado en prondamin2026BB
    df_show['Verificado'] = df_show.apply(
        lambda row: "✅" if (abs(row['difReal']) <= 100 or str(row.get('Status', '')) == 'Verificado') else "❌", 
        axis=1
    )
    
    def highlight_row(row):
        val = str(row.get('CEDULA-U', 'No Asignado'))
        is_assigned = val not in ["No Asignado", "", "None", "nan"]
        is_verified = row.get('Verificado') == "✅"
        if is_assigned:
            if is_verified:
                return ['background-color: #4CAF50; color: white;' for _ in row]
            else:
                return ['background-color: #FFFF00; color: #000000;' for _ in row]
        return ['' for _ in row]
    
    def color_diff(val):
        return 'background-color: #4CAF50; color: white;' if val >= 0 else 'background-color: #F44336; color: white;'

    styler = df_show.style.apply(highlight_row, axis=1)
    styler = styler.applymap(color_diff, subset=['difUpagos', 'difRec', 'difReal'])
    styler = styler.format({'difUpagos': "{:.2f}", 'difRec': "{:.2f}", 'difReal': "{:.2f}", 'Monto': "{:.2f}", 'MONTO_PAGO': "{:.2f}", 'MONTO_A_PAGAR': "{:.2f}"})
    
    st.dataframe(styler, use_container_width=True)
    return df_show

def style_user_table(styler):
    def highlight_status(row):
        status = row.get('Status', '')
        if status == 'Verificado':
            return ['background-color: #4CAF50; color: white;' for _ in row]
        elif status == 'Pendiente':
            return ['background-color: #FFFF00; color: #000000;' for _ in row]
        return ['' for _ in row]
    return styler.apply(highlight_status, axis=1)

@st.dialog("Confirmar Registro Manual")
def confirm_manual_verification(to_list):
    st.write(f"¿Desea cambiar a **Verificado** estas {len(to_list)} cuentas?")
    # Mostrar resumen con formato y columna difReal
    df_resumen = to_list[['NOMBRES', 'Referencia', 'Monto', 'difReal']].copy()
    st.dataframe(
        df_resumen.style.format({'Monto': '{:.2f}', 'difReal': '{:.2f}'}),
        use_container_width=True,
        hide_index=True
    )
    
    c1, c2 = st.columns(2)
    if c1.button("✅ SI", use_container_width=True, type="primary"):
        cedulas = to_list['CEDULA-U'].tolist()
        updated = bulk_update_status_verificado(cedulas)
        st.success(f"✅ ¡Cambio de status realizado! Se han verificado {updated} registros correctamente.")
        st.balloons()
        import time
        time.sleep(3)
        st.rerun()
    if c2.button("❌ No", use_container_width=True):
        st.rerun()

def busca_en_turso_pronda26(cedula): return run_async(_busca_en_turso_pronda26(cedula))
def busca_en_turso_pronda25(cedula): return run_async(_busca_en_turso_pronda25(cedula))
def login_admin(username, password): return run_async(_login_admin(username, password))
def verifica_clave_admin_f(clave): return run_async(_verifica_clave_admin_f(clave))

async def _upsert_user_info(cedula, nombres, apellidos, categoria, email, telefonos, distrito):
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        # Check if exists in 2026
        res = await client.execute("SELECT Status FROM prondamin2026BB WHERE CEDULA = ?", [cedula])
        if len(res.rows) > 0:
            # UPDATE
            sql = """
                UPDATE prondamin2026BB 
                SET NOMBRES = ?, APELLIDOS = ?, CATEGORIA = ?, EMAIL = ?, TELEFONOS = ?
                WHERE CEDULA = ?
            """
            await client.execute(sql, [nombres, apellidos, categoria, email, telefonos, cedula])
            return "Actualizado"
        else:
            # INSERT (from 2025 or new)
            sql = """
                INSERT INTO prondamin2026BB 
                (CEDULA, NOMBRES, APELLIDOS, CATEGORIA, DISTRITO, EMAIL, TELEFONOS, 
                 FORMA_PAGO, FECHA_PAGO, MONTO_PAGO, REFERENCIA, ARCHIVO_PAGO, 
                 CURSO_INSCRITO, MONTO_A_PAGAR, MODALIDAD, BancoE, ObservaciónPE, 
                 pagoEn, FECHA_REGISTRO, Status)
                VALUES (?, ?, ?, ?, ?, ?, ?, '-', '-', 0.0, '-', '', '-', 0.0, '-', '-', 'Editado por Admin', '-', ?, 'No Inscrito')
            """
            await client.execute(sql, [cedula, nombres, apellidos, categoria, distrito, email, telefonos, datetime.now().isoformat()])
            return "Insertado"

def upsert_user_info(cedula, nombres, apellidos, categoria, email, telefonos, distrito):
    return run_async(_upsert_user_info(cedula, nombres, apellidos, categoria, email, telefonos, distrito))

async def _upsert_full_user_admin(data):
    cedula = data.get('CEDULA')
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        # Existe?
        res = await client.execute("SELECT 1 FROM prondamin2026BB WHERE CEDULA = ?", [cedula])
        
        # Preparación de valores (asegurando tipos correctos)
        fields = list(data.keys())
        values = list(data.values())
        
        if len(res.rows) > 0:
            # UPDATE
            update_parts = ", ".join([f"{f} = ?" for f in fields if f != 'CEDULA'])
            sql = f"UPDATE prondamin2026BB SET {update_parts} WHERE CEDULA = ?"
            params = [v for f, v in zip(fields, values) if f != 'CEDULA'] + [cedula]
            await client.execute(sql, params)
            return "Actualizado"
        else:
            # INSERT
            placeholders = ", ".join(["?"] * len(fields))
            cols = ", ".join(fields)
            sql = f"INSERT INTO prondamin2026BB ({cols}) VALUES ({placeholders})"
            await client.execute(sql, values)
            return "Insertado"

def upsert_full_user_admin(data):
    return run_async(_upsert_full_user_admin(data))

async def _verifica_referencia_unica(referencia):
    async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
        result = await client.execute("SELECT 1 FROM prondamin2026BB WHERE REFERENCIA = ?", [referencia])
        return len(result.rows) == 0

def verifica_referencia_unica(referencia): return run_async(_verifica_referencia_unica(referencia))

def load_merged_data(districts): return run_async(_load_merged_data(districts))
def insert_registro(values_dict): return run_async(_insert_registro(values_dict))
def get_monto_a_pagar(fecha_str, modalidad="Virtual"):
    async def _get_monto_a_pagar():
        async with libsql.create_client(url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN) as client:
            try:
                result = await client.execute("SELECT * FROM APagar")
                if len(result.rows) > 0:
                    df = pd.DataFrame(result.rows, columns=result.columns)
                    mod_col = next((c for c in df.columns if 'modalidad' in c.lower()), None)
                    if mod_col:
                        # Filtrar por modalidad
                        df_mod = df[df[mod_col].astype(str).str.lower().str.strip() == modalidad.lower().strip()]
                        if not df_mod.empty:
                            df = df_mod
                            
                    amt_col = next((c for c in df.columns if 'monto' in c.lower() or 'pagar' in c.lower()), None)
                    date_col = next((c for c in df.columns if 'fecha' in c.lower()), None)
                    if amt_col and date_col:
                        # Parsear y buscar la fecha mas cercana
                        df['parsed_date'] = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True)
                        target_date = pd.to_datetime(fecha_str, errors='coerce', dayfirst=True) if fecha_str else pd.to_datetime("today")
                        if pd.isna(target_date): target_date = pd.to_datetime("today")
                        
                        df['diff'] = (df['parsed_date'] - target_date).abs()
                        closest = df.loc[df['diff'].idxmin()]
                        return float(closest[amt_col])
            except Exception as e:
                pass
        return 0.0
    return run_async(_get_monto_a_pagar())

def dropbox_to_raw(url: str) -> str:
    if "dl=0" in url or "dl=1" in url: return url.replace("dl=0", "raw=1").replace("dl=1", "raw=1")
    return url + "?raw=1" if url else ""

# -------------------------------------------------------------------
# MANEJO DE ESTADO DE SESIÓN Y ENRRUTAMIENTO STREAMLIT
# -------------------------------------------------------------------
if "page" not in st.session_state: st.session_state.page = "Inicio"
if "user_ctx" not in st.session_state: st.session_state.user_ctx = None
if "CATEGORIAS_LIST" not in st.session_state:
    st.session_state.CATEGORIAS_LIST = ["-"] + CATEGORIAS_BASE
if "DISTRITOS_LIST" not in st.session_state:
    st.session_state.DISTRITOS_LIST = ["-"] + DISTRITOS_BASE

def navigate_to(page_name):
    st.session_state.page = page_name

def base_success_dialog():
    sac.result(
        label="Matriculación exitosa",
        description="Su matriculación en Prondamin2026, ha sido registrada. El status de la misma es PENDIENTE. Cuando se verifique su pago por parte de la Administración de Minec, podrá iniciar el curso correspondiente",
        status="success"
    )
    if st.button("Cerrar", use_container_width=True, key="btn_success_close"):
        navigate_to("Inicio")
        st.rerun()

def base_error_dialog(errores):
    sac.result(
        label='Errores de Validación',
        description='\n\n'.join([f"• {e}" for e in errores]),
        status='error'
    )
    if st.button("Cerrar", use_container_width=True, key="btn_err_close"):
        st.rerun()

dialog_decorator = getattr(st, "dialog", getattr(st, "experimental_dialog", None))
success_dialog = dialog_decorator("Registro")(base_success_dialog) if dialog_decorator else base_success_dialog
error_dialog = dialog_decorator("Revisión Necesaria")(base_error_dialog) if dialog_decorator else base_error_dialog

@st.dialog("Registro Manual de Usuario 2026")
def admin_nuevo_usuario_dialog(allowed_districts=None):
    cedula = st.text_input("Cédula / Pasaporte", placeholder="V-12345678").strip()
    if not cedula:
        st.info("Ingrese una cédula para validar.")
        return
    
    # Validar si ya existe en 2026
    res26 = busca_en_turso_pronda26(cedula)
    if isinstance(res26, pd.DataFrame) and not res26.empty:
        st.error(f"⚠️ El usuario con la cédula **{cedula}** ya está registrado en el Padrón 2026.")
        user_ex = res26.iloc[0]
        st.markdown(f"**Nombre Actual:** {user_ex.get('NOMBRES')} {user_ex.get('APELLIDOS')}")
        st.markdown(f"**Status:** {user_ex.get('Status')}")
        if st.button("Cerrar", use_container_width=True):
            st.rerun()
        return
    
    # Validar si ya existe en 2025
    res25 = busca_en_turso_pronda25(cedula)
    if res25:
        st.warning(f"⚠️ El usuario con la cédula **{cedula}** ya existe en el Padrón 2025.")
        st.info("Este usuario debe realizar su proceso a través del formulario de **Consulta y Registro** en la página de inicio.")
        st.markdown(f"**Nombre en Padrón 2025:** {res25.get('NOMBRES')} {res25.get('APELLIDOS')}")
        if st.button("Cerrar", use_container_width=True):
            st.rerun()
        return

    # Si no existe en ninguno, permitir el registro
    st.success(f"✅ Cédula {cedula} disponible para un registro absolutamente nuevo.")
    
    col1, col2 = st.columns(2)
    nombres = col1.text_input("Nombres")
    apellidos = col2.text_input("Apellidos")
    
    col3, col4 = st.columns(2)
    
    # Lógica para nueva categoría
    cat_options = st.session_state.CATEGORIAS_LIST + ["➕ Nueva Categoría..."]
    categoria_sel = col3.selectbox("Categoría", cat_options, index=0)
    
    if categoria_sel == "➕ Nueva Categoría...":
        nueva_cat = col3.text_input("Nombre de la Nueva Categoría")
        categoria = nueva_cat.strip()
    else:
        categoria = categoria_sel
        
        
    # Filtrar distritos si hay restricción
    final_dist_list = [d for d in st.session_state.DISTRITOS_LIST if allowed_districts is None or d in allowed_districts]
    distrito = col4.selectbox("Distrito", final_dist_list, index=0)
    
    email = st.text_input("Correo Electrónico")
    telefonos = st.text_input("Teléfonos")
    
    st.info("Nota: El administrador solo registra los datos básicos. El usuario deberá ingresar al portal de consulta y registro para completar su pago.")

    if st.button("💾 Guardar Nuevo Usuario", type="primary", use_container_width=True):
        if not nombres or not apellidos:
            st.error("Nombres y Apellidos son obligatorios.")
        else:
            with st.spinner("Registrando..."):
                now_str = datetime.now().isoformat()
                # Si se agregó una nueva categoría y no está en la lista, añadirla permanentemente
                if categoria and categoria not in st.session_state.CATEGORIAS_LIST:
                    st.session_state.CATEGORIAS_LIST.append(categoria)
                
                values_dict = {
                    "CEDULA": cedula, "NOMBRES": nombres, "APELLIDOS": apellidos,
                    "CATEGORIA": categoria, "DISTRITO": distrito, "EMAIL": email, "TELEFONOS": telefonos,
                    "FORMA_PAGO": "-", "FECHA_PAGO": "-", 
                    "MONTO_PAGO": 0.0, "REFERENCIA": "-", "ARCHIVO_PAGO": "", 
                    "CURSO_INSCRITO": "-", "MONTO_A_PAGAR": 0.0, "MODALIDAD": "-",
                    "BancoE": "-", "ObservaciónPE": "Pre-registro Admin", "pagoEn": "-",
                    "FECHA_REGISTRO": now_str, "Status": "No Inscrito"
                }
                try:
                    run_async(_insert_registro(values_dict))
                    st.success(f"¡Usuario {nombres} {apellidos} registrado exitosamente!")
                    st.balloons()
                    import time
                    time.sleep(2)
                    st.rerun()
                except Exception as e:
                    st.error(f"Error al guardar: {e}")


@st.dialog("Edición Administrativa de Usuario")
def admin_manual_edit_dialog(allowed_districts=None):
    cedula = st.text_input("Ingrese la Cédula (ID) del usuario:", placeholder="Ej. 12345678").strip()
    if not cedula:
        st.info("Por favor, ingrese una cédula para comenzar.")
        return
        
    # Buscar datos
    res26 = busca_en_turso_pronda26(cedula)
    res25 = busca_en_turso_pronda25(cedula)
    
    user_data = {}
    is_new = False
    
    if isinstance(res26, pd.DataFrame) and not res26.empty:
        user_data = res26.iloc[0].to_dict()
    elif res25:
        user_data = {
            "CEDULA": cedula,
            "NOMBRES": res25.get('NOMBRES', ''),
            "APELLIDOS": res25.get('APELLIDOS', ''),
            "CATEGORIA": res25.get('CATEGORIA', ''),
            "DISTRITO": res25.get('DISTRITO', ''),
            "EMAIL": res25.get('EMAIL', ''),
            "TELEFONOS": res25.get('TELEFONOS', ''),
            "Status": "No Inscrito"
        }
        is_new = True
    else:
        st.error("Cédula no encontrada en 2025 ni 2026.")
        return
        
    # Verificar permisos de distrito
    u_dist_check = str(user_data.get('DISTRITO', '')).strip()
    if allowed_districts is not None and u_dist_check not in allowed_districts:
        st.error(f"Acceso Denegado: No tienes permisos para editar registros del distrito **{u_dist_check}**.")
        if st.button("Cerrar", use_container_width=True, key="admin_edit_denied_close"):
            st.rerun()
        return
        
    st.write(f"### Edición Manual: {user_data.get('NOMBRES')} {user_data.get('APELLIDOS')}")
    
    # Formulario
    with st.form("form_manual_edit_admin"):
        col_u, col_p = st.columns(2)
        with col_u:
            st.markdown("#### 👤 Datos de Usuario")
            nombres = st.text_input("Nombres", value=str(user_data.get('NOMBRES', '')))
            apellidos = st.text_input("Apellidos", value=str(user_data.get('APELLIDOS', '')))
            
            curr_cat = str(user_data.get('CATEGORIA', '')).strip()
            if curr_cat in ["", "nan", "None"]: curr_cat = "-"
            
            # Asegurar que el actual esté en la lista
            if curr_cat not in st.session_state.CATEGORIAS_LIST:
                st.session_state.CATEGORIAS_LIST.append(curr_cat)
            
            cat_options = st.session_state.CATEGORIAS_LIST + ["➕ Nueva Categoría..."]
            cat_idx = st.session_state.CATEGORIAS_LIST.index(curr_cat)
            categoria_sel = st.selectbox("Categoría", cat_options, index=cat_idx)
            
            if categoria_sel == "➕ Nueva Categoría...":
                nueva_cat = st.text_input("Nombre de la Nueva Categoría")
                categoria = nueva_cat.strip()
            else:
                categoria = categoria_sel
            
            curr_dist = str(user_data.get('DISTRITO', '')).strip()
            if curr_dist in ["", "nan", "None"]: curr_dist = "-"
            
            if curr_dist not in st.session_state.DISTRITOS_LIST:
                st.session_state.DISTRITOS_LIST.append(curr_dist)
            
            dist_idx = st.session_state.DISTRITOS_LIST.index(curr_dist)
            distrito = st.selectbox("Distrito", st.session_state.DISTRITOS_LIST, index=dist_idx)
            
            email = st.text_input("Email", value=str(user_data.get('EMAIL', '')))
            telefonos = st.text_input("Teléfonos", value=str(user_data.get('TELEFONOS', '')))
            
        with col_p:
            st.markdown("#### 💰 Datos de Pago")
            status_actual = user_data.get('Status', 'No Inscrito')
            is_verified = (status_actual == 'Verificado')
            
            if is_verified:
                st.info("✅ El pago ya ha sido asignado y verificado. Campos de pago bloqueados.")
            
            curr_mod = str(user_data.get('MODALIDAD', '')).strip()
            if curr_mod in ["", "-", "nan", "None"]:
                mod_list = ["-", "Presencial", "Virtual"]
                mod_idx = 0
            else:
                mod_list = ["Presencial", "Virtual"]
                mod_idx = 0 if curr_mod == "Presencial" else 1
            modalidad = st.selectbox("Modalidad", mod_list, index=mod_idx, disabled=is_verified)
            
            monto_a_pagar = st.number_input("Monto a Pagar", value=float(user_data.get('MONTO_A_PAGAR', 0.0)), 
                                            disabled=is_verified)
            
            formas = ["Transferencia", "Pago Móvil", "Efectivo", "Otro"]
            curr_forma = str(user_data.get('FORMA_PAGO', '')).strip()
            if curr_forma in ["", "-", "nan", "None"]:
                form_list = ["-"] + formas
                form_idx = 0
            else:
                form_list = formas
                form_idx = formas.index(curr_forma) if curr_forma in formas else 0
            forma_pago = st.selectbox("Forma de Pago", form_list, index=form_idx, disabled=is_verified)
            
            fecha_pago = st.text_input("Fecha de Pago", value=str(user_data.get('FECHA_PAGO', '-')), 
                                      disabled=is_verified)
            
            monto_pagado = st.number_input("Monto Pagado", value=float(user_data.get('MONTO_PAGO', 0.0)), 
                                         disabled=is_verified)
            
            referencia = st.text_input("Referencia", value=str(user_data.get('REFERENCIA', '-')), 
                                      disabled=is_verified)
            
            # Banco Emisor
            curr_banco = str(user_data.get('BancoE', '')).strip()
            if curr_banco in ["", "-", "nan", "None"]:
                banco_list = ["-"] + BANCOS_OPCIONES
                b_idx = 0
            else:
                banco_list = BANCOS_OPCIONES
                b_idx = BANCOS_OPCIONES.index(curr_banco) if curr_banco in BANCOS_OPCIONES else 0
            banco = st.selectbox("Banco Emisor", banco_list, index=b_idx, disabled=is_verified)
            
            pagado_en = st.text_input("Pagado En", value=str(user_data.get('pagoEn', '-')), 
                                     disabled=is_verified)
            
            observacion = st.text_area("Observación", value=str(user_data.get('ObservaciónPE', '')), 
                                      disabled=is_verified)

        if st.form_submit_button("💾 Guardar Cambios en Registro", type="primary", use_container_width=True):
            # Guardar Todo
            # Actualizar listas si hay algo nuevo
            if categoria and categoria != "➕ Nueva Categoría..." and categoria not in st.session_state.CATEGORIAS_LIST:
                st.session_state.CATEGORIAS_LIST.append(categoria)
            if distrito and distrito not in st.session_state.DISTRITOS_LIST:
                st.session_state.DISTRITOS_LIST.append(distrito)

            all_fields = {
                "CEDULA": cedula, "NOMBRES": nombres, "APELLIDOS": apellidos, "CATEGORIA": categoria,
                "DISTRITO": distrito, "EMAIL": email, "TELEFONOS": telefonos,
                "MODALIDAD": modalidad, "MONTO_A_PAGAR": monto_a_pagar, "FORMA_PAGO": forma_pago,
                "FECHA_PAGO": fecha_pago, "MONTO_PAGO": monto_pagado, "REFERENCIA": referencia,
                "BancoE": banco, "pagoEn": pagado_en, "ObservaciónPE": observacion,
                "Status": "Pendiente" if is_new or status_actual == "No Inscrito" else status_actual
            }
            if is_new:
                all_fields["FECHA_REGISTRO"] = datetime.now().isoformat()
                all_fields["ARCHIVO_PAGO"] = ""
                all_fields["CURSO_INSCRITO"] = "-"
                
            try:
                upsert_full_user_admin(all_fields)
                st.success("¡Registro actualizado exitosamente!")
                st.balloons()
                st.rerun()
            except Exception as e:
                st.error(f"Error al guardar: {e}")

if st.session_state.page == "Admin": # Dummy place to keep it for decoration if needed, but st.dialog doesn't need placement
    pass


# Layout Header
c_esp1, c_img1, c_img2, c_esp2 = st.columns([2, 1, 1, 2])
if os.path.exists(os.path.join("assets", "minecLogo.jpeg")):
    c_img1.image(os.path.join("assets", "minecLogo.jpeg"), use_container_width=True)
if os.path.exists(os.path.join("assets", "minecLogoTitle.jpeg")):
    c_img2.image(os.path.join("assets", "minecLogoTitle.jpeg"), use_container_width=True)

st.markdown("<h1 style='text-align: center;'>Portal Prondamin 2026</h1>", unsafe_allow_html=True)
st.divider()

# -------------------------------------------------------------------
# PÁGINAS
# -------------------------------------------------------------------

if st.session_state.page != "Inicio":
    if st.button("← Volver al Inicio"):
        navigate_to("Inicio")
        st.rerun()

# 1. INIT PAGE
if st.session_state.page == "Inicio":
    #st.markdown("### Bienvenidos al Sistema de Gestión Ministerial Prondamin 2026")
    #st.info("Seleccione una opción a continuación para continuar:")
    st.image(os.path.join("assets", "enMantenimiento.png"), use_container_width=True)
    #st.stop()
#===========================================================================
    if st.session_state.user_ctx is None:
        if st.button("🔒 Admin Login", use_container_width=True):
            navigate_to("Login")
            st.rerun()
    else:
        if st.button("📊 Dashboard Admin", use_container_width=True):
            navigate_to("Admin")
            st.rerun()
        if st.button("🚪 Cerrar Sesión", use_container_width=True):
            st.session_state.user_ctx = None
            navigate_to("Inicio")
            st.rerun()
    

#===========================================================================

    # col_nav1, col_nav2 = st.columns(2)
    # with col_nav1:
    #     if st.button("📋 Consulta y Registro", use_container_width=True, type="primary"):
    #         navigate_to("Registro")
    #         st.rerun()
    # with col_nav2:
    #     if st.session_state.user_ctx is None:
    #         if st.button("🔒 Admin Login", use_container_width=True):
    #             navigate_to("Login")
    #             st.rerun()
    #     else:
    #         if st.button("📊 Dashboard Admin", use_container_width=True):
    #             navigate_to("Admin")
    #             st.rerun()
    #         if st.button("🚪 Cerrar Sesión", use_container_width=True):
    #             st.session_state.user_ctx = None
    #             navigate_to("Inicio")
    #             st.rerun()
    
# 2. LOGIN ADMIN
elif st.session_state.page == "Login":
    st.markdown("## Acceso Administrativo")
    with st.form("login_form"):
        user_in = st.text_input("Usuario")
        pass_in = st.text_input("Contraseña", type="password")
        submit_btn = st.form_submit_button("Ingresar")
        
        if submit_btn:
            user_data = login_admin(user_in, pass_in)
            if user_data:
                st.session_state.user_ctx = user_data
                st.success("Acceso Concedido")
                navigate_to("Admin")
                st.rerun()
            else:
                st.error("Credenciales Incorrectas")

# 3. DASHBOARD ADMIN
elif st.session_state.page == "Admin":
    if not st.session_state.user_ctx:
        st.warning("Debes iniciar sesión.")
    else:
        ctx = st.session_state.user_ctx
        tipo_acceso = ctx.get("TipoDeAcceso", "")
        # Parseo del array de distritos desde el texto (ej: "[Andino, Central]")
        import ast
        try:
            admin_districts = [d.strip() for d in ast.literal_eval(tipo_acceso)] if tipo_acceso.startswith("[") else []
        except:
            admin_districts = [i.strip() for i in tipo_acceso[1:-1].split(",")] if tipo_acceso.startswith("[") else []
            
        limited_districts = []
        total_privilege_districts = []
        parsed_districts = []
        for d in admin_districts:
            if d.endswith("-L"):
                raw_d = d[:-2].strip()
                limited_districts.append(raw_d)
                parsed_districts.append(raw_d)
            elif d.endswith("-T"):
                raw_d = d[:-2].strip()
                total_privilege_districts.append(raw_d)
                parsed_districts.append(raw_d)
            else:
                parsed_districts.append(d)
        admin_districts = parsed_districts
        has_t_access = len(total_privilege_districts) > 0
        
        st.markdown(f"## Tablero de Administración - {ctx.get('Nombres')}")
        st.write(f"Rol/Distritos: **{tipo_acceso}**")
        
        is_global = tipo_acceso.strip(" []").lower() in ["total", "develop", "financiero"]
        
        # Botones de Acción (Global o con acceso Total -T)
        if (is_global or has_t_access) and tipo_acceso.strip(" []").lower() != "financiero":
            c_adm1, c_adm2 = st.columns(2)
            # Pasamos los distritos permitidos si no es global
            allowed = total_privilege_districts if not is_global else None
            
            if c_adm1.button("➕ Nuevo Usuario (Padrón 2026)", use_container_width=True):
                admin_nuevo_usuario_dialog(allowed_districts=allowed)
            if c_adm2.button("👤 Editar Usuario (Registro Manual)", use_container_width=True):
                admin_manual_edit_dialog(allowed_districts=allowed)
        elif is_global:
            if st.button("👤 Editar Usuario (Registro Manual)", use_container_width=True):
                admin_manual_edit_dialog()

        if not admin_districts and not is_global:

            st.warning(f"No hay registros visibles para tu rol.")
        else:
            distritos_a_listar = st.session_state.DISTRITOS_LIST if is_global else admin_districts
            df_full = load_merged_data(distritos_a_listar)
            
            if df_full.empty:
                st.write("No hay registros en Turso para tus distritos.")
            else:
                if is_global or has_t_access:
                    st.info(f"Panel {tipo_acceso} cargado. Acceso {'Global' if is_global else 'Especial (T)'}.")
                
                if is_global:
                    # SECCIÓN COMBINADA AL PRINCIPIO
                    with st.expander("📊 COMBINADO (Todos los Distritos)", expanded=True):
                        tab1, tab2, tab3 = st.tabs(["Estadísticas Globales", "Tabla Completa", "🏦 Movimientos DataBank"])
                        with tab1:
                            # Métricas Globales (Filtradas)
                            col_a, col_b, col_c = st.columns(3)
                            
                            p_pending = df_full['Status'].eq('Pendiente').sum()
                            p_verified = df_full['Status'].eq('Verificado').sum()
                            
                            col_a.metric("Total Muestra Filtrada", len(df_full))
                            col_b.metric("🟡 Inscritos Pendientes", p_pending)
                            col_c.metric("🟢 Inscritos Verificados", p_verified)
                            
                            # Métricas de Calidad de Datos (Solo para acceso Total/Develop)
                            if is_global:
                                st.markdown("---")
                                st.markdown("#### 🔍 Auditoría de Datos (Registros en Base de Datos)")
                                # Obtenemos data cruda de 2026 para comparar
                                df_2026_raw = run_async(_get_df_from_turso("prondamin2026BB"))
                                total_db_2026 = len(df_2026_raw)
                                # Registros que no coinciden con la lista oficial de DISTRITOS
                                df_errores = df_2026_raw[~df_2026_raw['DISTRITO'].isin(st.session_state.DISTRITOS_LIST)]
                                count_errores = len(df_errores)
                                
                                col_q1, col_q2 = st.columns(2)
                                col_q1.metric("Registros Totales en DB (2026)", total_db_2026)
                                if count_errores > 0:
                                    col_q2.metric("Distritos por Corregir", count_errores, delta=f"-{count_errores} omitidos", delta_color="inverse")
                                    st.error(f"⚠️ Atención: Hay **{count_errores}** registros con nombres de distrito no reconocidos. Estos registros han sido excluidos de las estadísticas superiores.")
                                    
                                    # Mostrar tabla de registros con errores
                                    st.write("### 📋 Registros con Errores de Distrito (Para corregir en DB)")
                                    st.dataframe(df_errores[['NOMBRES', 'APELLIDOS', 'CEDULA', 'DISTRITO', 'TELEFONOS']], use_container_width=True)
                                    
                                    # Mostrar cuáles son los distritos erróneos
                                    dist_erroneos = df_errores['DISTRITO'].unique().tolist()
                                    st.write(f"Valores no válidos detectados en DB: **{', '.join(dist_erroneos)}**")
                                else:
                                    col_q2.metric("Distritos por Corregir", 0)
                                    st.success("Toda la data de distritos coincide con la lista oficial.")

                                # --- NUEVA AUDITORÍA DE PAGOS Y REFERENCIAS ---
                                st.markdown("#### 💳 Auditoría de Pagos y Referencias (2026)")
                                
                                # Preparar datos numéricos para cálculo
                                df_2026_raw['MP_N'] = pd.to_numeric(df_2026_raw['MONTO_PAGO'], errors='coerce').fillna(0)
                                df_2026_raw['MA_N'] = pd.to_numeric(df_2026_raw['MONTO_A_PAGAR'], errors='coerce').fillna(0)
                                df_2026_raw['R_STR'] = df_2026_raw['REFERENCIA'].astype(str).str.strip()
                                
                                # Condiciones
                                c1 = df_2026_raw['R_STR'].isin(['', 'None', 'nan', '0']) # No existe
                                c2 = df_2026_raw['R_STR'].apply(lambda x: len(x) < 6 and x not in ['', 'None', 'nan', '0']) # Corta
                                c3 = df_2026_raw['MP_N'] == 0 # Monto 0
                                c4 = (df_2026_raw['MP_N'] - df_2026_raw['MA_N']).abs() > 100 # Diferencia > 100
                                
                                df_pago_err = df_2026_raw[c1 | c2 | c3 | c4].copy()
                                
                                if not df_pago_err.empty:
                                    st.warning(f"Se detectaron **{len(df_pago_err)}** registros con posibles inconsistencias en pagos o referencias.")
                                    # Mostrar tabla detallada
                                    st.dataframe(df_pago_err[['NOMBRES', 'APELLIDOS', 'CEDULA', 'REFERENCIA', 'MONTO_PAGO', 'MONTO_A_PAGAR', 'DISTRITO']], use_container_width=True)
                                    
                                    # Resumen de motivos
                                    motivos = []
                                    if c1.any(): motivos.append("Referencias Vacías")
                                    if c2.any(): motivos.append("Referencias cortas (< 6 dígitos)")
                                    if c3.any(): motivos.append("Monto Reportado es 0")
                                    if c4.any(): motivos.append("Discrepancia de monto > 100")
                                    st.info(f"Motivos detectados: {', '.join(motivos)}")
                                else:
                                    st.success("No se detectaron inconsistencias críticas en los pagos de 2026.")

                            st.write("---")
                            render_admin_charts(df_full)
                        with tab2:
                            df_table_global = df_full[['NOMBRES', 'APELLIDOS', 'CEDULA', 'DISTRITO', 'CATEGORIA', 'inscrito', 'Status', 'REFERENCIA', 'CURSO_INSCRITO']]
                            st.dataframe(style_user_table(df_table_global.style), use_container_width=True)
                        with tab3:
                            st.markdown("### Tabla databank (Cargas Bancarias)")
                            render_databank_table(get_databank_df())
                
                # SECCIÓN CARGA DE DATA BANCARIA (Solo para Total, Develop y Financiero)
                if tipo_acceso.strip(" []").lower() in ["total", "develop", "financiero"]:
                    with st.expander("📂 Carga y proceso de data bancaria", expanded=False):
                        st.markdown("### Importar Movimientos Bancarios (.xlsx)")
                        st.info("El archivo debe contener los datos a partir de la fila 9. Al subir, se mostrará una vista previa.")
                        
                        uploaded_xlsx = st.file_uploader("Subir archivo Excel", type=["xlsx"], key="bank_xlsx_uploader")
                        
                        if uploaded_xlsx:
                            try:
                                # Guardar y mostrar vista previa inmediatamente
                                if not os.path.exists("uploads"):
                                    os.makedirs("uploads")
                                
                                # Guardar archivo (opcional si ya se lee de memoria, pero lo mantengo por la solicitud previa)
                                file_path = os.path.join("uploads", uploaded_xlsx.name)
                                with open(file_path, "wb") as f:
                                    f.write(uploaded_xlsx.getbuffer())
                                
                                # Procesar Excel para Vista Previa
                                # Header en fila 9 (skiprows=8)
                                df_bank = pd.read_excel(uploaded_xlsx, sheet_name=0, skiprows=8)
                                
                                # Eliminar filas completamente vacías
                                df_bank = df_bank.dropna(how='all').reset_index(drop=True)
                                
                                # Filtrar por Tipo=='NC' y Referencia!='0'
                                if 'Tipo' in df_bank.columns and 'Referencia' in df_bank.columns:
                                    df_bank['Tipo'] = df_bank['Tipo'].astype(str).str.strip()
                                    df_bank['Referencia'] = df_bank['Referencia'].astype(str).str.strip()
                                    # Quitar .0 si es float
                                    df_bank['Referencia'] = df_bank['Referencia'].apply(lambda x: x[:-2] if x.endswith(".0") else x)
                                    
                                    df_bank = df_bank[(df_bank['Tipo'] == 'NC') & (df_bank['Referencia'] != '0')]
                                
                                # Filtrar columnas requeridas
                                required_cols = ["Tipo", "Fecha", "Referencia", "Descripción", "Monto Bs."]
                                missing = [c for c in required_cols if c not in df_bank.columns]
                                
                                if not missing:
                                    df_preview = df_bank[required_cols]
                                    st.write(f"**Vista Previa de Datos ({len(df_preview)} registros encontrados):**")
                                    st.dataframe(df_preview, use_container_width=True)
                                    
                                    # BOTÓN DE PROCESO (Confirmación después de vista previa)
                                    if st.button("🚀 Confirmar Carga a DataBank", use_container_width=True, type="primary"):
                                        with st.status("Procesando movimientos bancarios...", expanded=True) as stats:
                                            st.write("Asegurando tabla de destino y conectando...")
                                            check_and_create_databank_table()
                                            
                                            st.write(f"Preparando {len(df_preview)} registros para verificación...")
                                            added = insert_bank_data(df_preview)
                                            
                                            if added > 0:
                                                stats.update(label=f"¡Éxito! Se agregaron {added} nuevos registros.", state="complete", expanded=False)
                                                st.success(f"Se procesaron {len(df_preview)} filas. {added} fueron nuevas.")
                                                st.session_state.bank_import_success = True # Flag para mostrar el botón de cruce
                                                st.balloons()
                                            else:
                                                stats.update(label="No se detectaron nuevos registros para agregar.", state="complete", expanded=False)
                                                st.warning("Todos los registros ya existían en la base de datos (según Fecha, Referencia y Descripción).")
                                                st.session_state.bank_import_success = True # También activar flag aquí para permitir cruce
                                    
                                    # MOSTRAR BOTONES DE CRUCE SI HUBO ÉXITO O FORZADO (Fuera de st.button para persistir)
                                    if st.session_state.get("bank_import_success"):
                                        st.divider()
                                        st.markdown("#### Tareas de Post-Procesamiento")
                                        c_p1, c_p2 = st.columns(2)
                                        if c_p1.button("🔄 Procesar pagos2026", use_container_width=True):
                                            with st.spinner("Realizando cruce con prondamin2026BB..."):
                                                assigned, total = process_pagos_2026()
                                                st.success(f"Cruce completado: {assigned} de {total} registros vinculados.")
                                                st.session_state.show_process_results = True
                                        
                                        if st.session_state.get("show_process_results"):
                                            st.divider()
                                            st.markdown("### 📋 Resultados del Cruce (Tabla DataBank)")
                                            df_results = render_databank_table(get_databank_df())
                                            
                                            if df_results is not None:
                                                if st.button("🚀 registrar los Verificados", use_container_width=True, type="primary"):
                                                    # Filtrar cedulas con Verificado == ✅ y Cedula-U != No Asignado
                                                    to_verify = df_results[
                                                        (df_results['CEDULA-U'] != "No Asignado") & 
                                                        (df_results['Verificado'] == "✅")
                                                    ]['CEDULA-U'].tolist()
                                                    
                                                    if to_verify:
                                                        with st.spinner(f"Actualizando {len(to_verify)} usuarios..."):
                                                            updated = bulk_update_status_verificado(to_verify)
                                                            st.success(f"Se han actualizado {updated} usuarios a status 'Verificado'.")
                                                            st.balloons()
                                                            st.session_state.show_process_results = False
                                                            # st.rerun() # Opcional para refrescar estadisticas inmediatamente
                                                    else:
                                                        st.warning("No se encontraron registros asignados con el sello ✅ para registrar.")
                                        
                                        if c_p2.button("🧹 Limpiar Estado de Carga", use_container_width=True):
                                            st.session_state.bank_import_success = False
                                            st.rerun()
                                else:
                                    st.error(f"El archivo no tiene las columnas requeridas: {missing}")
                                    st.write("Columnas detectadas:", list(df_bank.columns))
                                        
                            except Exception as e:
                                st.error(f"Error procesando el archivo: {e}")
                                            
                # SECCIÓN VERIFICACIÓN MANUAL (Total, Financiero, Develop)
                if tipo_acceso.strip(" []").lower() in ["total", "financiero", "develop"]:
                    with st.expander("✅ Verificación Manual", expanded=False):
                        st.markdown("### Databank Verificación Manual")
                        st.info("Aquí se muestran los registros que no pudieron ser verificados automáticamente (❌).")
                        
                        df_db_raw = get_databank_df()
                        if not df_db_raw.empty:
                            # Procesamiento mínimo para mostrar en el editor
                            df_m = df_db_raw.copy()
                            df_m['Monto'] = pd.to_numeric(df_m['Monto'], errors='coerce').fillna(0)
                            df_m['MONTO_A_PAGAR'] = pd.to_numeric(df_m['MONTO_A_PAGAR'], errors='coerce').fillna(0)
                            df_m['difReal'] = df_m['Monto'] - df_m['MONTO_A_PAGAR']
                            
                            # Un registro está verificado si la diferencia es <= 100 o si el Status ya es 'Verificado' en prondamin2026BB
                            df_m['Verificado_Check'] = df_m.apply(
                                lambda row: abs(row['difReal']) <= 100 or str(row.get('Status', '')) == 'Verificado', 
                                axis=1
                            )
                            
                            # Filtrar solo los NO verificados y que tengan CEDULA-U asignada
                            df_to_edit = df_m[(df_m['Verificado_Check'] == False) & (df_m['CEDULA-U'] != "No Asignado")].copy()
                            
                            if not df_to_edit.empty:
                                df_to_edit['Manual'] = False  # Columna para el checkbox
                                
                                # Columnas a mostrar
                                cols_show = ['NOMBRES', 'APELLIDOS', 'CEDULA-U', 'Fecha', 'Referencia', 'Monto', 'MONTO_A_PAGAR', 'difReal', 'Manual']
                                
                                edited_df = st.data_editor(
                                    df_to_edit[cols_show],
                                    column_config={
                                        "Manual": st.column_config.CheckboxColumn("Seleccionar", help="Marque para verificar manualmente"),
                                        "NOMBRES": st.column_config.Column(disabled=True),
                                        "APELLIDOS": st.column_config.Column(disabled=True),
                                        "CEDULA-U": st.column_config.Column(disabled=True),
                                        "Fecha": st.column_config.Column(disabled=True),
                                        "Referencia": st.column_config.Column(disabled=True),
                                        "Monto": st.column_config.Column(disabled=True),
                                        "MONTO_A_PAGAR": st.column_config.Column(disabled=True),
                                        "difReal": st.column_config.Column(disabled=True),
                                    },
                                    hide_index=True,
                                    use_container_width=True,
                                    key="editor_manual"
                                )
                                
                                if st.button("🚀 Cambio Manual a Verificado", use_container_width=True, type="primary"):
                                    to_verify = edited_df[edited_df['Manual'] == True]
                                    if not to_verify.empty:
                                        confirm_manual_verification(to_verify)
                                    else:
                                        st.warning("No ha seleccionado ningún registro.")
                            else:
                                st.success("No hay registros pendientes de verificación manual con cédula asignada.")
                        else:
                            st.write("No hay datos en Databank.")


                # LISTADO POR DISTRITO
                for dist in distritos_a_listar:
                    is_limited = dist in limited_districts
                    is_total_dist = dist in total_privilege_districts
                    dist_df = df_full[df_full['distrito_final'] == dist]
                    with st.expander(f"Distrito: {dist} ({len(dist_df)} Registros)", expanded=not is_global):
                        if is_limited or is_total_dist:
                            tab1, tab2 = st.tabs(["Estadísticas", "Tabla de Datos"])
                        else:
                            tab1, tab2, tab3 = st.tabs(["Estadísticas", "Tabla de Datos", "👤 Editar Usuario"])
                            
                        with tab1:
                            col_a, col_b, col_c = st.columns(3)
                            
                            d_pending = dist_df['Status'].eq('Pendiente').sum()
                            d_verified = dist_df['Status'].eq('Verificado').sum()
                            
                            col_a.metric("Total Padrón 2025", len(dist_df))
                            col_b.metric("🟡 Pendientes", d_pending)
                            col_c.metric("🟢 Verificados", d_verified)
                            
                            st.write("---")
                            render_admin_charts(dist_df)
                            
                        with tab2:
                            if is_limited:
                                df_table_dist = dist_df[['NOMBRES', 'APELLIDOS', 'CEDULA', 'CATEGORIA', 'EMAIL', 'TELEFONOS', 'inscrito', 'Status']]
                            else:
                                df_table_dist = dist_df[['NOMBRES', 'APELLIDOS', 'CEDULA','CATEGORIA', 'inscrito', 'Status', 'REFERENCIA', 'CURSO_INSCRITO']]
                            st.dataframe(style_user_table(df_table_dist.style), use_container_width=True)

                        if not is_limited and not is_total_dist:
                            with tab3:
                                st.write(f"### ✏️ Editar Usuario de Distrito: {dist}")
                                user_list = [f"{r['NOMBRES']} {r['APELLIDOS']} ({r['CEDULA']})" for _, r in dist_df.iterrows()]
                                selected_label = st.selectbox("Seleccione el usuario a editar:", options=user_list, key=f"edit_sel_{dist}")
                                
                                if selected_label:
                                    ced_to_edit = selected_label.split("(")[-1].strip(")")
                                    user_to_edit = dist_df[dist_df['CEDULA'] == ced_to_edit].iloc[0]
                                    
                                    # Formulario sin usar st.form para permitir inputs dinámicos si se requiere, 
                                    # pero st.form es mejor para botones de guardado aislados
                                    with st.form(key=f"form_edit_{ced_to_edit}_{dist}"):
                                        st.write(f"Editando Cédula: **{ced_to_edit}**")
                                        e_nombres = st.text_input("Nombres", value=str(user_to_edit.get('NOMBRES', '')))
                                        e_apellidos = st.text_input("Apellidos", value=str(user_to_edit.get('APELLIDOS', '')))
                                        
                                        cat_val = str(user_to_edit.get('CATEGORIA', '')).strip()
                                        if cat_val in ["", "nan", "None"]: cat_val = "-"
                                        
                                        if cat_val not in st.session_state.CATEGORIAS_LIST:
                                            st.session_state.CATEGORIAS_LIST.append(cat_val)
                                            
                                        e_cat_options = st.session_state.CATEGORIAS_LIST + ["➕ Nueva Categoría..."]
                                        e_cat_idx = st.session_state.CATEGORIAS_LIST.index(cat_val)
                                        e_cat_sel = st.selectbox("Categoría", e_cat_options, index=e_cat_idx)
                                        
                                        if e_cat_sel == "➕ Nueva Categoría...":
                                            e_nueva_cat = st.text_input("Nombre de la Nueva Categoría", key=f"new_cat_{ced_to_edit}")
                                            e_categoria = e_nueva_cat.strip()
                                        else:
                                            e_categoria = e_cat_sel
                                        
                                        e_email = st.text_input("Correos", value=str(user_to_edit.get('EMAIL', '')))
                                        e_telefonos = st.text_input("Teléfonos", value=str(user_to_edit.get('TELEFONOS', '')))
                                        
                                        st.info(f"Distrito fijo: {dist}")
                                        
                                        submit_edit = st.form_submit_button("Guardar Cambios", type="primary", use_container_width=True)
                                        
                                        if submit_edit:
                                                # Actualizar lista de categorías si es nueva
                                                if e_categoria and e_categoria != "➕ Nueva Categoría..." and e_categoria not in st.session_state.CATEGORIAS_LIST:
                                                    st.session_state.CATEGORIAS_LIST.append(e_categoria)
                                                
                                                res_msg = upsert_user_info(ced_to_edit, e_nombres, e_apellidos, e_categoria, e_email, e_telefonos, dist)
                                                st.success(f"Usuario {e_nombres} {e_apellidos} {res_msg} correctamente.")
                                                st.balloons()
                                                # st.rerun() # Opcional, dependiendo de si queremos refrescar todo el panel

# 4. REGISTRO Y CONSULTA
elif st.session_state.page == "Registro":
    st.markdown("## Consulta y Registro 2026")
    
    cedula_input = st.text_input("Ingrese su Cédula o Pasaporte:", placeholder="Ej. 12345678").strip()
    
    if cedula_input:
        res_turso26 = busca_en_turso_pronda26(cedula_input)
        res_turso25 = busca_en_turso_pronda25(cedula_input)
        user_source = "new"
        
        # Diccionarios Defaults
        defaults = {"NOMBRES": "", "APELLIDOS": "", "CATEGORIA": "", "DISTRITO": "", "EMAIL": "", "TELEFONOS": ""}
        pago_info = None
        
        if isinstance(res_turso26, pd.DataFrame) and not res_turso26.empty:
            st.success("Cédula encontrada en Padrón 2026.")
            defaults = res_turso26.iloc[0].to_dict()
            user_source = "2026"
            pago_info = defaults
        elif res_turso25:
            user_source = "2025"
            defaults.update({
                "NOMBRES": res_turso25.get('NOMBRES2025', res_turso25.get('NOMBRES', '')),
                "APELLIDOS": res_turso25.get('APELLIDOS2025', res_turso25.get('APELLIDOS', '')),
                "CATEGORIA": res_turso25.get('CATEGORIA2025', res_turso25.get('CATEGORIA', '')),
                "DISTRITO": res_turso25.get('DISTRITO2025', res_turso25.get('DISTRITO', '')),
                "EMAIL": res_turso25.get('EMAIL2025', res_turso25.get('EMAIL', '')),
                "TELEFONOS": res_turso25.get('TELEFONOS2025', res_turso25.get('TELEFONOS', ''))
            })
        else:
            st.error("No se pudo proceder: La cédula no se encuentra en el Padrón 2025.")
            st.info("Solo los usuarios registrados en 2025 pueden realizar su automatriculación. Si eres un nuevo usuario y/o tu cedula no está registrada en nuestra base de datos, por favor contacte a su administrador de distrito quien podrá ingresarlo desde el panel admin.")
            st.stop()

        
        st.write(f"### Bienvenid@ {defaults.get('NOMBRES')} {defaults.get('APELLIDOS')}")
        
        # Certificados Anteriores (Desde 2025 data, asumiendo campos certificado2022 o CERTIFICADO2022)
        with st.expander("Certificados Anteriores (Prondamin)", expanded=False):
            if res_turso25:
                cols = st.columns(4)
                for i, yr in enumerate(["2022", "2023", "2024", "2025"]):
                    cert_val = res_turso25.get(f"certificado{yr}") or res_turso25.get(f"CERTIFICADO{yr}") or ""
                    cert_url = str(cert_val).strip()
                    if cert_url and cert_url not in ["nan", "None", ""]:
                        cert_url = dropbox_to_raw(cert_url)
                        with cols[i]:
                            st.write(f"**{yr}**")
                            # El render de st.image de un URL remoto a veces falla si el host restringe CORS, markdown es mejor fallback:
                            try:
                                st.image(cert_url, use_container_width=True)
                            except:
                                st.markdown(f'<img src="{cert_url}" style="width:100%">', unsafe_allow_html=True)
            else:
                st.write("Sin certificados anteriores disponibles.")
                
        # FORMULARIO
        st.write("---")
        st.markdown("### Formulario de Actualización / Registro 2026")
        
        # DETERMINAR SI ES SOLO LECTURA (Si ya tiene pago registrado)
        has_payment = False
        if user_source == "2026":
            ref = str(defaults.get('REFERENCIA', '')).strip()
            # Si tiene una referencia real (no es guión ni vacío), es read_only
            if ref and ref not in ["-", "", "None", "nan", "0"]:
                has_payment = True
        
        read_only = has_payment
        
        if read_only:
            curr_status = defaults.get('Status', 'Pendiente')
            if curr_status == 'Verificado':
                sac.result(
                    label='Excelente, su Inscripción y pago han sido Verificado',
                    description=f"Status={curr_status}",
                    status='success'
                )
            else:
                sac.result(
                    label='Ya inscrito en espera de revision de pago de la administración.',
                    description=f"Status={curr_status}",
                    status='warning'
                )
        elif user_source == "2026":
            st.info("Usted ha sido pre-registrado por el administrador. Por favor complete sus detalles de pago para finalizar su inscripción.")

        
        # Inicializar Componentes Claves con Session State bindings
        import datetime as dt
        if "reg_fecha" not in st.session_state: st.session_state.reg_fecha = dt.date.today()
        if "reg_monto" not in st.session_state: st.session_state.reg_monto = None
        if "reg_ref" not in st.session_state: st.session_state.reg_ref = ""
        if "processed_file_id" not in st.session_state: st.session_state.processed_file_id = ""
        
        c1, c2 = st.columns(2)
        nombres = c1.text_input("Nombres", value=defaults.get('NOMBRES'), disabled=read_only)
        apellidos = c2.text_input("Apellidos", value=defaults.get('APELLIDOS'), disabled=read_only)
        
        c3, c4 = st.columns(2)
        
        # Normalizar Categoría para Registro
        val_cat = str(defaults.get("CATEGORIA", "")).strip()
        if val_cat in ["", "nan", "None"]: val_cat = "-"
        if val_cat not in st.session_state.CATEGORIAS_LIST:
            st.session_state.CATEGORIAS_LIST.append(val_cat)
        
        cat_idx = st.session_state.CATEGORIAS_LIST.index(val_cat)
        categoria = c3.selectbox("Categoría", st.session_state.CATEGORIAS_LIST, index=cat_idx, disabled=True)
        
        # Normalizar Distrito para Registro
        val_dist = str(defaults.get("DISTRITO", "")).strip()
        if val_dist in ["", "nan", "None"]: val_dist = "-"
        if val_dist not in st.session_state.DISTRITOS_LIST:
            st.session_state.DISTRITOS_LIST.append(val_dist)
            
        dist_idx = st.session_state.DISTRITOS_LIST.index(val_dist)
        distrito = c4.selectbox("Distrito", st.session_state.DISTRITOS_LIST, index=dist_idx, disabled=True)
        
        
        emails = st.text_input("Correos Electrónicos", value=defaults.get('EMAIL'), disabled=read_only)
        telefonos = st.text_input("Teléfonos", value=defaults.get('TELEFONOS'), disabled=read_only)
        
        if not read_only:
            st.write("---")
            st.write("#### Detalles de Pago")
        
            modalidad = sac.chip(
                items=[sac.ChipItem('Virtual'), sac.ChipItem('Presencial')],
                label='Modalidad',
                index=0,
                align='start'
            )
            if not modalidad: modalidad = 'Virtual'
            
            # Obtenemos monto a pagar de la DB APagar
            fecha_str_eval = st.session_state.reg_fecha.strftime("%d-%m-%Y") if isinstance(st.session_state.reg_fecha, dt.date) else ""
            monto_oficial = get_monto_a_pagar(fecha_str_eval, modalidad)
            st.info(f"**Monto a Pagar ({modalidad}):** {monto_oficial:.2f}")
            forma_pago = sac.chip(
                items=[sac.ChipItem('Pago Móvil'), sac.ChipItem('Transferencia'), sac.ChipItem('Otro')],
                label='Forma de Pago',
                index=0,
                align='start'
            )
            if not forma_pago: forma_pago = 'Pago Móvil'
        else:
            forma_pago = "Otro"
            modalidad = "Virtual"
        
        uploaded_file = None
        banco = ""
        observacion_pago = ""
        moneda = ""
        admin_valid = False

        if read_only:
            fecha_pago = dt.date.today()
            monto_pago = 0.0
            referencia_pago = ""
        else:
            if forma_pago == "Otro":
                clave_admf = st.text_input("Clave AdminF", type="password")
                if clave_admf:
                    if verifica_clave_admin_f(clave_admf):
                        admin_valid = True
                    else:
                        st.error("Clave Incorrecta o no autorizada.")
                
                if admin_valid:
                    moneda = sac.chip(
                        items=[sac.ChipItem('Dólares'), sac.ChipItem('Bolívares')],
                        label='Pagado en',
                        index=0,
                        align='start'
                    )
                    if not moneda: moneda = 'Dólares'
                    
                    observacion_pago = st.text_input("Observación a pago extraordinario")
                    c5, c6, c7 = st.columns(3)
                    try:
                        fecha_pago = c5.date_input("Fecha de Pago", key="reg_fecha", format="DD-MM-YYYY")
                    except:
                        fecha_pago = c5.date_input("Fecha de Pago", key="reg_fecha")
                    monto_pago = c6.number_input("Monto Pagado", key="reg_monto", format="%.2f", min_value=0.00, max_value=999999.99, placeholder=f"{monto_oficial:.2f}")
                    # Referencia libre
                    st.session_state.reg_ref = str(st.session_state.reg_ref)
                    referencia_pago = c7.text_input("Referencia", key="reg_ref", placeholder="Referencia libre")
                else:
                    fecha_pago = dt.date.today()
                    monto_pago = 0.0
                    referencia_pago = ""
            else:
                c5, c6, c7 = st.columns(3)
                try:
                    fecha_pago = c5.date_input("Fecha de Pago", key="reg_fecha", format="DD-MM-YYYY", help="Fecha en la que realizó el pago.")
                except:
                    fecha_pago = c5.date_input("Fecha de Pago", key="reg_fecha")
                monto_pago = c6.number_input("Monto Pagado", key="reg_monto", format="%.2f", min_value=0.00, max_value=999999.99, placeholder=f"{monto_oficial:.2f}", help="Monto exacto que pagó. En Bolívares. Máximo 6 dígitos enteros y 2 decimales")
                
                if forma_pago == "Transferencia":
                    banco = c5.selectbox("Banco Emisor", options=BANCOS_OPCIONES)
                    st.session_state.reg_ref = ''.join(filter(str.isdigit, st.session_state.reg_ref))[:8]
                    referencia_pago = c7.text_input("Referencia", key="reg_ref", max_chars=8, placeholder="########", help="Ingresa los dígitos de su comprobante (mínimo 6 dígitos). Solo números.")
                else: # Pago Móvil
                    st.session_state.reg_ref = ''.join(filter(str.isdigit, st.session_state.reg_ref))[:6]
                    referencia_pago = c7.text_input("Referencia", key="reg_ref", max_chars=6, placeholder="######", help="Ingresa los últimos 6 dígitos de la referencia. Solo números (máximo 6)")

        if read_only or (forma_pago == "Otro" and not admin_valid):
            guardar = st.button("Procesar Registro", type="primary", use_container_width=True, disabled=True)
        else:
            guardar = st.button("Procesar Registro", type="primary", use_container_width=True)
        
        if guardar:
            errores = []
            
            nombres = nombres or ""
            apellidos = apellidos or ""
            categoria = categoria or ""
            distrito = distrito or ""
            telefonos = telefonos or ""
            emails = emails or ""
            referencia_pago = referencia_pago or ""

            if not nombres.strip(): errores.append("Nombres es obligatorio.")
            if not apellidos.strip(): errores.append("Apellidos es obligatorio.")
            if not categoria.strip(): errores.append("Categoría es obligatoria.")
            if not distrito.strip(): errores.append("Distrito es obligatorio.")
            if not telefonos.strip(): errores.append("Teléfonos es obligatorio.")
            
            import re
            if not emails.strip():
                errores.append("Correos es obligatorio.")
            else:
                lista_emails = [e.strip() for e in emails.replace(",", " ").split() if e.strip()]
                regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
                for email in lista_emails:
                    if not re.match(regex, email):
                        errores.append(f"El correo '{email}' no tiene un formato válido.")
            
            monto_pago = monto_pago or 0.0
            if monto_pago <= 0:
                errores.append("El Monto Pagado es obligatorio y debe ser mayor a 0.")
                
            if forma_pago in ["Pago Móvil", "Transferencia"]:
                if abs(monto_pago - monto_oficial) > 100:
                    errores.append(f"La diferencia entre el Monto Pagado ({monto_pago}) y a Pagar ({monto_oficial:.2f}) supera el margen permitido de 100.")
            
            if not referencia_pago.strip():
                errores.append("La Referencia es obligatoria.")
            else:
                if forma_pago != "Otro":
                    if not referencia_pago.isdigit():
                        errores.append("La referencia solo puede contener números para Pago Móvil / Transferencia.")
                    elif len(referencia_pago.strip()) < 6:
                        errores.append("La referencia debe tener al menos 6 dígitos para Pago Móvil o Transferencia.")
                
                # Check Uniqueness for all cases
                if not verifica_referencia_unica(referencia_pago):
                    errores.append(f"La referencia '{referencia_pago}' ya se encuentra registrada en el sistema.")
                
            if errores:
                error_dialog(errores)
            else:
                with st.spinner("Subiendo datos y captura a Turso y R2..."):
                    img_path = ""
                    # Subir a R2
                    if uploaded_file and s3_client:
                        fname = f"capture_{cedula_input}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uploaded_file.name}"
                        try:
                            s3_client.put_object(
                                Bucket=R2_BUCKET_NAME, Key=fname,
                                Body=uploaded_file.getvalue(), ContentType=uploaded_file.type
                            )
                            img_path = f"{R2_PUBLIC_URL}/{fname}".replace('//capture', '/capture') if R2_PUBLIC_URL else fname
                        except Exception as e:
                            st.error(f"Error subiendo a R2: {e}")

                    # Formateo String de Fecha
                    fecha_str = fecha_pago.strftime("%d-%m-%Y") if fecha_pago else ""

                    # Insertar a BD Turso (Agregar MONTO_A_PAGAR como monto_oficial loggeado)
                    values_dict = {
                        "CEDULA": cedula_input, "NOMBRES": nombres, "APELLIDOS": apellidos,
                        "CATEGORIA": categoria, "DISTRITO": distrito, "EMAIL": emails, "TELEFONOS": telefonos,
                        "FORMA_PAGO": forma_pago, "FECHA_PAGO": fecha_str, "MONTO_PAGO": monto_pago if monto_pago is not None else 0.0,
                        "REFERENCIA": referencia_pago, "ARCHIVO_PAGO": img_path, "CURSO_INSCRITO": "-",
                        "MONTO_A_PAGAR": monto_oficial,
                        "MODALIDAD": modalidad,
                        "BancoE": banco,
                        "ObservaciónPE": observacion_pago,
                        "pagoEn": moneda,
                        "FECHA_REGISTRO": datetime.now().isoformat(),
                        "Status": "Pendiente"
                    }
                    
                    try:
                        upsert_full_user_admin(values_dict)
                        success_dialog()
                        st.balloons()
                    except Exception as e:
                        st.error(f"Error base de datos: {e}")

# Flujo de Navegación Inferior
if st.session_state.page != "Inicio":
    st.write("---")
    if st.button("← Volver al Inicio", key="btn_volver_abajo"):
        navigate_to("Inicio")
        st.rerun()
