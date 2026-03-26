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

# Listas
CATEGORIAS = ["Ministro Ordenado", "Ministro Licenciado", "Ministro Cristiano", "Ministro Distrital"]
DISTRITOS = ["Andino", "Centro",  "Centro Llanos", "Falcón", "Lara", "Llanos Occidentales",  "Metropolitano", "Nor Oriente", "Sur Oriente",  "Yaracuy", "Zulia" ]
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

# Wrappers para ejecutar asíncronamente en Streamlit
def busca_en_turso_pronda26(cedula): return run_async(_busca_en_turso_pronda26(cedula))
def busca_en_turso_pronda25(cedula): return run_async(_busca_en_turso_pronda25(cedula))
def login_admin(username, password): return run_async(_login_admin(username, password))
def verifica_clave_admin_f(clave): return run_async(_verifica_clave_admin_f(clave))

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
    st.markdown("### Bienvenidos al Sistema de Gestión Ministerial Prondamin 2026")
    st.info("Seleccione una opción a continuación para continuar:")
    #st.image(os.path.join("assets", "enMantenimiento.png"), use_container_width=True)
    #st.stop()
    col_nav1, col_nav2 = st.columns(2)
    with col_nav1:
        if st.button("📋 Consulta y Registro", use_container_width=True, type="primary"):
            navigate_to("Registro")
            st.rerun()
    with col_nav2:
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
            
        st.markdown(f"## Tablero de Administración - {ctx.get('Nombres')}")
        st.write(f"Rol/Distritos: **{tipo_acceso}**")
        
        if not admin_districts and tipo_acceso not in ["Total", "Develop", "[Total]", "[Develop]", "Financiero", "[Financiero]"]:
            st.warning(f"No hay registros visibles para tu rol.")
        elif tipo_acceso in ["Total", "Develop", "[Total]", "[Develop]", "Financiero", "[Financiero]"]:
            st.info(f"Panel {tipo_acceso} cargado. Acceso Global.")
        else:
            df = load_merged_data(admin_districts)
            if df.empty:
                st.write("No hay registros en Turso para tus distritos.")
            else:
                for dist in admin_districts:
                    dist_df = df[df['distrito_final'] == dist]
                    with st.expander(f"Distrito: {dist} ({len(dist_df)} Registros)", expanded=True):
                        tab1, tab2 = st.tabs(["Estadísticas", "Tabla de Datos"])
                        
                        with tab1:
                            col_a, col_b, col_c = st.columns(3)
                            inscritos = dist_df['Status'].isin(['Pendiente', 'Verificado']).sum()
                            aprobados = dist_df['Status'].eq('Verificado').sum()
                            
                            col_a.metric("Total Padrón 2025", len(dist_df))
                            col_b.metric("Inscritos 2026", inscritos)
                            col_c.metric("Aprobados", aprobados)
                            
                            st.write("Estatus de Inscripción:")
                            st.bar_chart(dist_df['Status'].value_counts())
                            
                        with tab2:
                            st.dataframe(dist_df[['NOMBRES', 'APELLIDOS', 'CEDULA','CATEGORIA', 'inscrito', 'Status', 'REFERENCIA', 'CURSO_INSCRITO']], use_container_width=True)

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
        
        read_only = (user_source == "2026")
        
        if read_only:
            sac.result(
                label='Ya inscrito en espera de revision de pago de la administración.',
                description='Status=Pendiente',
                status='warning'
            )
        
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
        cat_idx = CATEGORIAS.index(defaults.get("CATEGORIA")) if defaults.get("CATEGORIA") in CATEGORIAS else 0
        dist_idx = DISTRITOS.index(defaults.get("DISTRITO")) if defaults.get("DISTRITO") in DISTRITOS else 0
        
        categoria = c3.selectbox("Categoría", CATEGORIAS, index=cat_idx, disabled=read_only)
        distrito = c4.selectbox("Distrito", DISTRITOS, index=dist_idx, disabled=read_only)
        
        curso_idx = CURSOS.index(defaults.get("CURSO_INSCRITO")) + 1 if defaults.get("CURSO_INSCRITO") in CURSOS else 0
        curso = st.selectbox("Curso a Inscribir", [""] + CURSOS, index=curso_idx, disabled=read_only)
        
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
                    banco = c5.text_input("Banco Emisor")
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
            
            if not nombres.strip(): errores.append("Nombres es obligatorio.")
            if not apellidos.strip(): errores.append("Apellidos es obligatorio.")
            if not categoria.strip(): errores.append("Categoría es obligatoria.")
            if not distrito.strip(): errores.append("Distrito es obligatorio.")
            if not curso.strip(): errores.append("Curso a Inscribir es obligatorio.")
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
            
            if monto_pago <= 0:
                errores.append("El Monto Pagado es obligatorio y debe ser mayor a 0.")
                
            if forma_pago in ["Pago Móvil", "Transferencia"]:
                if abs(monto_pago - monto_oficial) > 200:
                    errores.append(f"La diferencia entre el Monto Pagado ({monto_pago}) y a Pagar ({monto_oficial:.2f}) supera el margen permitido de 200.")
            
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
            elif user_source == "2026":
                st.warning("Usted ya está registrado en el Padrón 2026. Los cambios no se han sobrescrito en este entorno Demo.")
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
                        "REFERENCIA": referencia_pago, "ARCHIVO_PAGO": img_path, "CURSO_INSCRITO": curso,
                        "MONTO_A_PAGAR": monto_oficial,
                        "MODALIDAD": modalidad,
                        "BancoE": banco,
                        "ObservaciónPE": observacion_pago,
                        "pagoEn": moneda,
                        "FECHA_REGISTRO": datetime.now().isoformat(),
                        "Status": "Pendiente"
                    }
                    
                    try:
                        insert_registro(values_dict)
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
