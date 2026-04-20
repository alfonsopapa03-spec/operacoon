"""
CRM de Clientes - Transporte de Carga
Colombia - Conectado a Supabase (PostgreSQL)
Complemento del Sistema de Anticipos v1
Módulos:
  - Ficha completa de cada cliente
  - Historial de viajes por cliente (vinculado a anticipos_v1)
  - Tarifas pactadas por ruta
  - Contactos del cliente
  - Notas y seguimiento comercial
  - KPIs por cliente (viajes, facturación, rutas frecuentes)
  - Exportación a Excel
"""

import streamlit as st
import psycopg2
from psycopg2 import pool
import pandas as pd
from datetime import datetime, timedelta, date
from io import BytesIO
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ==================== CONFIGURACIÓN ====================
SUPABASE_DB_URL = "postgresql://postgres.ntnpckmbyfmjhfskfwyu:Conejito100#@aws-1-us-east-1.pooler.supabase.com:6543/postgres"

# ==================== FORMATO COLOMBIANO ====================
def fmt(valor):
    if valor is None:
        return "0"
    try:
        return f"{int(float(valor)):,}".replace(',', '.')
    except:
        return str(valor)

def limpiar(texto):
    if not texto:
        return 0.0
    try:
        return float(str(texto).replace('.', '').replace(',', '.'))
    except:
        return 0.0

def hora_colombia():
    return datetime.utcnow() - timedelta(hours=5)

def fmt_fecha(valor):
    if valor is None:
        return "—"
    try:
        if isinstance(valor, (date, datetime)):
            return valor.strftime('%d/%m/%Y')
        return pd.to_datetime(valor).strftime('%d/%m/%Y')
    except:
        return str(valor)[:10]

# ==================== CONNECTION POOL ====================
@st.cache_resource
def get_pool():
    return psycopg2.pool.ThreadedConnectionPool(
        minconn=1, maxconn=5,
        dsn=SUPABASE_DB_URL,
        connect_timeout=10,
        options="-c statement_timeout=15000"
    )

def get_conn():
    return get_pool().getconn()

def put_conn(conn):
    get_pool().putconn(conn)

# ==================== BASE DE DATOS ====================
class DB:
    def _exec(self, query, params=None, fetch=None):
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(query, params)
            if fetch == "all":
                return cur.fetchall(), [d[0] for d in cur.description]
            elif fetch == "one":
                return cur.fetchone()
            else:
                conn.commit()
                return True
        except Exception as e:
            conn.rollback()
            st.error(f"DB error: {e}")
            return None
        finally:
            put_conn(conn)

    def _query_df(self, query, params=None):
        conn = get_conn()
        try:
            df = pd.read_sql_query(query, conn, params=params)
            return df
        except Exception as e:
            st.error(f"DB query error: {e}")
            return pd.DataFrame()
        finally:
            put_conn(conn)

    def init_tablas(self):
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crm_clientes (
                    id SERIAL PRIMARY KEY,
                    nombre TEXT UNIQUE NOT NULL,
                    nit TEXT,
                    direccion TEXT,
                    ciudad TEXT,
                    telefono TEXT,
                    email TEXT,
                    contacto_principal TEXT,
                    condicion_pago TEXT DEFAULT '30 días',
                    estado TEXT DEFAULT 'activo',
                    categoria TEXT DEFAULT 'regular',
                    observaciones TEXT,
                    fecha_inicio_relacion DATE,
                    fecha_registro TIMESTAMP NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crm_contactos (
                    id SERIAL PRIMARY KEY,
                    cliente_id INTEGER NOT NULL REFERENCES crm_clientes(id) ON DELETE CASCADE,
                    nombre TEXT NOT NULL,
                    cargo TEXT,
                    telefono TEXT,
                    email TEXT,
                    es_principal BOOLEAN DEFAULT FALSE,
                    observaciones TEXT,
                    fecha_registro TIMESTAMP NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crm_tarifas (
                    id SERIAL PRIMARY KEY,
                    cliente_id INTEGER NOT NULL REFERENCES crm_clientes(id) ON DELETE CASCADE,
                    origen TEXT NOT NULL,
                    destino TEXT NOT NULL,
                    tarifa BIGINT NOT NULL,
                    tipo_vehiculo TEXT DEFAULT 'tractomula',
                    vigente BOOLEAN DEFAULT TRUE,
                    observaciones TEXT,
                    fecha_desde DATE,
                    fecha_hasta DATE,
                    registrado_por TEXT,
                    fecha_registro TIMESTAMP NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crm_notas (
                    id SERIAL PRIMARY KEY,
                    cliente_id INTEGER NOT NULL REFERENCES crm_clientes(id) ON DELETE CASCADE,
                    tipo TEXT DEFAULT 'nota',
                    titulo TEXT NOT NULL,
                    contenido TEXT NOT NULL,
                    autor TEXT,
                    fecha_seguimiento DATE,
                    completada BOOLEAN DEFAULT FALSE,
                    fecha_registro TIMESTAMP NOT NULL
                )
            """)
            conn.commit()
        except Exception as e:
            conn.rollback()
            st.error(f"Error inicializando tablas CRM: {e}")
        finally:
            put_conn(conn)

    # ---- Clientes ----
    def obtener_clientes(self, estado=None, categoria=None, buscar=None):
        q = "SELECT * FROM crm_clientes WHERE 1=1"
        params = []
        if estado and estado != "Todos":
            q += " AND estado = %s"; params.append(estado.lower())
        if categoria and categoria != "Todas":
            q += " AND categoria = %s"; params.append(categoria.lower())
        if buscar:
            q += " AND (nombre ILIKE %s OR nit ILIKE %s OR ciudad ILIKE %s)"
            params += [f"%{buscar}%", f"%{buscar}%", f"%{buscar}%"]
        q += " ORDER BY nombre"
        return self._query_df(q, params if params else None)

    def obtener_cliente(self, cliente_id):
        df = self._query_df("SELECT * FROM crm_clientes WHERE id = %s", (cliente_id,))
        return df.iloc[0] if not df.empty else None

    def obtener_cliente_por_nombre(self, nombre):
        df = self._query_df("SELECT * FROM crm_clientes WHERE nombre ILIKE %s", (nombre,))
        return df.iloc[0] if not df.empty else None

    def crear_cliente(self, data):
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO crm_clientes
                    (nombre, nit, direccion, ciudad, telefono, email,
                     contacto_principal, condicion_pago, estado, categoria,
                     observaciones, fecha_inicio_relacion, fecha_registro)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (
                data['nombre'].strip().upper(),
                data.get('nit','').strip(),
                data.get('direccion','').strip(),
                data.get('ciudad','').strip().upper(),
                data.get('telefono','').strip(),
                data.get('email','').strip().lower(),
                data.get('contacto_principal','').strip(),
                data.get('condicion_pago','30 días'),
                data.get('estado','activo'),
                data.get('categoria','regular'),
                data.get('observaciones','').strip(),
                data.get('fecha_inicio_relacion'),
                hora_colombia()
            ))
            nuevo_id = cur.fetchone()[0]
            conn.commit()
            return nuevo_id
        except Exception as e:
            conn.rollback()
            st.error(f"Error creando cliente: {e}")
            return None
        finally:
            put_conn(conn)

    def actualizar_cliente(self, cliente_id, data):
        return bool(self._exec("""
            UPDATE crm_clientes SET
                nombre=%s, nit=%s, direccion=%s, ciudad=%s, telefono=%s,
                email=%s, contacto_principal=%s, condicion_pago=%s,
                estado=%s, categoria=%s, observaciones=%s, fecha_inicio_relacion=%s
            WHERE id=%s
        """, (
            data['nombre'].strip().upper(),
            data.get('nit','').strip(),
            data.get('direccion','').strip(),
            data.get('ciudad','').strip().upper(),
            data.get('telefono','').strip(),
            data.get('email','').strip().lower(),
            data.get('contacto_principal','').strip(),
            data.get('condicion_pago','30 días'),
            data.get('estado','activo'),
            data.get('categoria','regular'),
            data.get('observaciones','').strip(),
            data.get('fecha_inicio_relacion'),
            cliente_id
        )))

    def eliminar_cliente(self, cliente_id):
        self._exec("DELETE FROM crm_clientes WHERE id = %s", (cliente_id,))

    # ---- Contactos ----
    def obtener_contactos(self, cliente_id):
        return self._query_df(
            "SELECT * FROM crm_contactos WHERE cliente_id = %s ORDER BY es_principal DESC, nombre",
            (cliente_id,)
        )

    def crear_contacto(self, data):
        conn = get_conn()
        try:
            cur = conn.cursor()
            if data.get('es_principal'):
                cur.execute(
                    "UPDATE crm_contactos SET es_principal=FALSE WHERE cliente_id=%s",
                    (data['cliente_id'],)
                )
            cur.execute("""
                INSERT INTO crm_contactos
                    (cliente_id, nombre, cargo, telefono, email, es_principal, observaciones, fecha_registro)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (
                data['cliente_id'],
                data['nombre'].strip(),
                data.get('cargo','').strip(),
                data.get('telefono','').strip(),
                data.get('email','').strip().lower(),
                bool(data.get('es_principal', False)),
                data.get('observaciones','').strip(),
                hora_colombia()
            ))
            nuevo_id = cur.fetchone()[0]
            conn.commit()
            return nuevo_id
        except Exception as e:
            conn.rollback()
            st.error(f"Error creando contacto: {e}")
            return None
        finally:
            put_conn(conn)

    def eliminar_contacto(self, contacto_id):
        self._exec("DELETE FROM crm_contactos WHERE id = %s", (contacto_id,))

    # ---- Tarifas ----
    def obtener_tarifas(self, cliente_id, solo_vigentes=False):
        q = "SELECT * FROM crm_tarifas WHERE cliente_id = %s"
        params = [cliente_id]
        if solo_vigentes:
            q += " AND vigente = TRUE"
        q += " ORDER BY origen, destino"
        return self._query_df(q, params)

    def crear_tarifa(self, data):
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO crm_tarifas
                    (cliente_id, origen, destino, tarifa, tipo_vehiculo,
                     vigente, observaciones, fecha_desde, fecha_hasta,
                     registrado_por, fecha_registro)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (
                data['cliente_id'],
                data['origen'].strip().upper(),
                data['destino'].strip().upper(),
                int(data['tarifa']),
                data.get('tipo_vehiculo','tractomula'),
                bool(data.get('vigente', True)),
                data.get('observaciones','').strip(),
                data.get('fecha_desde'),
                data.get('fecha_hasta'),
                data.get('registrado_por','').strip().upper(),
                hora_colombia()
            ))
            nuevo_id = cur.fetchone()[0]
            conn.commit()
            return nuevo_id
        except Exception as e:
            conn.rollback()
            st.error(f"Error creando tarifa: {e}")
            return None
        finally:
            put_conn(conn)

    def actualizar_tarifa(self, tarifa_id, data):
        return bool(self._exec("""
            UPDATE crm_tarifas SET
                origen=%s, destino=%s, tarifa=%s, tipo_vehiculo=%s,
                vigente=%s, observaciones=%s, fecha_desde=%s, fecha_hasta=%s
            WHERE id=%s
        """, (
            data['origen'].strip().upper(),
            data['destino'].strip().upper(),
            int(data['tarifa']),
            data.get('tipo_vehiculo','tractomula'),
            bool(data.get('vigente', True)),
            data.get('observaciones','').strip(),
            data.get('fecha_desde'),
            data.get('fecha_hasta'),
            tarifa_id
        )))

    def eliminar_tarifa(self, tarifa_id):
        self._exec("DELETE FROM crm_tarifas WHERE id = %s", (tarifa_id,))

    # ---- Notas / Seguimiento ----
    def obtener_notas(self, cliente_id=None, pendientes=False):
        q = "SELECT n.*, c.nombre as cliente_nombre FROM crm_notas n JOIN crm_clientes c ON c.id = n.cliente_id WHERE 1=1"
        params = []
        if cliente_id:
            q += " AND n.cliente_id = %s"; params.append(cliente_id)
        if pendientes:
            q += " AND n.completada = FALSE AND n.fecha_seguimiento IS NOT NULL"
        q += " ORDER BY n.fecha_registro DESC"
        return self._query_df(q, params if params else None)

    def crear_nota(self, data):
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO crm_notas
                    (cliente_id, tipo, titulo, contenido, autor,
                     fecha_seguimiento, completada, fecha_registro)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (
                data['cliente_id'],
                data.get('tipo','nota'),
                data['titulo'].strip(),
                data['contenido'].strip(),
                data.get('autor','').strip().upper(),
                data.get('fecha_seguimiento'),
                False,
                hora_colombia()
            ))
            nuevo_id = cur.fetchone()[0]
            conn.commit()
            return nuevo_id
        except Exception as e:
            conn.rollback()
            st.error(f"Error creando nota: {e}")
            return None
        finally:
            put_conn(conn)

    def marcar_nota_completada(self, nota_id, completada=True):
        self._exec("UPDATE crm_notas SET completada=%s WHERE id=%s", (completada, nota_id))

    def eliminar_nota(self, nota_id):
        self._exec("DELETE FROM crm_notas WHERE id = %s", (nota_id,))

    # ---- Historial de viajes desde anticipos_v1 ----
    def obtener_viajes_cliente(self, nombre_cliente, fecha_ini=None, fecha_fin=None):
        q = "SELECT * FROM anticipos_v1 WHERE cliente ILIKE %s"
        params = [f"%{nombre_cliente}%"]
        if fecha_ini:
            q += " AND fecha_viaje >= %s"; params.append(fecha_ini)
        if fecha_fin:
            q += " AND fecha_viaje <= %s"; params.append(fecha_fin)
        q += " ORDER BY fecha_viaje DESC"
        return self._query_df(q, params)

    def obtener_kpis_cliente(self, nombre_cliente):
        df = self._query_df(
            "SELECT * FROM anticipos_v1 WHERE cliente ILIKE %s",
            (f"%{nombre_cliente}%",)
        )
        if df.empty:
            return {
                'total_viajes': 0, 'viajes_legalizados': 0, 'viajes_pendientes': 0,
                'total_anticipos': 0, 'promedio_anticipo': 0,
                'primera_operacion': None, 'ultima_operacion': None,
                'rutas': pd.DataFrame(), 'conductores': pd.DataFrame(),
                'por_mes': pd.DataFrame()
            }
        total_anticipos = int(df['valor_anticipo'].sum())
        promedio = int(df['valor_anticipo'].mean()) if len(df) > 0 else 0

        rutas = df.groupby(['origen','destino']).agg(
            viajes=('id','count'),
            anticipo_total=('valor_anticipo','sum')
        ).reset_index().sort_values('viajes', ascending=False)

        conductores = df.groupby('conductor').agg(
            viajes=('id','count')
        ).reset_index().sort_values('viajes', ascending=False)

        df['mes'] = pd.to_datetime(df['fecha_viaje']).dt.to_period('M').astype(str)
        por_mes = df.groupby('mes').agg(
            viajes=('id','count'),
            anticipos=('valor_anticipo','sum')
        ).reset_index().sort_values('mes')

        return {
            'total_viajes': len(df),
            'viajes_legalizados': int(df['legalizado'].sum()),
            'viajes_pendientes': len(df) - int(df['legalizado'].sum()),
            'total_anticipos': total_anticipos,
            'promedio_anticipo': promedio,
            'primera_operacion': df['fecha_viaje'].min(),
            'ultima_operacion': df['fecha_viaje'].max(),
            'rutas': rutas,
            'conductores': conductores,
            'por_mes': por_mes
        }

    def obtener_todos_clientes_con_kpis(self):
        df_clientes = self.obtener_clientes()
        df_viajes = self._query_df("SELECT cliente, valor_anticipo, legalizado, fecha_viaje FROM anticipos_v1")
        if df_clientes.empty:
            return pd.DataFrame()
        resultados = []
        for _, c in df_clientes.iterrows():
            viajes_c = df_viajes[df_viajes['cliente'].str.upper() == c['nombre'].upper()] \
                if not df_viajes.empty else pd.DataFrame()
            resultados.append({
                'id': c['id'],
                'nombre': c['nombre'],
                'ciudad': c.get('ciudad',''),
                'categoria': c.get('categoria',''),
                'estado': c.get('estado',''),
                'condicion_pago': c.get('condicion_pago',''),
                'contacto_principal': c.get('contacto_principal',''),
                'telefono': c.get('telefono',''),
                'total_viajes': len(viajes_c),
                'total_anticipos': int(viajes_c['valor_anticipo'].sum()) if not viajes_c.empty else 0,
                'pendientes': int((~viajes_c['legalizado'].astype(bool)).sum()) if not viajes_c.empty else 0,
                'ultima_operacion': viajes_c['fecha_viaje'].max() if not viajes_c.empty else None,
            })
        return pd.DataFrame(resultados)


# ==================== EXCEL EXPORT ====================
def generar_excel_crm(db) -> BytesIO:
    wb = Workbook()
    color_h = "1F4E79"
    fh = Font(name="Arial", bold=True, color="FFFFFF", size=10)
    fn = Font(name="Arial", size=9)
    ft = Font(name="Arial", bold=True, size=13, color="1F4E79")
    thin = Side(style="thin", color="BDBDBD")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    center = Alignment(horizontal="center", vertical="center")
    left_a = Alignment(horizontal="left", vertical="center")

    ws1 = wb.active
    ws1.title = "Clientes"
    ws1.merge_cells("A1:N1")
    ws1["A1"] = f"CRM Clientes — {hora_colombia().strftime('%d/%m/%Y %H:%M')}"
    ws1["A1"].font = ft
    ws1["A1"].alignment = center

    cols1 = ["ID","Nombre","NIT","Ciudad","Teléfono","Email","Contacto principal",
             "Condición pago","Categoría","Estado","Observaciones",
             "Fecha inicio","Total viajes","Total anticipos (COP)"]
    rh = 3
    for ci, cn in enumerate(cols1, 1):
        cell = ws1.cell(row=rh, column=ci, value=cn)
        cell.font = fh
        cell.fill = PatternFill("solid", fgColor=color_h)
        cell.alignment = center
        cell.border = border

    df_resumen = db.obtener_todos_clientes_con_kpis()
    df_clientes_full = db.obtener_clientes()
    for ri, (_, row) in enumerate(df_clientes_full.iterrows(), start=rh+1):
        res = df_resumen[df_resumen['id'] == row['id']].iloc[0] \
            if not df_resumen.empty and (df_resumen['id'] == row['id']).any() else None
        color_fila = "E8F5E9" if row.get('estado') == 'activo' else "F3F3F3"
        fill = PatternFill("solid", fgColor=color_fila)
        valores = [
            row['id'], row['nombre'], row.get('nit',''),
            row.get('ciudad',''), row.get('telefono',''), row.get('email',''),
            row.get('contacto_principal',''), row.get('condicion_pago',''),
            row.get('categoria',''), row.get('estado',''),
            row.get('observaciones',''),
            str(row.get('fecha_inicio_relacion',''))[:10],
            res['total_viajes'] if res is not None else 0,
            res['total_anticipos'] if res is not None else 0,
        ]
        for ci, val in enumerate(valores, 1):
            cell = ws1.cell(row=ri, column=ci, value=val)
            cell.fill = fill; cell.border = border; cell.font = fn
            cell.alignment = center if ci in [1,8,9,10,13,14] else left_a
            if ci == 14:
                cell.number_format = '#,##0'

    anchos1 = [6,28,14,16,14,24,22,14,12,10,28,14,12,20]
    for ci, aw in enumerate(anchos1, 1):
        ws1.column_dimensions[get_column_letter(ci)].width = aw
    ws1.freeze_panes = f"A{rh+1}"

    ws2 = wb.create_sheet("Tarifas")
    ws2.merge_cells("A1:I1")
    ws2["A1"] = "Tarifas pactadas por cliente y ruta"
    ws2["A1"].font = ft; ws2["A1"].alignment = center
    cols2 = ["Cliente","Origen","Destino","Tarifa (COP)","Tipo vehículo","Vigente","Desde","Hasta","Registrado por"]
    for ci, cn in enumerate(cols2, 1):
        cell = ws2.cell(row=3, column=ci, value=cn)
        cell.font = fh; cell.fill = PatternFill("solid", fgColor=color_h)
        cell.alignment = center; cell.border = border

    ri2 = 4
    for _, c in df_clientes_full.iterrows():
        df_tar = db.obtener_tarifas(c['id'])
        for _, t in df_tar.iterrows():
            color_t = "E3F2FD" if t.get('vigente') else "F3F3F3"
            fill2 = PatternFill("solid", fgColor=color_t)
            vals2 = [
                c['nombre'], t['origen'], t['destino'],
                int(t['tarifa']), t.get('tipo_vehiculo',''),
                "Sí" if t.get('vigente') else "No",
                str(t.get('fecha_desde',''))[:10],
                str(t.get('fecha_hasta',''))[:10],
                t.get('registrado_por',''),
            ]
            for ci, val in enumerate(vals2, 1):
                cell = ws2.cell(row=ri2, column=ci, value=val)
                cell.fill = fill2; cell.border = border; cell.font = fn
                cell.alignment = center if ci in [4,6] else left_a
                if ci == 4: cell.number_format = '#,##0'
            ri2 += 1

    anchos2 = [28,18,18,18,14,8,12,12,20]
    for ci, aw in enumerate(anchos2, 1):
        ws2.column_dimensions[get_column_letter(ci)].width = aw
    ws2.freeze_panes = "A4"

    output = BytesIO(); wb.save(output); output.seek(0)
    return output


# ==================== HELPERS ====================
CATEGORIAS = ["regular", "preferencial", "potencial", "inactivo"]
CONDICIONES_PAGO = ["contado", "8 días", "15 días", "30 días", "45 días", "60 días"]
TIPOS_NOTA = ["nota", "llamada", "reunión", "correo", "seguimiento", "queja", "oportunidad"]
TIPOS_VEHICULO = ["tractomula", "camión", "furgón", "minimula"]


def badge_categoria(cat):
    colores = {
        "preferencial": "🔵",
        "regular":      "🟢",
        "potencial":    "🟡",
        "inactivo":     "⚫",
    }
    return colores.get(cat, "⚪") + " " + cat.capitalize()

def badge_tipo_nota(tipo):
    iconos = {
        "nota": "📝", "llamada": "📞", "reunión": "🤝",
        "correo": "📧", "seguimiento": "🔔",
        "queja": "⚠️", "oportunidad": "💡",
    }
    return iconos.get(tipo, "📌") + " " + tipo.capitalize()


# ==================== APP PRINCIPAL ====================
def main():
    st.set_page_config(
        page_title="CRM Clientes - Transporte de Carga",
        layout="wide",
        page_icon="🏢"
    )
    st.title("🏢 CRM de Clientes - Transporte de Carga")

    session_defaults = {
        'db': None,
        'cliente_seleccionado': None,
        'editando_cliente': False,
        'confirmar_eliminar_cliente': None,
        'confirmar_eliminar_contacto': None,
        'confirmar_eliminar_tarifa': None,
        'confirmar_eliminar_nota': None,
        'editando_tarifa_id': None,
        'tab_cliente_activo': 0,
    }
    for key, val in session_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

    if st.session_state.db is None:
        db = DB()
        db.init_tablas()
        st.session_state.db = db
    db = st.session_state.db

    tab_resumen, tab_clientes, tab_nuevo, tab_seguimiento, tab_tarifas_global = st.tabs([
        "📊 Resumen",
        "📋 Clientes",
        "➕ Nuevo cliente",
        "🔔 Seguimiento",
        "💲 Tarifas",
    ])

    # ==================== TAB 1: RESUMEN ====================
    with tab_resumen:
        st.header("Resumen de clientes")
        df_resumen = db.obtener_todos_clientes_con_kpis()

        if df_resumen.empty:
            st.info("Aún no hay clientes registrados. Ve a **➕ Nuevo cliente** para comenzar.")
        else:
            total_clientes  = len(df_resumen)
            activos         = len(df_resumen[df_resumen['estado'] == 'activo'])
            total_viajes    = int(df_resumen['total_viajes'].sum())
            total_anticipos = int(df_resumen['total_anticipos'].sum())
            con_pendientes  = int((df_resumen['pendientes'] > 0).sum())

            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Total clientes",       total_clientes)
            col2.metric("Clientes activos",     activos)
            col3.metric("Total viajes",         total_viajes)
            col4.metric("Total anticipos",      f"${fmt(total_anticipos)}")
            col5.metric("Con anticipos pend.",  con_pendientes)

            st.divider()

            col_f1, col_f2, col_f3 = st.columns(3)
            with col_f1:
                buscar_r = st.text_input("Buscar cliente", placeholder="Nombre o ciudad...", key="r_buscar")
            with col_f2:
                filtro_cat_r = st.selectbox("Categoría", ["Todas"] + [c.capitalize() for c in CATEGORIAS], key="r_cat")
            with col_f3:
                filtro_estado_r = st.selectbox("Estado", ["Todos", "Activo", "Inactivo"], key="r_estado")

            df_show = df_resumen.copy()
            if buscar_r:
                df_show = df_show[
                    df_show['nombre'].str.contains(buscar_r, case=False, na=False) |
                    df_show['ciudad'].str.contains(buscar_r, case=False, na=False)
                ]
            if filtro_cat_r != "Todas":
                df_show = df_show[df_show['categoria'].str.lower() == filtro_cat_r.lower()]
            if filtro_estado_r != "Todos":
                df_show = df_show[df_show['estado'].str.lower() == filtro_estado_r.lower()]

            df_show = df_show.sort_values('total_viajes', ascending=False)

            for _, row in df_show.iterrows():
                icono = "🔵" if row['categoria'] == 'preferencial' else \
                        "🟢" if row['categoria'] == 'regular' else \
                        "🟡" if row['categoria'] == 'potencial' else "⚫"
                alerta_pend = f" | ⚠️ {row['pendientes']} pend." if row['pendientes'] > 0 else ""
                label_r = (
                    f"{icono} **{row['nombre']}** | {row['ciudad']} | "
                    f"{row['total_viajes']} viajes | ${fmt(row['total_anticipos'])} COP"
                    f"{alerta_pend}"
                )
                with st.expander(label_r):
                    col_ra, col_rb = st.columns([3, 2])
                    with col_ra:
                        st.write(f"📞 {row.get('telefono','—')} | 👤 {row.get('contacto_principal','—')}")
                        st.write(f"💳 Condición pago: **{row.get('condicion_pago','—')}** | Categoría: **{row.get('categoria','').capitalize()}**")
                        if row['ultima_operacion'] is not None:
                            st.write(f"📅 Última operación: **{fmt_fecha(row['ultima_operacion'])}**")
                        if row['pendientes'] > 0:
                            st.warning(f"⚠️ Tiene **{row['pendientes']}** anticipo(s) sin legalizar")
                    with col_rb:
                        if st.button("📂 Ver ficha completa", key=f"ver_{row['id']}", type="primary"):
                            st.session_state.cliente_seleccionado = row['id']
                            st.rerun()

            st.divider()
            excel_crm = generar_excel_crm(db)
            st.download_button(
                label="📥 Exportar CRM a Excel",
                data=excel_crm,
                file_name=f"crm_clientes_{hora_colombia().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary"
            )

    # ==================== TAB 2: FICHA DE CLIENTE ====================
    with tab_clientes:
        st.header("Ficha de cliente")

        df_clientes_lista = db.obtener_clientes()
        if df_clientes_lista.empty:
            st.info("No hay clientes registrados aún.")
        else:
            opciones_ids = df_clientes_lista['id'].tolist()
            opciones_nombres = df_clientes_lista['nombre'].tolist()

            idx_default = 0
            if st.session_state.cliente_seleccionado in opciones_ids:
                idx_default = opciones_ids.index(st.session_state.cliente_seleccionado)

            cliente_id_sel = st.selectbox(
                "Seleccionar cliente",
                opciones_ids,
                format_func=lambda x: df_clientes_lista[df_clientes_lista['id']==x]['nombre'].values[0],
                index=idx_default,
                key="sel_cliente_ficha"
            )
            st.session_state.cliente_seleccionado = cliente_id_sel

            cliente = db.obtener_cliente(cliente_id_sel)
            if cliente is None:
                st.error("Cliente no encontrado.")
            else:
                kpis = db.obtener_kpis_cliente(cliente['nombre'])

                col_h1, col_h2, col_h3 = st.columns([4, 1, 1])
                with col_h1:
                    cat_badge = badge_categoria(cliente.get('categoria','regular'))
                    estado_color = "🟢" if cliente.get('estado') == 'activo' else "⚫"
                    st.subheader(f"{estado_color} {cliente['nombre']}")
                    st.caption(f"{cat_badge} | {cliente.get('ciudad','—')} | NIT: {cliente.get('nit','—')}")
                with col_h2:
                    if st.button("✏️ Editar", key="btn_editar_cli", type="primary"):
                        st.session_state.editando_cliente = True
                        st.rerun()
                with col_h3:
                    if st.session_state.confirmar_eliminar_cliente == cliente_id_sel:
                        st.warning("¿Eliminar?")
                        c_si, c_no = st.columns(2)
                        with c_si:
                            if st.button("Sí", key="si_del_cli"):
                                db.eliminar_cliente(cliente_id_sel)
                                st.session_state.cliente_seleccionado = None
                                st.session_state.confirmar_eliminar_cliente = None
                                st.rerun()
                        with c_no:
                            if st.button("No", key="no_del_cli"):
                                st.session_state.confirmar_eliminar_cliente = None
                                st.rerun()
                    else:
                        if st.button("🗑️ Eliminar", key="btn_del_cli"):
                            st.session_state.confirmar_eliminar_cliente = cliente_id_sel
                            st.rerun()

                # KPIs rápidos
                k1, k2, k3, k4, k5 = st.columns(5)
                k1.metric("Total viajes",       kpis['total_viajes'])
                k2.metric("Legalizados",        kpis['viajes_legalizados'])
                k3.metric("Pendientes",         kpis['viajes_pendientes'])
                k4.metric("Total anticipos",    f"${fmt(kpis['total_anticipo'])}" if kpis['total_viajes'] > 0 else "$0")
                k5.metric("Promedio anticipo",  f"${fmt(kpis['promedio_anticipo'])}" if kpis['total_viajes'] > 0 else "$0")

                st.divider()

                # Pestañas de la ficha
                ft1, ft2, ft3, ft4, ft5 = st.tabs([
                    "📋 Datos generales",
                    "👥 Contactos",
                    "💲 Tarifas pactadas",
                    "📦 Historial de viajes",
                    "📝 Notas y seguimiento",
                ])

                # ---- Datos generales ----
                with ft1:
                    if st.session_state.editando_cliente:
                        st.subheader("✏️ Editar cliente")
                        with st.form("form_editar_cliente"):
                            col1e, col2e = st.columns(2)
                            with col1e:
                                nombre_e   = st.text_input("Nombre / Razón social", value=cliente['nombre'])
                                nit_e      = st.text_input("NIT", value=cliente.get('nit','') or '')
                                ciudad_e   = st.text_input("Ciudad", value=cliente.get('ciudad','') or '')
                                direccion_e = st.text_input("Dirección", value=cliente.get('direccion','') or '')
                                fi_rel_e   = st.date_input(
                                    "Inicio de la relación comercial",
                                    value=pd.to_datetime(cliente['fecha_inicio_relacion']).date()
                                    if cliente.get('fecha_inicio_relacion') else datetime.today()
                                )
                            with col2e:
                                telefono_e  = st.text_input("Teléfono", value=cliente.get('telefono','') or '')
                                email_e     = st.text_input("Email", value=cliente.get('email','') or '')
                                contacto_e  = st.text_input("Contacto principal", value=cliente.get('contacto_principal','') or '')
                                cond_e      = st.selectbox(
                                    "Condición de pago",
                                    CONDICIONES_PAGO,
                                    index=CONDICIONES_PAGO.index(cliente.get('condicion_pago','30 días'))
                                    if cliente.get('condicion_pago') in CONDICIONES_PAGO else 3
                                )
                                cat_e = st.selectbox(
                                    "Categoría",
                                    CATEGORIAS,
                                    index=CATEGORIAS.index(cliente.get('categoria','regular'))
                                    if cliente.get('categoria') in CATEGORIAS else 0
                                )
                                estado_e = st.selectbox(
                                    "Estado",
                                    ["activo","inactivo"],
                                    index=0 if cliente.get('estado','activo') == 'activo' else 1
                                )
                            obs_e = st.text_area("Observaciones", value=cliente.get('observaciones','') or '', height=80)
                            col_g, col_c = st.columns(2)
                            with col_g:
                                guardar_e = st.form_submit_button("💾 Guardar cambios", type="primary")
                            with col_c:
                                cancelar_e = st.form_submit_button("✖ Cancelar")

                            if guardar_e:
                                if not nombre_e.strip():
                                    st.error("⚠️ El nombre es obligatorio.")
                                else:
                                    ok = db.actualizar_cliente(cliente_id_sel, {
                                        'nombre': nombre_e, 'nit': nit_e,
                                        'direccion': direccion_e, 'ciudad': ciudad_e,
                                        'telefono': telefono_e, 'email': email_e,
                                        'contacto_principal': contacto_e,
                                        'condicion_pago': cond_e,
                                        'estado': estado_e, 'categoria': cat_e,
                                        'observaciones': obs_e,
                                        'fecha_inicio_relacion': fi_rel_e
                                    })
                                    if ok:
                                        st.success("✅ Cliente actualizado.")
                                        st.session_state.editando_cliente = False
                                        st.rerun()
                            if cancelar_e:
                                st.session_state.editando_cliente = False
                                st.rerun()
                    else:
                        col_d1, col_d2 = st.columns(2)
                        with col_d1:
                            st.markdown("**Información básica**")
                            st.write(f"🏢 Nombre: **{cliente['nombre']}**")
                            st.write(f"🪪 NIT: {cliente.get('nit','—')}")
                            st.write(f"📍 Ciudad: {cliente.get('ciudad','—')}")
                            st.write(f"🏠 Dirección: {cliente.get('direccion','—')}")
                            st.write(f"📞 Teléfono: {cliente.get('telefono','—')}")
                            st.write(f"📧 Email: {cliente.get('email','—')}")
                        with col_d2:
                            st.markdown("**Condiciones comerciales**")
                            st.write(f"👤 Contacto principal: {cliente.get('contacto_principal','—')}")
                            st.write(f"💳 Condición de pago: **{cliente.get('condicion_pago','—')}**")
                            st.write(f"🏷️ Categoría: {badge_categoria(cliente.get('categoria','regular'))}")
                            st.write(f"📅 Relación desde: {fmt_fecha(cliente.get('fecha_inicio_relacion'))}")
                            if cliente.get('observaciones'):
                                st.info(f"📝 {cliente['observaciones']}")

                        if not kpis['rutas'].empty:
                            st.divider()
                            st.markdown("**Rutas más frecuentes**")
                            df_rutas_show = kpis['rutas'].head(5).copy()
                            df_rutas_show['anticipo_total'] = df_rutas_show['anticipo_total'].apply(lambda x: f"${fmt(x)}")
                            df_rutas_show.columns = ['Origen','Destino','Viajes','Anticipo total']
                            st.dataframe(df_rutas_show, use_container_width=True, hide_index=True)

                        if not kpis['conductores'].empty:
                            st.markdown("**Conductores frecuentes**")
                            df_cond_show = kpis['conductores'].head(5).copy()
                            df_cond_show.columns = ['Conductor','Viajes']
                            st.dataframe(df_cond_show, use_container_width=True, hide_index=True)

                # ---- Contactos ----
                with ft2:
                    st.subheader("Contactos del cliente")
                    df_contactos = db.obtener_contactos(cliente_id_sel)

                    if df_contactos.empty:
                        st.info("No hay contactos registrados para este cliente.")
                    else:
                        for _, cont in df_contactos.iterrows():
                            principal_tag = " ⭐ Principal" if cont.get('es_principal') else ""
                            with st.expander(f"👤 {cont['nombre']} — {cont.get('cargo','—')}{principal_tag}"):
                                col_ca, col_cb = st.columns([3,1])
                                with col_ca:
                                    st.write(f"📞 {cont.get('telefono','—')} | 📧 {cont.get('email','—')}")
                                    if cont.get('observaciones'):
                                        st.caption(cont['observaciones'])
                                with col_cb:
                                    if st.session_state.confirmar_eliminar_contacto == cont['id']:
                                        c_si2, c_no2 = st.columns(2)
                                        with c_si2:
                                            if st.button("Sí", key=f"si_cont_{cont['id']}"):
                                                db.eliminar_contacto(cont['id'])
                                                st.session_state.confirmar_eliminar_contacto = None
                                                st.rerun()
                                        with c_no2:
                                            if st.button("No", key=f"no_cont_{cont['id']}"):
                                                st.session_state.confirmar_eliminar_contacto = None
                                                st.rerun()
                                    else:
                                        if st.button("🗑️", key=f"del_cont_{cont['id']}"):
                                            st.session_state.confirmar_eliminar_contacto = cont['id']
                                            st.rerun()

                    st.divider()
                    st.subheader("Agregar contacto")
                    with st.form(f"form_contacto_{cliente_id_sel}", clear_on_submit=True):
                        col_c1, col_c2 = st.columns(2)
                        with col_c1:
                            nombre_cont  = st.text_input("Nombre completo *")
                            cargo_cont   = st.text_input("Cargo")
                            principal_c  = st.checkbox("Es el contacto principal")
                        with col_c2:
                            tel_cont     = st.text_input("Teléfono")
                            email_cont   = st.text_input("Email")
                            obs_cont     = st.text_area("Observaciones", height=60)
                        if st.form_submit_button("💾 Agregar contacto", type="primary"):
                            if not nombre_cont.strip():
                                st.error("⚠️ El nombre es obligatorio.")
                            else:
                                nid_c = db.crear_contacto({
                                    'cliente_id': cliente_id_sel,
                                    'nombre': nombre_cont,
                                    'cargo': cargo_cont,
                                    'telefono': tel_cont,
                                    'email': email_cont,
                                    'es_principal': principal_c,
                                    'observaciones': obs_cont
                                })
                                if nid_c:
                                    st.success(f"✅ Contacto **{nombre_cont.strip()}** agregado.")
                                    st.rerun()

                # ---- Tarifas pactadas ----
                with ft3:
                    st.subheader("Tarifas pactadas con este cliente")
                    df_tarifas = db.obtener_tarifas(cliente_id_sel)

                    if df_tarifas.empty:
                        st.info("No hay tarifas registradas para este cliente.")
                    else:
                        for _, tar in df_tarifas.iterrows():
                            vigente_tag = "✅ Vigente" if tar.get('vigente') else "❌ No vigente"
                            label_tar = (
                                f"{vigente_tag} | {tar['origen']} → {tar['destino']} | "
                                f"${fmt(tar['tarifa'])} COP | {tar.get('tipo_vehiculo','')}"
                            )
                            with st.expander(label_tar):
                                if st.session_state.editando_tarifa_id == tar['id']:
                                    with st.form(f"form_edit_tar_{tar['id']}"):
                                        col_te1, col_te2 = st.columns(2)
                                        with col_te1:
                                            orig_e = st.text_input("Origen", value=tar['origen'])
                                            dest_e = st.text_input("Destino", value=tar['destino'])
                                            tar_e_txt = st.text_input("Tarifa (COP)", value=fmt(tar['tarifa']))
                                            tar_e = limpiar(tar_e_txt)
                                        with col_te2:
                                            tv_e = st.selectbox(
                                                "Tipo vehículo", TIPOS_VEHICULO,
                                                index=TIPOS_VEHICULO.index(tar.get('tipo_vehiculo','tractomula'))
                                                if tar.get('tipo_vehiculo') in TIPOS_VEHICULO else 0
                                            )
                                            vig_e = st.checkbox("Vigente", value=bool(tar.get('vigente', True)))
                                            fd_e = st.date_input(
                                                "Fecha desde",
                                                value=pd.to_datetime(tar['fecha_desde']).date()
                                                if tar.get('fecha_desde') else datetime.today()
                                            )
                                            fh_e = st.date_input(
                                                "Fecha hasta",
                                                value=pd.to_datetime(tar['fecha_hasta']).date()
                                                if tar.get('fecha_hasta') else datetime.today()
                                            )
                                        obs_te = st.text_input("Observaciones", value=tar.get('observaciones','') or '')
                                        col_gtar, col_ctar = st.columns(2)
                                        with col_gtar:
                                            g_tar = st.form_submit_button("💾 Guardar", type="primary")
                                        with col_ctar:
                                            c_tar = st.form_submit_button("✖ Cancelar")
                                        if g_tar:
                                            ok_tar = db.actualizar_tarifa(tar['id'], {
                                                'origen': orig_e, 'destino': dest_e,
                                                'tarifa': tar_e, 'tipo_vehiculo': tv_e,
                                                'vigente': vig_e, 'observaciones': obs_te,
                                                'fecha_desde': fd_e, 'fecha_hasta': fh_e
                                            })
                                            if ok_tar:
                                                st.success("✅ Tarifa actualizada.")
                                                st.session_state.editando_tarifa_id = None
                                                st.rerun()
                                        if c_tar:
                                            st.session_state.editando_tarifa_id = None
                                            st.rerun()
                                else:
                                    col_ti, col_tb = st.columns([4,1])
                                    with col_ti:
                                        st.write(f"📅 Vigente desde: {fmt_fecha(tar.get('fecha_desde'))} | hasta: {fmt_fecha(tar.get('fecha_hasta'))}")
                                        if tar.get('observaciones'):
                                            st.caption(tar['observaciones'])
                                    with col_tb:
                                        if st.button("✏️ Editar", key=f"edit_tar_{tar['id']}"):
                                            st.session_state.editando_tarifa_id = tar['id']
                                            st.rerun()
                                        if st.session_state.confirmar_eliminar_tarifa == tar['id']:
                                            c_si3, c_no3 = st.columns(2)
                                            with c_si3:
                                                if st.button("Sí", key=f"si_tar_{tar['id']}"):
                                                    db.eliminar_tarifa(tar['id'])
                                                    st.session_state.confirmar_eliminar_tarifa = None
                                                    st.rerun()
                                            with c_no3:
                                                if st.button("No", key=f"no_tar_{tar['id']}"):
                                                    st.session_state.confirmar_eliminar_tarifa = None
                                                    st.rerun()
                                        else:
                                            if st.button("🗑️", key=f"del_tar_{tar['id']}"):
                                                st.session_state.confirmar_eliminar_tarifa = tar['id']
                                                st.rerun()

                    st.divider()
                    st.subheader("Registrar nueva tarifa")
                    with st.form(f"form_tarifa_{cliente_id_sel}", clear_on_submit=True):
                        col_t1, col_t2 = st.columns(2)
                        with col_t1:
                            orig_t   = st.text_input("Origen *", placeholder="Ciudad de origen")
                            dest_t   = st.text_input("Destino *", placeholder="Ciudad de destino")
                            tar_txt  = st.text_input("Tarifa (COP) *", placeholder="Ej: 1.500.000")
                            tar_val  = limpiar(tar_txt)
                            if tar_val > 0:
                                st.caption(f"💵 {fmt(tar_val)} COP")
                        with col_t2:
                            tipo_v   = st.selectbox("Tipo de vehículo", TIPOS_VEHICULO)
                            vigente_t = st.checkbox("Vigente", value=True)
                            fd_t     = st.date_input("Fecha desde", value=datetime.today())
                            fh_t     = st.date_input("Fecha hasta (opcional)", value=None)
                            reg_por_t = st.text_input("Registrado por", placeholder="Tu nombre")
                        obs_t = st.text_input("Observaciones")
                        if st.form_submit_button("💾 Registrar tarifa", type="primary"):
                            errores_t = []
                            if not orig_t.strip():  errores_t.append("Origen obligatorio")
                            if not dest_t.strip():  errores_t.append("Destino obligatorio")
                            if tar_val <= 0:        errores_t.append("Tarifa debe ser mayor a 0")
                            if errores_t:
                                for et in errores_t: st.error(f"⚠️ {et}")
                            else:
                                nid_t = db.crear_tarifa({
                                    'cliente_id': cliente_id_sel,
                                    'origen': orig_t, 'destino': dest_t,
                                    'tarifa': tar_val, 'tipo_vehiculo': tipo_v,
                                    'vigente': vigente_t, 'observaciones': obs_t,
                                    'fecha_desde': fd_t, 'fecha_hasta': fh_t,
                                    'registrado_por': reg_por_t
                                })
                                if nid_t:
                                    st.success(f"✅ Tarifa registrada: {orig_t.upper()} → {dest_t.upper()} | ${fmt(tar_val)} COP")
                                    st.rerun()

                # ---- Historial de viajes ----
                with ft4:
                    st.subheader(f"Historial de viajes — {cliente['nombre']}")
                    col_hv1, col_hv2 = st.columns(2)
                    with col_hv1:
                        fi_hv = st.date_input("Desde", value=None, key="hv_fi")
                    with col_hv2:
                        ff_hv = st.date_input("Hasta", value=None, key="hv_ff")

                    df_viajes_c = db.obtener_viajes_cliente(
                        cliente['nombre'],
                        fi_hv.strftime('%Y-%m-%d') if fi_hv else None,
                        ff_hv.strftime('%Y-%m-%d') if ff_hv else None
                    )

                    if df_viajes_c.empty:
                        st.info("No se encontraron viajes para este cliente en el período seleccionado.")
                    else:
                        cols_v = ['id','manifiesto','fecha_viaje','placa','conductor',
                                  'origen','destino','valor_anticipo','legalizado']
                        df_v_show = df_viajes_c[[c for c in cols_v if c in df_viajes_c.columns]].copy()
                        df_v_show['fecha_viaje'] = df_v_show['fecha_viaje'].apply(fmt_fecha)
                        df_v_show['valor_anticipo'] = df_v_show['valor_anticipo'].apply(lambda x: f"${fmt(x)}")
                        df_v_show['legalizado'] = df_v_show['legalizado'].apply(
                            lambda x: "✅" if x else "🔴"
                        )
                        df_v_show.rename(columns={
                            'id':'ID','manifiesto':'Manif.','fecha_viaje':'Fecha',
                            'placa':'Placa','conductor':'Conductor',
                            'origen':'Origen','destino':'Destino',
                            'valor_anticipo':'Anticipo','legalizado':'Estado'
                        }, inplace=True)
                        st.dataframe(df_v_show, use_container_width=True, hide_index=True, height=300)
                        st.caption(f"Total: **{len(df_viajes_c)}** viajes | Anticipos: **${fmt(df_viajes_c['valor_anticipo'].sum())} COP**")

                # ---- Notas y seguimiento ----
                with ft5:
                    st.subheader("Notas y seguimiento")
                    df_notas_c = db.obtener_notas(cliente_id_sel)

                    if df_notas_c.empty:
                        st.info("No hay notas para este cliente.")
                    else:
                        for _, nota in df_notas_c.iterrows():
                            completada = bool(nota.get('completada', False))
                            tipo_label = badge_tipo_nota(nota.get('tipo','nota'))
                            fecha_seg = nota.get('fecha_seguimiento')
                            seg_tag = f" | 📅 Seguimiento: {fmt_fecha(fecha_seg)}" if fecha_seg else ""
                            check_tag = " ✅" if completada else ""
                            with st.expander(f"{tipo_label} | {nota['titulo']}{seg_tag}{check_tag}"):
                                col_na, col_nb = st.columns([4,1])
                                with col_na:
                                    st.write(nota['contenido'])
                                    if nota.get('autor'):
                                        st.caption(f"Por: {nota['autor']} | {fmt_fecha(nota.get('fecha_registro'))}")
                                with col_nb:
                                    if not completada:
                                        if st.button("✅ Completar", key=f"comp_nota_{nota['id']}"):
                                            db.marcar_nota_completada(nota['id'], True)
                                            st.rerun()
                                    else:
                                        if st.button("↩️ Reabrir", key=f"reab_nota_{nota['id']}"):
                                            db.marcar_nota_completada(nota['id'], False)
                                            st.rerun()
                                    if st.session_state.confirmar_eliminar_nota == nota['id']:
                                        c_si4, c_no4 = st.columns(2)
                                        with c_si4:
                                            if st.button("Sí", key=f"si_nota_{nota['id']}"):
                                                db.eliminar_nota(nota['id'])
                                                st.session_state.confirmar_eliminar_nota = None
                                                st.rerun()
                                        with c_no4:
                                            if st.button("No", key=f"no_nota_{nota['id']}"):
                                                st.session_state.confirmar_eliminar_nota = None
                                                st.rerun()
                                    else:
                                        if st.button("🗑️", key=f"del_nota_{nota['id']}"):
                                            st.session_state.confirmar_eliminar_nota = nota['id']
                                            st.rerun()

                    st.divider()
                    st.subheader("Nueva nota / actividad")
                    with st.form(f"form_nota_{cliente_id_sel}", clear_on_submit=True):
                        col_n1, col_n2 = st.columns(2)
                        with col_n1:
                            tipo_nota    = st.selectbox("Tipo", TIPOS_NOTA,
                                format_func=lambda x: badge_tipo_nota(x))
                            titulo_nota  = st.text_input("Título *", placeholder="Resumen breve")
                            autor_nota   = st.text_input("Autor", placeholder="Tu nombre")
                        with col_n2:
                            fecha_seg_n  = st.date_input("Fecha de seguimiento (opcional)", value=None)
                        contenido_nota = st.text_area("Contenido / Detalle *", height=100)
                        if st.form_submit_button("💾 Guardar nota", type="primary"):
                            errores_n = []
                            if not titulo_nota.strip():   errores_n.append("El título es obligatorio.")
                            if not contenido_nota.strip(): errores_n.append("El contenido es obligatorio.")
                            if errores_n:
                                for en in errores_n: st.error(f"⚠️ {en}")
                            else:
                                nid_n = db.crear_nota({
                                    'cliente_id': cliente_id_sel,
                                    'tipo': tipo_nota,
                                    'titulo': titulo_nota,
                                    'contenido': contenido_nota,
                                    'autor': autor_nota,
                                    'fecha_seguimiento': fecha_seg_n,
                                })
                                if nid_n:
                                    st.success(f"✅ Nota guardada.")
                                    st.rerun()

    # ==================== TAB 3: NUEVO CLIENTE ====================
    with tab_nuevo:
        st.header("Registrar nuevo cliente")
        with st.form("form_nuevo_cliente_crm", clear_on_submit=True):
            col1n, col2n = st.columns(2)
            with col1n:
                nombre_n    = st.text_input("Nombre / Razón social *", placeholder="Ej: GLOBO EXPRESS S.A.S")
                nit_n       = st.text_input("NIT", placeholder="Ej: 900.123.456-7")
                ciudad_n    = st.text_input("Ciudad *", placeholder="Ej: BOGOTÁ")
                direccion_n = st.text_input("Dirección", placeholder="Ej: Cra 15 # 93-47")
                fi_rel_n    = st.date_input("Fecha inicio relación comercial", value=datetime.today())
            with col2n:
                telefono_n  = st.text_input("Teléfono", placeholder="Ej: 3001234567")
                email_n     = st.text_input("Email", placeholder="contacto@empresa.com")
                contacto_n  = st.text_input("Contacto principal", placeholder="Nombre del contacto")
                cond_n      = st.selectbox("Condición de pago", CONDICIONES_PAGO, index=3)
                cat_n       = st.selectbox("Categoría", CATEGORIAS,
                    format_func=lambda x: badge_categoria(x))
            obs_n = st.text_area("Observaciones", height=80)

            if st.form_submit_button("💾 Registrar cliente", type="primary"):
                errores_nuevo = []
                if not nombre_n.strip(): errores_nuevo.append("El nombre es obligatorio.")
                if not ciudad_n.strip(): errores_nuevo.append("La ciudad es obligatoria.")
                if errores_nuevo:
                    for en in errores_nuevo: st.error(f"⚠️ {en}")
                else:
                    nid_nuevo = db.crear_cliente({
                        'nombre': nombre_n, 'nit': nit_n,
                        'direccion': direccion_n, 'ciudad': ciudad_n,
                        'telefono': telefono_n, 'email': email_n,
                        'contacto_principal': contacto_n,
                        'condicion_pago': cond_n,
                        'estado': 'activo', 'categoria': cat_n,
                        'observaciones': obs_n,
                        'fecha_inicio_relacion': fi_rel_n
                    })
                    if nid_nuevo:
                        st.success(f"✅ Cliente **{nombre_n.strip().upper()}** registrado (ID: {nid_nuevo})")
                        st.session_state.cliente_seleccionado = nid_nuevo
                        st.rerun()

    # ==================== TAB 4: SEGUIMIENTO ====================
    with tab_seguimiento:
        st.header("🔔 Seguimientos pendientes")
        st.caption("Tareas, llamadas o reuniones con fecha de seguimiento asignada y aún no completadas.")

        df_pendientes_seg = db.obtener_notas(pendientes=True)

        if df_pendientes_seg.empty:
            st.success("✅ No hay seguimientos pendientes.")
        else:
            hoy_seg = hora_colombia().date()
            vencidos, hoy_items, proximos = [], [], []
            for _, n in df_pendientes_seg.iterrows():
                try:
                    fs = pd.to_datetime(n['fecha_seguimiento']).date()
                except:
                    fs = None
                if fs is None:
                    continue
                if fs < hoy_seg:
                    vencidos.append((n, fs))
                elif fs == hoy_seg:
                    hoy_items.append((n, fs))
                else:
                    proximos.append((n, fs))

            col_sv1, col_sv2, col_sv3 = st.columns(3)
            col_sv1.metric("🔴 Vencidos", len(vencidos))
            col_sv2.metric("🟡 Hoy",      len(hoy_items))
            col_sv3.metric("🟢 Próximos", len(proximos))

            def render_seguimiento_items(items, label):
                if not items:
                    return
                st.subheader(label)
                for nota, fs in sorted(items, key=lambda x: x[1]):
                    dias_diff = (fs - hoy_seg).days
                    if dias_diff < 0:
                        dias_label = f"Venció hace {abs(dias_diff)} día(s)"
                    elif dias_diff == 0:
                        dias_label = "Hoy"
                    else:
                        dias_label = f"En {dias_diff} día(s)"

                    with st.expander(
                        f"{badge_tipo_nota(nota.get('tipo','nota'))} | "
                        f"**{nota.get('cliente_nombre','?')}** | {nota['titulo']} | {dias_label}"
                    ):
                        col_sia, col_sib = st.columns([4,1])
                        with col_sia:
                            st.write(nota['contenido'])
                            st.caption(f"📅 {fmt_fecha(fs)} | Autor: {nota.get('autor','—')}")
                        with col_sib:
                            if st.button("✅ Completar", key=f"seg_comp_{nota['id']}", type="primary"):
                                db.marcar_nota_completada(nota['id'], True)
                                st.rerun()
                            if st.button("📂 Ver cliente", key=f"seg_ver_{nota['id']}"):
                                st.session_state.cliente_seleccionado = int(nota['cliente_id'])
                                st.rerun()

            render_seguimiento_items(vencidos,  "🔴 Vencidos")
            render_seguimiento_items(hoy_items, "🟡 Para hoy")
            render_seguimiento_items(proximos,  "🟢 Próximos")

    # ==================== TAB 5: TARIFAS GLOBAL ====================
    with tab_tarifas_global:
        st.header("💲 Vista global de tarifas")
        st.caption("Consulta y compara tarifas de todos los clientes por ruta.")

        col_tg1, col_tg2, col_tg3 = st.columns(3)
        with col_tg1:
            buscar_orig = st.text_input("Filtrar origen", placeholder="Ciudad...")
        with col_tg2:
            buscar_dest = st.text_input("Filtrar destino", placeholder="Ciudad...")
        with col_tg3:
            solo_vig = st.checkbox("Solo vigentes", value=True)

        df_clientes_t = db.obtener_clientes()
        todas_tarifas = []
        for _, c in df_clientes_t.iterrows():
            df_t = db.obtener_tarifas(c['id'], solo_vigentes=solo_vig)
            for _, t in df_t.iterrows():
                todas_tarifas.append({
                    'Cliente': c['nombre'],
                    'Origen': t['origen'],
                    'Destino': t['destino'],
                    'Tarifa (COP)': int(t['tarifa']),
                    'Tipo vehículo': t.get('tipo_vehiculo',''),
                    'Vigente': "Sí" if t.get('vigente') else "No",
                    'Desde': fmt_fecha(t.get('fecha_desde')),
                    'Registrado por': t.get('registrado_por',''),
                })

        if not todas_tarifas:
            st.info("No hay tarifas registradas aún.")
        else:
            df_todas = pd.DataFrame(todas_tarifas)
            if buscar_orig:
                df_todas = df_todas[df_todas['Origen'].str.contains(buscar_orig, case=False, na=False)]
            if buscar_dest:
                df_todas = df_todas[df_todas['Destino'].str.contains(buscar_dest, case=False, na=False)]

            df_todas = df_todas.sort_values(['Origen','Destino','Tarifa (COP)'])
            df_todas_show = df_todas.copy()
            df_todas_show['Tarifa (COP)'] = df_todas_show['Tarifa (COP)'].apply(lambda x: f"${fmt(x)}")
            st.dataframe(df_todas_show, use_container_width=True, hide_index=True, height=400)
            st.caption(f"Total tarifas mostradas: **{len(df_todas)}**")

            if len(df_todas) > 0:
                st.divider()
                st.subheader("Comparativo por ruta")
                rutas_unicas = df_todas[['Origen','Destino']].drop_duplicates()
                rutas_labels = [f"{r['Origen']} → {r['Destino']}" for _, r in rutas_unicas.iterrows()]
                if rutas_labels:
                    ruta_sel = st.selectbox("Seleccionar ruta", rutas_labels, key="comp_ruta")
                    if ruta_sel:
                        orig_sel, dest_sel = ruta_sel.split(" → ")
                        df_comp = df_todas[
                            (df_todas['Origen'] == orig_sel) &
                            (df_todas['Destino'] == dest_sel)
                        ].copy()
                        if not df_comp.empty:
                            df_comp_show = df_comp[['Cliente','Tarifa (COP)','Tipo vehículo','Vigente']].copy()
                            df_comp_show['Tarifa (COP)'] = df_comp_show['Tarifa (COP)'].apply(
                                lambda x: f"${fmt(x)}" if isinstance(x, (int,float)) else x
                            )
                            st.dataframe(df_comp_show, use_container_width=True, hide_index=True)
                            tarifas_num = df_todas[
                                (df_todas['Origen'] == orig_sel) &
                                (df_todas['Destino'] == dest_sel)
                            ]['Tarifa (COP)']
                            col_comp1, col_comp2, col_comp3 = st.columns(3)
                            col_comp1.metric("Tarifa mínima", f"${fmt(tarifas_num.min())}")
                            col_comp2.metric("Tarifa máxima", f"${fmt(tarifas_num.max())}")
                            col_comp3.metric("Promedio",      f"${fmt(int(tarifas_num.mean()))}")


if __name__ == "__main__":
    main()
