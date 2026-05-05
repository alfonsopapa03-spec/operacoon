import streamlit as st
import psycopg2
from psycopg2 import pool as pg_pool
import pandas as pd
from datetime import datetime, timedelta, date
import io
import warnings
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import pytz

# ==================== CONFIGURACIÓN ====================
st.set_page_config(
    page_title="Inspecciones Preoperacionales Semanales",
    layout="wide",
    page_icon="🔧",
    initial_sidebar_state="collapsed"
)

# ==================== CREDENCIALES ====================
SUPABASE_DB_URL = "postgresql://postgres.ogfenizdijcboekqhuhd:Conejito200$@aws-1-us-west-2.pooler.supabase.com:6543/postgres"

# ==================== CATÁLOGO DE MÁQUINAS (ACTUALIZADO) ====================
MAQUINAS = [
    "Bloquera", "Caldera", "Dobladora", "Enderezadora", "Enmalladora",
    "Molino", "Montacarga", "Paneladora", "Pantógrafo", "Preexpansora", "Soldador manual"
]

DIAS_SEMANA = ["LUN", "MAR", "MIE", "JUE", "VIE", "SAB", "DOM"]

# ==================== ÍTEMS DE INSPECCIÓN (TUS ÍTEMS ORIGINALES) ====================
ITEMS_ANTES_USO = [
    "¿Tiene permiso el trabajador para utilizar la máquina?",
    "¿Ha sido capacitado el trabajador para utilizar la máquina?",
    "¿Se ha verificado que la presión del aire se encuentre en 125 PSI?",
    "¿Se ha inspeccionado que los desviadores contengan material adecuadamente?",
    "¿Se ha inspeccionado que los electro-válvulas funcionen adecuadamente?",
    "¿Se ha comprobado que los ganchos de ajuste funcionen correctamente?",
    "¿El 'carrusel' de fricción del material funciona satisfactoriamente?",
    "¿Se ha verificado que el tope se encuentre en óptimas condiciones?",
    "¿Se ha inspeccionado que los botones y controles funcionen oportunamente?",
    "¿Las paradas de emergencia (6 en total) funcionan correctamente?",
    "¿Se ha verificado el estado de los cabezales (inferior/superior)?",
    "¿El nivel de aceite de lubricación se encuentra en nivel adecuado?",
    "¿El manómetro de aceite de sobrecarga funciona correctamente?",
    "¿Se ha ajustado la altura del panel a la medida correspondiente?",
]

ITEMS_EPP = [
    "¿Se ha inspeccionado el lugar de trabajo? (material combustible, riesgo de incendios, etc.)",
    "¿La iluminación del área de trabajo es adecuada para operación de la máquina sin riesgos?",
    "¿Cuenta con los elementos de protección personal? (protector de ojos, oídos, guantes y cabezado)",
    "¿El trabajador está vestido apropiadamente? (Camisa manga larga, pantalón y calzado)",
    "¿Se evidencia el NO uso de joyas, relojes y ropa holgada?",
    "¿Se tiene el cabello recogido si lo tiene largo?",
]

ITEMS_ELECTRICA = [
    "¿Se ha verificado que el cable de alimentación está en buen estado?",
    "¿Se ha revisado que el enchufe se encuentre en buenas condiciones?",
    "¿El interruptor de encendido funciona correctamente?",
]

# OPCIONES DE RESPUESTA PARA LA TABLA
OPCIONES_INSPECCION = ["-", "C", "NC", "N/A"] # '-' significa día no trabajado/evaluado
ESTADOS_INSPECCION  = ["✅ Aprobada", "⚠️ Con Observaciones", "❌ Rechazada"]

# ==================== CSS ====================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700&family=Barlow:wght@300;400;500&display=swap');
    html, body, [class*="css"] { font-family: 'Barlow', sans-serif; }
    .main-header {
        background: linear-gradient(135deg, #0f2027, #203a43, #2c5364);
        padding: 1.5rem 2rem; border-radius: 12px; margin-bottom: 1.5rem;
    }
    .main-header h1 {
        font-family: 'Barlow Condensed', sans-serif;
        font-size: 2rem; font-weight: 700; color: white; margin: 0; letter-spacing: 1px;
    }
    .main-header p { color: #a0c4d8; margin: 0; font-size: 0.9rem; }
    .seccion-titulo {
        background: #203a43; color: white; padding: 0.4rem 1rem;
        border-radius: 6px; font-weight: 700; font-size: 0.95rem;
        margin: 1rem 0 0.5rem 0; letter-spacing: 0.5px;
    }
    .campo-obligatorio { color: #e74c3c; font-weight: bold; }
</style>
""", unsafe_allow_html=True)


# ==================== BASE DE DATOS ====================
@st.cache_resource
def get_pool():
    try:
        return pg_pool.SimpleConnectionPool(
            minconn=1, maxconn=5, dsn=SUPABASE_DB_URL,
            sslmode="require", connect_timeout=15, options="-c statement_timeout=30000"
        )
    except Exception as e:
        st.error(f"❌ No se pudo conectar a la base de datos: {e}")
        st.stop()


class DB:
    def __init__(self):
        self.pool = get_pool()
        self.init()

    def conn(self):
        try:
            c = self.pool.getconn()
            c.cursor().execute("SELECT 1")
            return c
        except Exception:
            return psycopg2.connect(dsn=SUPABASE_DB_URL, sslmode="require", connect_timeout=15)

    def release(self, c):
        try:
            if c and not c.closed: self.pool.putconn(c)
        except Exception: pass

    def init(self):
        c = None
        try:
            c = self.conn()
            cur = c.cursor()
            
            # Tabla principal (ahora con fecha_ini y fecha_fin)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS inspecciones_preop (
                    id SERIAL PRIMARY KEY,
                    fecha_registro TIMESTAMP DEFAULT (now() AT TIME ZONE 'America/Bogota'),
                    fecha_ini DATE NOT NULL,
                    fecha_fin DATE NOT NULL,
                    maquina TEXT NOT NULL,
                    modelo TEXT,
                    marca TEXT,
                    placa TEXT,
                    trabajador TEXT NOT NULL,
                    revisado_por TEXT NOT NULL,
                    cliente_proyecto TEXT NOT NULL,
                    responsable_mantenimiento TEXT NOT NULL,
                    estado TEXT DEFAULT 'Aprobada',
                    observaciones TEXT
                )
            """)
            
            # Tabla ítems (ahora con LUN, MAR, MIE, JUE, VIE, SAB, DOM)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS inspecciones_preop_items (
                    id SERIAL PRIMARY KEY,
                    inspeccion_id INTEGER REFERENCES inspecciones_preop(id) ON DELETE CASCADE,
                    seccion TEXT NOT NULL,
                    item_numero INTEGER NOT NULL,
                    descripcion TEXT NOT NULL,
                    lun TEXT DEFAULT '-', mar TEXT DEFAULT '-', mie TEXT DEFAULT '-',
                    jue TEXT DEFAULT '-', vie TEXT DEFAULT '-', sab TEXT DEFAULT '-',
                    dom TEXT DEFAULT '-'
                )
            """)
            
            # Migración automática si ya existían las tablas viejas
            cur.execute("""
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='inspecciones_preop' AND column_name='fecha') THEN
                        ALTER TABLE inspecciones_preop RENAME COLUMN fecha TO fecha_ini;
                        ALTER TABLE inspecciones_preop ADD COLUMN fecha_fin DATE DEFAULT CURRENT_DATE;
                    END IF;
                    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='inspecciones_preop_items' AND column_name='resultado') THEN
                        ALTER TABLE inspecciones_preop_items RENAME COLUMN resultado TO lun;
                        ALTER TABLE inspecciones_preop_items ADD COLUMN mar TEXT DEFAULT '-';
                        ALTER TABLE inspecciones_preop_items ADD COLUMN mie TEXT DEFAULT '-';
                        ALTER TABLE inspecciones_preop_items ADD COLUMN jue TEXT DEFAULT '-';
                        ALTER TABLE inspecciones_preop_items ADD COLUMN vie TEXT DEFAULT '-';
                        ALTER TABLE inspecciones_preop_items ADD COLUMN sab TEXT DEFAULT '-';
                        ALTER TABLE inspecciones_preop_items ADD COLUMN dom TEXT DEFAULT '-';
                    END IF;
                END $$;
            """)
            c.commit()
            cur.close()
        except Exception as e:
            st.error(f"Error inicializando DB: {e}")
        finally:
            self.release(c)

    def guardar_inspeccion(self, datos: dict, items: list) -> bool:
        c = None
        try:
            c = self.conn()
            cur = c.cursor()
            cur.execute("""
                INSERT INTO inspecciones_preop
                (fecha_ini, fecha_fin, maquina, modelo, marca, placa, trabajador, revisado_por,
                 cliente_proyecto, responsable_mantenimiento, estado, observaciones)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                datos["fecha_ini"], datos["fecha_fin"], datos["maquina"], datos["modelo"], 
                datos["marca"], datos["placa"], datos["trabajador"], datos["revisado_por"],
                datos["cliente_proyecto"], datos["responsable_mantenimiento"],
                datos["estado"], datos["observaciones"],
            ))
            inspeccion_id = cur.fetchone()[0]
            for it in items:
                cur.execute("""
                    INSERT INTO inspecciones_preop_items
                    (inspeccion_id, seccion, item_numero, descripcion, lun, mar, mie, jue, vie, sab, dom)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    inspeccion_id, it["seccion"], it["item_numero"], it["descripcion"],
                    it["LUN"], it["MAR"], it["MIE"], it["JUE"], it["VIE"], it["SAB"], it["DOM"]
                ))
            c.commit()
            cur.close()
            return True
        except Exception as e:
            st.error(f"Error guardando: {e}")
            return False
        finally:
            self.release(c)

    def actualizar_inspeccion(self, inspeccion_id: int, datos: dict, items: list) -> bool:
        c = None
        try:
            c = self.conn()
            cur = c.cursor()
            cur.execute("""
                UPDATE inspecciones_preop SET
                fecha_ini=%s, fecha_fin=%s, maquina=%s, modelo=%s, marca=%s, placa=%s,
                trabajador=%s, revisado_por=%s, cliente_proyecto=%s,
                responsable_mantenimiento=%s, estado=%s, observaciones=%s
                WHERE id=%s
            """, (
                datos["fecha_ini"], datos["fecha_fin"], datos["maquina"], datos["modelo"], 
                datos["marca"], datos["placa"], datos["trabajador"], datos["revisado_por"],
                datos["cliente_proyecto"], datos["responsable_mantenimiento"],
                datos["estado"], datos["observaciones"], inspeccion_id
            ))
            cur.execute("DELETE FROM inspecciones_preop_items WHERE inspeccion_id=%s", (inspeccion_id,))
            for it in items:
                cur.execute("""
                    INSERT INTO inspecciones_preop_items
                    (inspeccion_id, seccion, item_numero, descripcion, lun, mar, mie, jue, vie, sab, dom)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    inspeccion_id, it["seccion"], it["item_numero"], it["descripcion"],
                    it["LUN"], it["MAR"], it["MIE"], it["JUE"], it["VIE"], it["SAB"], it["DOM"]
                ))
            c.commit()
            cur.close()
            return True
        except Exception as e:
            st.error(f"Error actualizando: {e}")
            return False
        finally:
            self.release(c)

    def eliminar_inspeccion(self, inspeccion_id: int) -> bool:
        c = None
        try:
            c = self.conn()
            cur = c.cursor()
            cur.execute("DELETE FROM inspecciones_preop WHERE id=%s", (inspeccion_id,))
            c.commit()
            cur.close()
            return True
        except Exception as e:
            return False
        finally:
            self.release(c)

    def obtener_inspecciones(self, fecha_ini=None, fecha_fin=None, maquina=None, estado=None, trabajador=None) -> pd.DataFrame:
        c = None
        try:
            c = self.conn()
            q = """
                SELECT id, fecha_ini, fecha_fin, maquina, modelo, marca, placa,
                       trabajador, revisado_por, cliente_proyecto,
                       responsable_mantenimiento, estado, observaciones, fecha_registro
                FROM inspecciones_preop WHERE 1=1
            """
            params = []
            if fecha_ini: q += " AND fecha_ini >= %s"; params.append(fecha_ini)
            if fecha_fin: q += " AND fecha_ini <= %s"; params.append(fecha_fin)
            if maquina and maquina != "Todas": q += " AND maquina = %s"; params.append(maquina)
            if estado and estado != "Todos": q += " AND estado ILIKE %s"; params.append(f"%{estado}%")
            if trabajador: q += " AND trabajador ILIKE %s"; params.append(f"%{trabajador}%")
            q += " ORDER BY fecha_ini DESC, id DESC"
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return pd.read_sql(q, c, params=params)
        except Exception as e:
            return pd.DataFrame()
        finally:
            self.release(c)

    def obtener_items_inspeccion(self, inspeccion_id: int) -> pd.DataFrame:
        c = None
        try:
            c = self.conn()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return pd.read_sql("""
                    SELECT seccion, item_numero, descripcion, 
                           lun as "LUN", mar as "MAR", mie as "MIE", 
                           jue as "JUE", vie as "VIE", sab as "SAB", dom as "DOM"
                    FROM inspecciones_preop_items
                    WHERE inspeccion_id = %s
                    ORDER BY seccion, item_numero
                """, c, params=[inspeccion_id])
        except Exception:
            return pd.DataFrame()
        finally:
            self.release(c)

    def obtener_todos_los_items(self, ids: list) -> pd.DataFrame:
        if not ids: return pd.DataFrame()
        c = None
        try:
            c = self.conn()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return pd.read_sql("""
                    SELECT inspeccion_id, seccion, item_numero, descripcion, 
                           lun as "LUN", mar as "MAR", mie as "MIE", 
                           jue as "JUE", vie as "VIE", sab as "SAB", dom as "DOM"
                    FROM inspecciones_preop_items
                    WHERE inspeccion_id = ANY(%s)
                    ORDER BY inspeccion_id, seccion, item_numero
                """, c, params=[ids])
        except Exception:
            return pd.DataFrame()
        finally:
            self.release(c)

    def stats_dashboard(self, f_ini, f_fin) -> pd.DataFrame:
        c = None
        try:
            c = self.conn()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return pd.read_sql("""
                    SELECT i.id, i.fecha_ini, i.maquina, i.trabajador, i.estado,
                           SUM(
                               (CASE WHEN it.lun = 'NC' THEN 1 ELSE 0 END) +
                               (CASE WHEN it.mar = 'NC' THEN 1 ELSE 0 END) +
                               (CASE WHEN it.mie = 'NC' THEN 1 ELSE 0 END) +
                               (CASE WHEN it.jue = 'NC' THEN 1 ELSE 0 END) +
                               (CASE WHEN it.vie = 'NC' THEN 1 ELSE 0 END) +
                               (CASE WHEN it.sab = 'NC' THEN 1 ELSE 0 END) +
                               (CASE WHEN it.dom = 'NC' THEN 1 ELSE 0 END)
                           ) as num_nc,
                           COUNT(it.id) * 7 as total_items
                    FROM inspecciones_preop i
                    LEFT JOIN inspecciones_preop_items it ON it.inspeccion_id = i.id
                    WHERE i.fecha_ini >= %s AND i.fecha_ini <= %s
                    GROUP BY i.id, i.fecha_ini, i.maquina, i.trabajador, i.estado
                    ORDER BY i.fecha_ini
                """, c, params=[f_ini, f_fin])
        except Exception as e:
            return pd.DataFrame()
        finally:
            self.release(c)


# ==================== HELPERS DE UI (GRID EDITABLE) ====================
def render_data_editor(seccion_label: str, items_lista: list, key: str, df_previo=None):
    st.markdown(f"<div class='seccion-titulo'>📋 {seccion_label}</div>", unsafe_allow_html=True)
    
    if df_previo is not None and not df_previo.empty:
        df = df_previo.copy()
        df.rename(columns={"item_numero": "N°", "descripcion": "Descripción"}, inplace=True)
    else:
        df = pd.DataFrame({
            "N°": range(1, len(items_lista) + 1),
            "Descripción": items_lista,
            "LUN": ["-"] * len(items_lista), "MAR": ["-"] * len(items_lista),
            "MIE": ["-"] * len(items_lista), "JUE": ["-"] * len(items_lista),
            "VIE": ["-"] * len(items_lista), "SAB": ["-"] * len(items_lista),
            "DOM": ["-"] * len(items_lista),
        })

    config = {
        "N°": st.column_config.NumberColumn("N°", width="small", disabled=True),
        "Descripción": st.column_config.TextColumn("Descripción", width="large", disabled=True)
    }
    for d in DIAS_SEMANA:
        config[d] = st.column_config.SelectboxColumn(d, options=OPCIONES_INSPECCION, required=True, width="small")

    return st.data_editor(df, key=key, hide_index=True, column_config=config, use_container_width=True)

def empaquetar_items(df_au, df_epp, df_elec):
    items = []
    def _add(df_sec, nombre_sec):
        for _, r in df_sec.iterrows():
            items.append({
                "seccion": nombre_sec, "item_numero": r["N°"], "descripcion": r["Descripción"],
                "LUN": r["LUN"], "MAR": r["MAR"], "MIE": r["MIE"], "JUE": r["JUE"], 
                "VIE": r["VIE"], "SAB": r["SAB"], "DOM": r["DOM"]
            })
    _add(df_au, "ANTES DE SU USO")
    _add(df_epp, "ELEMENTOS DE PROTECCIÓN PERSONAL")
    _add(df_elec, "SEGURIDAD ELÉCTRICA")
    return items

def validar_datos_control(trabajador, revisado_por, cliente, resp_mant) -> list:
    errores = []
    if not trabajador.strip(): errores.append("👷 **Trabajador** es obligatorio.")
    if not revisado_por.strip(): errores.append("👤 **Revisado por** es obligatorio.")
    if not cliente.strip(): errores.append("🏢 **Cliente / Proyecto** es obligatorio.")
    if not resp_mant.strip(): errores.append("🔧 **Responsable de Mantenimiento** es obligatorio.")
    return errores


# ==================== EXCEL ====================
def generar_excel(df_inspecciones: pd.DataFrame, db: "DB", titulo: str) -> bytes:
    wb = Workbook()
    ft_titulo = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
    ft_header = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
    ft_normal = Font(name="Calibri", size=9)
    ft_nc     = Font(name="Calibri", size=9, color="C0392B", bold=True)
    ft_apro   = Font(name="Calibri", size=9, color="1E8449")

    fill_titulo  = PatternFill("solid", start_color="0F2027")
    fill_header  = PatternFill("solid", start_color="203A43")
    fill_alt     = PatternFill("solid", start_color="EBF5FB")
    fill_nc_row  = PatternFill("solid", start_color="FADBD8")

    borde  = Border(left=Side(style="thin"), right=Side(style="thin"), top=Side(style="thin"), bottom=Side(style="thin"))
    centro = Alignment(horizontal="center", vertical="center", wrap_text=True)
    izq    = Alignment(horizontal="left", vertical="center", wrap_text=True)

    ids_list = df_inspecciones["id"].astype(int).tolist()
    df_all_items = db.obtener_todos_los_items(ids_list)
    items_por_id = {insp_id: grp for insp_id, grp in df_all_items.groupby("inspeccion_id")} if not df_all_items.empty else {}

    # --- HOJA 1: RESUMEN ---
    ws = wb.active
    ws.title = "Inspecciones"
    ws.merge_cells("A1:N1")
    ws["A1"] = f"🔧 {titulo}   |   Generado: {datetime.now(pytz.timezone('America/Bogota')).strftime('%d/%m/%Y %H:%M')}"
    ws["A1"].font = ft_titulo; ws["A1"].fill = fill_titulo; ws["A1"].alignment = centro

    cols_h1 = [("id","ID",6), ("fecha_ini","FECHA INI",12), ("fecha_fin","FECHA FIN",12),
               ("maquina","MÁQUINA",20), ("trabajador","TRABAJADOR",24), ("revisado_por","REVISADO POR",24),
               ("cliente_proyecto","CLIENTE/PROY.",20), ("responsable_mantenimiento","RESP. MANT.",24),
               ("estado","ESTADO",18), ("num_nc","# NC (Semana)",12), ("observaciones","OBSERVACIONES",32)]
    
    for idx, (_, nombre, ancho) in enumerate(cols_h1, start=1):
        c = ws.cell(row=2, column=idx, value=nombre)
        c.font = ft_header; c.fill = fill_header; c.alignment = centro; c.border = borde
        ws.column_dimensions[get_column_letter(idx)].width = ancho

    for r_idx, (_, fila) in enumerate(df_inspecciones.iterrows(), start=3):
        insp_id = int(fila["id"])
        df_it = items_por_id.get(insp_id, pd.DataFrame())
        
        # Contar total de NCs en todos los días para esa semana
        num_nc = 0
        if not df_it.empty:
            num_nc = (df_it[DIAS_SEMANA] == 'NC').sum().sum()

        valores = {
            "id": insp_id, "fecha_ini": str(fila.get("fecha_ini","")), "fecha_fin": str(fila.get("fecha_fin","")),
            "maquina": fila.get("maquina",""), "trabajador": fila.get("trabajador",""),
            "revisado_por": fila.get("revisado_por",""), "cliente_proyecto": fila.get("cliente_proyecto",""),
            "responsable_mantenimiento": fila.get("responsable_mantenimiento",""),
            "estado": fila.get("estado",""), "num_nc": num_nc, "observaciones": fila.get("observaciones","")
        }
        fill_f = fill_nc_row if "Rechazada" in valores["estado"] else (fill_alt if r_idx % 2 == 0 else None)
        
        for c_idx, (key, _, _) in enumerate(cols_h1, start=1):
            val = valores.get(key, "")
            cell = ws.cell(row=r_idx, column=c_idx, value=str(val) if val != "" else "")
            cell.border = borde; cell.alignment = centro if key in ("id","fecha_ini","fecha_fin","estado","num_nc") else izq
            if fill_f: cell.fill = fill_f
            if key == "num_nc" and num_nc > 0: cell.font = ft_nc
            elif key == "estado" and "Aprobada" in str(val): cell.font = ft_apro
            else: cell.font = ft_normal

    # --- HOJA 2: DETALLE ---
    ws2 = wb.create_sheet("Detalle Semanal")
    hdrs2 = ["ID", "MÁQUINA", "SECCIÓN", "N°", "DESCRIPCIÓN", "LUN", "MAR", "MIE", "JUE", "VIE", "SAB", "DOM"]
    anchos2 = [8, 18, 30, 5, 60, 6, 6, 6, 6, 6, 6, 6]
    for ci, (h, w) in enumerate(zip(hdrs2, anchos2), start=1):
        c = ws2.cell(1, ci, h); c.font = ft_header; c.fill = fill_header; c.alignment = centro; c.border = borde
        ws2.column_dimensions[get_column_letter(ci)].width = w

    fila2 = 2
    for _, insp in df_inspecciones.iterrows():
        insp_id = int(insp["id"])
        df_it = items_por_id.get(insp_id, pd.DataFrame())
        if df_it.empty: continue
        for _, it in df_it.iterrows():
            vals = [insp_id, str(insp.get("maquina","")), str(it.get("seccion","")), int(it.get("item_numero",0)), str(it.get("descripcion",""))]
            vals.extend([str(it.get(d, "-")) for d in DIAS_SEMANA])
            for ci, v in enumerate(vals, start=1):
                c = ws2.cell(fila2, ci, v); c.border = borde; c.alignment = izq if ci == 5 else centro
                if v == "NC":
                    c.font = ft_nc; c.fill = fill_nc_row
                else:
                    c.font = ft_normal
            fila2 += 1

    output = io.BytesIO()
    wb.save(output)
    return output.getvalue()


# ==================== MAIN ====================
def main():
    st.markdown("""
    <div class="main-header">
        <h1>🔧 INSPECCIONES PREOPERACIONALES SEMANALES</h1>
        <p>Registro y seguimiento de inspecciones de equipos — SCA ZF</p>
    </div>
    """, unsafe_allow_html=True)

    if "db" not in st.session_state: st.session_state.db = DB()
    if "editando_id" not in st.session_state: st.session_state.editando_id = None
    db = st.session_state.db

    tab1, tab2, tab3 = st.tabs(["📝 Registrar Semana", "🔍 Historial y Reportes", "📊 Dashboard"])

    # =========================================================================
    # TAB 1 – NUEVA INSPECCIÓN
    # =========================================================================
    with tab1:
        st.markdown("### Registrar Inspección Preoperacional (Semanal)")

        st.markdown("<div class='seccion-titulo'>🏭 1. DATOS DEL EQUIPO</div>", unsafe_allow_html=True)
        d1, d2, d3, d4, d5 = st.columns([1.5, 1.2, 1, 1, 1])
        with d1: 
            # Selector de rango (Semana)
            hoy = date.today()
            inicio_sem = hoy - timedelta(days=hoy.weekday())
            rango_f = st.date_input("📅 Rango de Fechas (Semana)", value=(inicio_sem, inicio_sem + timedelta(days=6)), key="n_rango")
        with d2: maquina_sel  = st.selectbox("⚙️ Máquina", MAQUINAS, key="n_maquina")
        with d3: modelo_inp   = st.text_input("Modelo", placeholder="Ej: ZF-200X", key="n_modelo")
        with d4: marca_inp    = st.text_input("Marca", placeholder="Ej: SCA", key="n_marca")
        with d5: placa_inp    = st.text_input("Placa / Serie", placeholder="Ej: MQ-001", key="n_placa")

        st.markdown("<div class='seccion-titulo'>🔍 2. LISTA DE ACTIVIDADES (Llene los días trabajados)</div>", unsafe_allow_html=True)
        st.caption("Selecciona **C** = Cumple | **NC** = No Cumple | **N/A** = No Aplica | **-** = No evaluado")
        
        # Grid Estilo Excel
        df_au = render_data_editor("ANTES DE SU USO", ITEMS_ANTES_USO, "new_au")
        df_epp = render_data_editor("ELEMENTOS DE PROTECCIÓN PERSONAL", ITEMS_EPP, "new_epp")
        df_elec = render_data_editor("SEGURIDAD ELÉCTRICA", ITEMS_ELECTRICA, "new_elec")

        st.markdown("<div class='seccion-titulo'>📋 4. DATOS DE CONTROL — <span class='campo-obligatorio'>* Obligatorio</span></div>", unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        with c1: trabajador_inp = st.text_input("👷 Trabajador *", key="n_trab")
        with c2: revisado_inp   = st.text_input("👤 Revisado por *", key="n_rev")
        with c3: cliente_inp    = st.text_input("🏢 Cliente / Proyecto *", key="n_cli")
        with c4: resp_mant_inp  = st.text_input("🔧 Resp. Mantenimiento *", key="n_mant")

        e1, e2 = st.columns([1, 3])
        with e1: estado_inp = st.selectbox("🚦 Estado Final", ESTADOS_INSPECCION, key="n_estado")
        with e2: obs_inp    = st.text_area("💬 Observaciones de la semana", height=80, key="n_obs")

        st.divider()
        if st.button("💾 Guardar Inspección Semanal", type="primary", use_container_width=True):
            if not isinstance(rango_f, tuple) or len(rango_f) != 2:
                st.error("❌ Por favor selecciona la Fecha Inicial y Fecha Final de la semana.")
            else:
                errores = validar_datos_control(trabajador_inp, revisado_inp, cliente_inp, resp_mant_inp)
                if errores:
                    st.error("❌ " + " | ".join(errores))
                else:
                    items_form = empaquetar_items(df_au, df_epp, df_elec)
                    datos = {
                        "fecha_ini": rango_f[0], "fecha_fin": rango_f[1], "maquina": maquina_sel,
                        "modelo": modelo_inp, "marca": marca_inp, "placa": placa_inp,
                        "trabajador": trabajador_inp.strip(), "revisado_por": revisado_inp.strip(),
                        "cliente_proyecto": cliente_inp.strip(), "responsable_mantenimiento": resp_mant_inp.strip(),
                        "estado": estado_inp.split(" ", 1)[1] if " " in estado_inp else estado_inp,
                        "observaciones": obs_inp,
                    }
                    if db.guardar_inspeccion(datos, items_form):
                        st.success(f"✅ Inspección Semanal guardada con éxito para {maquina_sel}.")
                        st.balloons()

    # =========================================================================
    # TAB 2 – HISTORIAL
    # =========================================================================
    with tab2:
        st.markdown("### 🔍 Historial de Inspecciones")

        with st.expander("🛠️ Filtros", expanded=True):
            f1, f2, f3, f4, f5 = st.columns(5)
            with f1: fi = st.date_input("Desde (Fecha Inicial)", datetime.now() - timedelta(days=30))
            with f2: ff = st.date_input("Hasta (Fecha Inicial)", datetime.now())
            with f3: fm = st.selectbox("Máquina", ["Todas"] + MAQUINAS)
            with f4: ftrab = st.text_input("Trabajador")
            with f5: fe = st.selectbox("Estado", ["Todos"] + [e.split(" ", 1)[1] for e in ESTADOS_INSPECCION])

        df_hist = db.obtener_inspecciones(
            fi, ff, fm if fm != "Todas" else None,
            fe if fe != "Todos" else None, ftrab if ftrab else None
        )

        if not df_hist.empty:
            st.divider()
            col_e1, col_e2 = st.columns([2, 5])
            with col_e1:
                nombre_rep = st.text_input("Nombre del Excel", value="Inspecciones_Semanales")
            with col_e2:
                st.markdown("<br>", unsafe_allow_html=True)
                excel_data = generar_excel(df_hist, db, titulo=nombre_rep)
                st.download_button("⬇️ Descargar Excel", data=excel_data,
                                   file_name=f"{nombre_rep}.xlsx", type="primary")

            st.divider()
            cols_tabla = ["id","fecha_ini","fecha_fin","maquina","trabajador","estado"]
            st.dataframe(df_hist[cols_tabla], use_container_width=True, hide_index=True)

            st.divider()
            st.subheader("✏️ Ver Detalle / Editar Semana")
            df_hist["_label"] = df_hist.apply(
                lambda r: f"ID {r['id']} | Sem: {r['fecha_ini']} al {r['fecha_fin']} | {r['maquina']} | {r.get('trabajador','')}", axis=1
            )
            sel = st.selectbox("Seleccionar inspección:", [""] + df_hist["_label"].tolist())

            if sel:
                vid = int(sel.split(" | ")[0].replace("ID ", ""))
                row = df_hist[df_hist["id"] == vid].iloc[0]
                df_items_sel = db.obtener_items_inspeccion(vid)
                editando = st.session_state.editando_id == vid

                if not editando:
                    c1, c2, c3 = st.columns(3)
                    c1.info(f"**Máquina:** {row['maquina']}\n\n**Semana:** {row['fecha_ini']} al {row['fecha_fin']}")
                    c2.write(f"**Trabajador:** {row.get('trabajador','')}\n\n**Revisado por:** {row.get('revisado_por','')}")
                    c3.write(f"**Estado:** {row.get('estado','')}\n\n**Observaciones:** {row.get('observaciones','')}")

                    st.markdown("#### Detalle de días calificados:")
                    st.dataframe(df_items_sel, use_container_width=True, hide_index=True)

                    bc1, bc2 = st.columns(2)
                    if bc1.button("✏️ Editar esta semana", use_container_width=True):
                        st.session_state.editando_id = vid
                        st.rerun()
                    if bc2.button("🗑️ Eliminar", use_container_width=True):
                        if db.eliminar_inspeccion(vid):
                            st.success("✅ Eliminada.")
                            st.rerun()
                else:
                    st.warning(f"#### ✏️ Editando ID {vid}")
                    
                    df_au_prev   = df_items_sel[df_items_sel["seccion"] == "ANTES DE SU USO"]
                    df_epp_prev  = df_items_sel[df_items_sel["seccion"] == "ELEMENTOS DE PROTECCIÓN PERSONAL"]
                    df_elec_prev = df_items_sel[df_items_sel["seccion"] == "SEGURIDAD ELÉCTRICA"]

                    ec1, ec2, ec3 = st.columns([1.5, 1, 1])
                    e_rango = ec1.date_input("Semana", value=(row["fecha_ini"], row["fecha_fin"]))
                    maq_idx = MAQUINAS.index(row["maquina"]) if row["maquina"] in MAQUINAS else 0
                    e_maq = ec2.selectbox("Máquina", MAQUINAS, index=maq_idx)
                    e_estado = ec3.selectbox("Estado", [e.split(" ",1)[1] for e in ESTADOS_INSPECCION], 
                                             index=[e.split(" ",1)[1] for e in ESTADOS_INSPECCION].index(row["estado"]) if row["estado"] in [e.split(" ",1)[1] for e in ESTADOS_INSPECCION] else 0)

                    e_df_au = render_data_editor("ANTES DE SU USO", ITEMS_ANTES_USO, f"e_au_{vid}", df_au_prev)
                    e_df_epp = render_data_editor("EPP", ITEMS_EPP, f"e_epp_{vid}", df_epp_prev)
                    e_df_elec = render_data_editor("ELÉCTRICA", ITEMS_ELECTRICA, f"e_elec_{vid}", df_elec_prev)

                    e_obs = st.text_area("Observaciones", value=row["observaciones"])

                    sg1, sg2 = st.columns(2)
                    if sg1.button("💾 Guardar Cambios", type="primary", use_container_width=True):
                        if len(e_rango) == 2:
                            d_edit = {
                                "fecha_ini": e_rango[0], "fecha_fin": e_rango[1], "maquina": e_maq,
                                "modelo": row["modelo"], "marca": row["marca"], "placa": row["placa"],
                                "trabajador": row["trabajador"], "revisado_por": row["revisado_por"],
                                "cliente_proyecto": row["cliente_proyecto"], "responsable_mantenimiento": row["responsable_mantenimiento"],
                                "estado": e_estado, "observaciones": e_obs
                            }
                            if db.actualizar_inspeccion(vid, d_edit, empaquetar_items(e_df_au, e_df_epp, e_df_elec)):
                                st.success("✅ Actualizado!")
                                st.session_state.editando_id = None
                                st.rerun()
                    if sg2.button("❌ Cancelar", use_container_width=True):
                        st.session_state.editando_id = None
                        st.rerun()
        else:
            st.warning("No hay inspecciones con los filtros seleccionados.")

    # =========================================================================
    # TAB 3 – DASHBOARD
    # =========================================================================
    with tab3:
        st.markdown("### 📊 Dashboard de Inspecciones")
        try:
            import plotly.express as px
            
            rango_dash = st.date_input("Filtrar Semanas (Por Fecha Inicial)", value=(datetime.now().replace(day=1).date(), datetime.now().date()))
            if not isinstance(rango_dash, tuple) or len(rango_dash) != 2:
                st.info("Selecciona el rango de fechas completo.")
                return

            df_s = db.stats_dashboard(rango_dash[0], rango_dash[1])
            if df_s.empty:
                st.info("No hay datos en este período.")
                return

            total = len(df_s)
            apro = len(df_s[df_s["estado"].str.contains("Aprobada", na=False)])
            pct = round(apro / total * 100) if total > 0 else 0
            total_nc = int(df_s["num_nc"].sum())

            k1, k2, k3 = st.columns(3)
            k1.metric("Semanas Registradas", total)
            k2.metric("✅ Semanas Aprobadas", apro, f"{pct}%")
            k3.metric("🔴 Total NC (Todos los días)", total_nc)

            st.divider()
            g1, g2 = st.columns(2)
            with g1:
                st.markdown("#### % Aprobación por Máquina")
                resumen_apr = df_s.groupby("maquina", as_index=False).agg(
                    aprobadas=("estado", lambda x: x.str.contains("Aprobada", na=False).sum()),
                    total=("estado", "count")
                )
                resumen_apr["pct_apro"] = (resumen_apr["aprobadas"] / resumen_apr["total"] * 100).round(1)
                fig5 = px.bar(resumen_apr.sort_values("pct_apro"), x="pct_apro", y="maquina", orientation="h", color="pct_apro", color_continuous_scale="Greens", text="pct_apro")
                st.plotly_chart(fig5, use_container_width=True)

            with g2:
                st.markdown("#### Evolución Semanal")
                df_ev = df_s.groupby("fecha_ini").size().reset_index(name="registros")
                fig2 = px.line(df_ev, x="fecha_ini", y="registros", markers=True)
                st.plotly_chart(fig2, use_container_width=True)

        except Exception as e:
            st.warning("Para ver el Dashboard instala plotly: `pip install plotly`")


if __name__ == "__main__":
    main()
