import streamlit as st
import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2 import OperationalError
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import io
import math
import pytz
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ==================== CONFIGURACIÓN ====================
st.set_page_config(
    page_title="Sistema de Cubicaje",
    layout="wide",
    page_icon="📦",
    initial_sidebar_state="expanded"
)

# ==================== CREDENCIALES ====================
SUPABASE_DB_URL = "postgresql://postgres.ogfenizdijcboekqhuhd:Conejito200$@aws-1-us-west-2.pooler.supabase.com:6543/postgres"

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
    .main-header p { color: #a0c4d8; margin: 0.3rem 0 0 0; font-size: 0.9rem; }

    .seccion-titulo {
        background: #203a43; color: white; padding: 0.4rem 1rem;
        border-radius: 6px; font-weight: 700; font-size: 0.95rem;
        margin: 1rem 0 0.5rem 0; letter-spacing: 0.5px;
    }
    .metric-card {
        background: linear-gradient(135deg, #0f2027, #203a43, #2c5364);
        border-radius: 10px; padding: 18px; color: white;
        text-align: center; border: 1px solid rgba(255,255,255,0.1);
    }
    .metric-value { font-size: 1.9rem; font-weight: 700; color: #00d4ff;
                    font-family: 'Barlow Condensed', sans-serif; }
    .metric-label { font-size: 0.82rem; opacity: 0.75; margin-top: 4px; }

    .kpi-box {
        background: white; border-radius: 10px; padding: 1rem 1.2rem;
        border-left: 5px solid #2c5364; box-shadow: 0 2px 8px rgba(0,0,0,0.07);
    }
    .badge-c   { background:#2ecc71; color:white; border-radius:4px; padding:2px 8px;
                 font-weight:700; font-size:0.8rem; }
    .badge-nc  { background:#e74c3c; color:white; border-radius:4px; padding:2px 8px;
                 font-weight:700; font-size:0.8rem; }
    div[data-testid="stTabs"] button {
        font-family: 'Barlow Condensed', sans-serif;
        font-weight: 600; font-size: 1rem; letter-spacing: 0.5px;
    }
</style>
""", unsafe_allow_html=True)


# ==================== BASE DE DATOS ====================
@st.cache_resource
def get_pool():
    try:
        return pg_pool.SimpleConnectionPool(
            minconn=1, maxconn=5,
            dsn=SUPABASE_DB_URL,
            sslmode="require",
            connect_timeout=15,
            options="-c statement_timeout=30000"
        )
    except OperationalError as e:
        st.error(f"❌ Error crítico de conexión: {e}")
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
            try:
                return psycopg2.connect(
                    dsn=SUPABASE_DB_URL, sslmode="require", connect_timeout=15
                )
            except OperationalError as e:
                st.error(f"❌ No se pudo conectar: {e}")
                st.stop()

    def release(self, c):
        try:
            if c and not c.closed:
                self.pool.putconn(c)
        except Exception:
            pass

    def init(self):
        """Crea las tablas de cubicaje si no existen."""
        c = None
        try:
            c = self.conn()
            cur = c.cursor()
            # Tabla principal de envíos / lotes de cubicaje
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cubicaje_lotes (
                    id               SERIAL PRIMARY KEY,
                    fecha_registro   TIMESTAMP DEFAULT (now() AT TIME ZONE 'America/Bogota'),
                    nombre_lote      TEXT NOT NULL,
                    responsable      TEXT NOT NULL,
                    cliente_proyecto TEXT,
                    cont_tipo        TEXT,
                    cont_largo       NUMERIC NOT NULL,
                    cont_ancho       NUMERIC NOT NULL,
                    cont_alto        NUMERIC NOT NULL,
                    factor_vol       INTEGER DEFAULT 5000,
                    eficiencia_pct   NUMERIC,
                    contenedores_nec INTEGER,
                    vol_total_m3     NUMERIC,
                    peso_total_kg    NUMERIC,
                    observaciones    TEXT
                )
            """)
            # Tabla de productos por lote
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cubicaje_productos (
                    id          SERIAL PRIMARY KEY,
                    lote_id     INTEGER REFERENCES cubicaje_lotes(id) ON DELETE CASCADE,
                    nombre      TEXT NOT NULL,
                    largo       NUMERIC NOT NULL,
                    ancho       NUMERIC NOT NULL,
                    alto        NUMERIC NOT NULL,
                    peso_kg     NUMERIC DEFAULT 0,
                    cantidad    INTEGER DEFAULT 1,
                    vol_unit_m3 NUMERIC,
                    vol_total_m3 NUMERIC,
                    peso_vol_kg  NUMERIC,
                    peso_cobrable NUMERIC
                )
            """)
            c.commit()
            cur.close()
        except Exception as e:
            st.error(f"Error inicializando tablas: {e}")
        finally:
            self.release(c)

    # ── CRUD LOTES ──────────────────────────────────────
    def guardar_lote(self, datos: dict, productos: list) -> bool:
        c = None
        try:
            c = self.conn()
            cur = c.cursor()
            cur.execute("""
                INSERT INTO cubicaje_lotes
                (nombre_lote, responsable, cliente_proyecto, cont_tipo,
                 cont_largo, cont_ancho, cont_alto, factor_vol,
                 eficiencia_pct, contenedores_nec, vol_total_m3,
                 peso_total_kg, observaciones)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                datos["nombre_lote"], datos["responsable"], datos["cliente_proyecto"],
                datos["cont_tipo"], datos["cont_largo"], datos["cont_ancho"],
                datos["cont_alto"], datos["factor_vol"], datos["eficiencia_pct"],
                datos["contenedores_nec"], datos["vol_total_m3"],
                datos["peso_total_kg"], datos["observaciones"]
            ))
            lote_id = cur.fetchone()[0]
            for p in productos:
                cur.execute("""
                    INSERT INTO cubicaje_productos
                    (lote_id, nombre, largo, ancho, alto, peso_kg, cantidad,
                     vol_unit_m3, vol_total_m3, peso_vol_kg, peso_cobrable)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    lote_id, p["nombre"], p["largo"], p["ancho"], p["alto"],
                    p["peso_kg"], p["cantidad"], p["vol_unit_m3"],
                    p["vol_total_m3"], p["peso_vol_kg"], p["peso_cobrable"]
                ))
            c.commit()
            cur.close()
            return True
        except Exception as e:
            st.error(f"Error guardando lote: {e}")
            return False
        finally:
            self.release(c)

    def eliminar_lote(self, lote_id: int) -> bool:
        c = None
        try:
            c = self.conn()
            cur = c.cursor()
            cur.execute("DELETE FROM cubicaje_lotes WHERE id=%s", (lote_id,))
            c.commit()
            cur.close()
            return True
        except Exception as e:
            st.error(f"Error eliminando: {e}")
            return False
        finally:
            self.release(c)

    def obtener_lotes(self, fecha_ini=None, fecha_fin=None,
                      responsable=None, cliente=None) -> pd.DataFrame:
        c = None
        try:
            c = self.conn()
            q = """
                SELECT id, fecha_registro, nombre_lote, responsable,
                       cliente_proyecto, cont_tipo, cont_largo, cont_ancho,
                       cont_alto, factor_vol, eficiencia_pct,
                       contenedores_nec, vol_total_m3, peso_total_kg, observaciones
                FROM cubicaje_lotes WHERE 1=1
            """
            params = []
            if fecha_ini:
                q += " AND fecha_registro::date >= %s"; params.append(fecha_ini)
            if fecha_fin:
                q += " AND fecha_registro::date <= %s"; params.append(fecha_fin)
            if responsable:
                q += " AND responsable ILIKE %s"; params.append(f"%{responsable}%")
            if cliente:
                q += " AND cliente_proyecto ILIKE %s"; params.append(f"%{cliente}%")
            q += " ORDER BY fecha_registro DESC"
            return pd.read_sql(q, c, params=params)
        except Exception as e:
            st.error(f"Error consultando lotes: {e}")
            return pd.DataFrame()
        finally:
            self.release(c)

    def obtener_productos_lote(self, lote_id: int) -> pd.DataFrame:
        c = None
        try:
            c = self.conn()
            return pd.read_sql("""
                SELECT nombre, largo, ancho, alto, peso_kg, cantidad,
                       vol_unit_m3, vol_total_m3, peso_vol_kg, peso_cobrable
                FROM cubicaje_productos WHERE lote_id=%s
                ORDER BY id
            """, c, params=[lote_id])
        except Exception as e:
            st.error(f"Error obteniendo productos: {e}")
            return pd.DataFrame()
        finally:
            self.release(c)

    def obtener_todos_los_productos(self, ids: list) -> pd.DataFrame:
        if not ids:
            return pd.DataFrame()
        c = None
        try:
            c = self.conn()
            return pd.read_sql("""
                SELECT lote_id, nombre, largo, ancho, alto, peso_kg,
                       cantidad, vol_unit_m3, vol_total_m3, peso_vol_kg, peso_cobrable
                FROM cubicaje_productos WHERE lote_id = ANY(%s)
                ORDER BY lote_id, id
            """, c, params=[ids])
        except Exception as e:
            st.error(f"Error: {e}")
            return pd.DataFrame()
        finally:
            self.release(c)

    def stats_dashboard(self, fecha_ini, fecha_fin) -> pd.DataFrame:
        c = None
        try:
            c = self.conn()
            return pd.read_sql("""
                SELECT l.id, l.fecha_registro::date AS fecha,
                       l.nombre_lote, l.responsable, l.cliente_proyecto,
                       l.cont_tipo, l.eficiencia_pct, l.contenedores_nec,
                       l.vol_total_m3, l.peso_total_kg,
                       COUNT(p.id) AS num_productos,
                       COALESCE(SUM(p.cantidad),0) AS total_unidades
                FROM cubicaje_lotes l
                LEFT JOIN cubicaje_productos p ON p.lote_id = l.id
                WHERE l.fecha_registro::date >= %s
                  AND l.fecha_registro::date <= %s
                GROUP BY l.id
                ORDER BY l.fecha_registro DESC
            """, c, params=[fecha_ini, fecha_fin])
        except Exception as e:
            st.error(f"Error stats: {e}")
            return pd.DataFrame()
        finally:
            self.release(c)


# ==================== CÁLCULOS ====================
def calcular_volumen_m3(largo, ancho, alto):
    return (largo * ancho * alto) / 1_000_000

def calcular_peso_volumetrico(largo, ancho, alto, factor=5000):
    return (largo * ancho * alto) / factor

def calcular_cubicaje(df: pd.DataFrame, contenedor: dict) -> dict:
    vol_cont = contenedor["largo"] * contenedor["ancho"] * contenedor["alto"]
    vol_total = (df["largo"] * df["ancho"] * df["alto"] * df["cantidad"]).sum()
    eficiencia = min((vol_total / vol_cont) * 100, 100) if vol_cont > 0 else 0
    contenedores_nec = math.ceil(vol_total / vol_cont) if vol_cont > 0 else 0
    return {
        "vol_contenedor_m3":    vol_cont / 1_000_000,
        "vol_total_m3":         vol_total / 1_000_000,
        "eficiencia_pct":       round(eficiencia, 2),
        "contenedores_nec":     contenedores_nec,
        "vol_disponible_m3":    max(0, (vol_cont - vol_total) / 1_000_000),
    }

def enriquecer_df(df: pd.DataFrame, factor: int) -> pd.DataFrame:
    df = df.copy()
    for col in ["largo", "ancho", "alto", "peso_kg", "cantidad"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["cantidad"] = df["cantidad"].fillna(1).astype(int)
    df = df.dropna(subset=["largo", "ancho", "alto"])
    df["vol_unit_m3"]        = df.apply(lambda r: calcular_volumen_m3(r.largo, r.ancho, r.alto), axis=1)
    df["vol_total_m3"]       = df["vol_unit_m3"] * df["cantidad"]
    df["peso_vol_kg"]        = df.apply(lambda r: calcular_peso_volumetrico(r.largo, r.ancho, r.alto, factor), axis=1)
    df["peso_cobrable"]      = df.apply(lambda r: max(r.peso_kg, r.peso_vol_kg) if r.peso_kg > 0 else r.peso_vol_kg, axis=1)
    df["peso_cobrable_total"]= df["peso_cobrable"] * df["cantidad"]
    return df


# ==================== EXCEL ====================
def generar_excel(df_lotes: pd.DataFrame, db: DB) -> bytes:
    wb = Workbook()
    ft_tit  = Font(name="Calibri", bold=True, size=13, color="FFFFFF")
    ft_hdr  = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
    ft_norm = Font(name="Calibri", size=9)
    ft_num  = Font(name="Calibri", size=9, color="1A5276", bold=True)
    fill_tit = PatternFill("solid", start_color="0F2027")
    fill_hdr = PatternFill("solid", start_color="203A43")
    fill_alt = PatternFill("solid", start_color="EBF5FB")
    borde   = Border(left=Side(style="thin"), right=Side(style="thin"),
                     top=Side(style="thin"),  bottom=Side(style="thin"))
    centro  = Alignment(horizontal="center", vertical="center", wrap_text=True)
    izq     = Alignment(horizontal="left",   vertical="center", wrap_text=True)

    now_col = datetime.now(pytz.timezone("America/Bogota"))
    ids_list = df_lotes["id"].astype(int).tolist()
    df_all_prod = db.obtener_todos_los_productos(ids_list)
    prod_por_lote = {}
    if not df_all_prod.empty:
        for lid, grp in df_all_prod.groupby("lote_id"):
            prod_por_lote[int(lid)] = grp

    # ── Hoja 1: Lotes ──
    ws = wb.active
    ws.title = "Lotes"
    cols1 = [
        ("id","ID",6),("fecha_registro","FECHA",18),("nombre_lote","NOMBRE LOTE",28),
        ("responsable","RESPONSABLE",24),("cliente_proyecto","CLIENTE/PROY.",22),
        ("cont_tipo","CONTENEDOR",20),("eficiencia_pct","% EFIC.",10),
        ("contenedores_nec","# CONT.",9),("vol_total_m3","VOL TOTAL m³",14),
        ("peso_total_kg","PESO TOTAL kg",14),("observaciones","OBSERVACIONES",35),
    ]
    ws.merge_cells(f"A1:{get_column_letter(len(cols1))}1")
    ws["A1"] = f"📦 CUBICAJE — Generado: {now_col.strftime('%d/%m/%Y %H:%M')} | Total lotes: {len(df_lotes)}"
    ws["A1"].font = ft_tit; ws["A1"].fill = fill_tit; ws["A1"].alignment = centro
    ws.row_dimensions[1].height = 28
    for ci, (key, nombre, ancho) in enumerate(cols1, 1):
        c = ws.cell(2, ci, nombre)
        c.font = ft_hdr; c.fill = fill_hdr; c.alignment = centro; c.border = borde
        ws.column_dimensions[get_column_letter(ci)].width = ancho
    ws.row_dimensions[2].height = 26
    for ri, (_, row) in enumerate(df_lotes.iterrows(), 3):
        fill_f = fill_alt if ri % 2 == 0 else None
        for ci, (key, _, _) in enumerate(cols1, 1):
            val = row.get(key, "")
            cell = ws.cell(ri, ci, str(val) if val != "" else "")
            cell.border = borde
            cell.alignment = centro if key in ("id","eficiencia_pct","contenedores_nec","vol_total_m3","peso_total_kg") else izq
            cell.font = ft_num if key == "eficiencia_pct" else ft_norm
            if fill_f: cell.fill = fill_f
        ws.row_dimensions[ri].height = 18
    ws.freeze_panes = "A3"

    # ── Hoja 2: Productos ──
    ws2 = wb.create_sheet("Detalle Productos")
    cols2 = [
        ("lote_id","ID LOTE",8),("nombre","PRODUCTO",28),
        ("largo","L(cm)",8),("ancho","A(cm)",8),("alto","H(cm)",8),
        ("cantidad","CANT.",7),("vol_unit_m3","VOL/U m³",12),
        ("vol_total_m3","VOL TOTAL m³",14),("peso_kg","PESO REAL kg",13),
        ("peso_vol_kg","PESO VOL kg",12),("peso_cobrable","P. COBRABLE kg",15),
    ]
    ws2.merge_cells(f"A1:{get_column_letter(len(cols2))}1")
    ws2["A1"] = "Detalle de Productos por Lote"
    ws2["A1"].font = ft_tit; ws2["A1"].fill = fill_tit; ws2["A1"].alignment = centro
    ws2.row_dimensions[1].height = 26
    for ci, (_, nombre, ancho) in enumerate(cols2, 1):
        c = ws2.cell(2, ci, nombre)
        c.font = ft_hdr; c.fill = fill_hdr; c.alignment = centro; c.border = borde
        ws2.column_dimensions[get_column_letter(ci)].width = ancho
    ws2.row_dimensions[2].height = 24
    fila2 = 3
    for _, lote in df_lotes.iterrows():
        lid = int(lote["id"])
        df_p = prod_por_lote.get(lid, pd.DataFrame())
        if df_p.empty: continue
        for _, prod in df_p.iterrows():
            fill_f2 = fill_alt if fila2 % 2 == 0 else None
            vals = [lid, prod.get("nombre",""), prod.get("largo",""),
                    prod.get("ancho",""), prod.get("alto",""), prod.get("cantidad",""),
                    prod.get("vol_unit_m3",""), prod.get("vol_total_m3",""),
                    prod.get("peso_kg",""), prod.get("peso_vol_kg",""),
                    prod.get("peso_cobrable","")]
            for ci, v in enumerate(vals, 1):
                cell = ws2.cell(fila2, ci, v)
                cell.font = ft_norm; cell.border = borde
                cell.alignment = izq if ci == 2 else centro
                if fill_f2: cell.fill = fill_f2
            ws2.row_dimensions[fila2].height = 18
            fila2 += 1
    ws2.freeze_panes = "A3"

    # ── Hoja 3: Resumen por Responsable ──
    ws3 = wb.create_sheet("Por Responsable")
    ws3.merge_cells("A1:F1")
    ws3["A1"] = "Resumen por Responsable"
    ws3["A1"].font = ft_tit; ws3["A1"].fill = fill_tit; ws3["A1"].alignment = centro
    ws3.row_dimensions[1].height = 26
    hdrs3 = ["RESPONSABLE","TOTAL LOTES","VOL TOTAL m³","PESO TOTAL kg","EF. PROM %","CONT. USADOS"]
    anchos3 = [30,12,14,14,12,13]
    for ci, (h, w) in enumerate(zip(hdrs3, anchos3), 1):
        c = ws3.cell(2, ci, h)
        c.font = ft_hdr; c.fill = fill_hdr; c.alignment = centro; c.border = borde
        ws3.column_dimensions[get_column_letter(ci)].width = w
    if not df_lotes.empty and "responsable" in df_lotes.columns:
        res = df_lotes.groupby("responsable", as_index=False).agg(
            total=("responsable","count"),
            vol_total=("vol_total_m3","sum"),
            peso_total=("peso_total_kg","sum"),
            ef_prom=("eficiencia_pct","mean"),
            cont_usados=("contenedores_nec","sum"),
        ).sort_values("total", ascending=False)
        for ri, row in enumerate(res.itertuples(), 3):
            fill_f = fill_alt if ri % 2 == 0 else None
            vals = [row.responsable, int(row.total),
                    round(float(row.vol_total or 0),3),
                    round(float(row.peso_total or 0),2),
                    f"{round(float(row.ef_prom or 0),1)}%",
                    int(row.cont_usados or 0)]
            for ci, v in enumerate(vals, 1):
                cell = ws3.cell(ri, ci, v)
                cell.font = ft_norm; cell.border = borde
                cell.alignment = izq if ci == 1 else centro
                if fill_f: cell.fill = fill_f
            ws3.row_dimensions[ri].height = 18
    ws3.freeze_panes = "A3"

    output = io.BytesIO()
    wb.save(output)
    return output.getvalue()


# ==================== GRÁFICOS ====================
def grafico_gauge(eficiencia: float):
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=eficiencia,
        title={"text": "% Llenado", "font": {"color": "white"}},
        gauge={
            "axis":      {"range": [0, 100], "tickcolor": "white"},
            "bar":       {"color": "#00d4ff"},
            "bgcolor":   "#1a1a2e",
            "steps": [
                {"range": [0, 50],   "color": "#2c1654"},
                {"range": [50, 80],  "color": "#1a3a5c"},
                {"range": [80, 100], "color": "#0d4f3c"},
            ],
            "threshold": {"line": {"color": "#ff4b6e", "width": 4}, "value": 100}
        },
        number={"font": {"color": "#00d4ff", "size": 46}},
    ))
    fig.update_layout(
        paper_bgcolor="rgba(10,10,30,1)",
        font=dict(color="white"),
        height=300,
        margin=dict(t=40, b=10, l=10, r=10),
    )
    return fig

def grafico_torta(df: pd.DataFrame):
    vals = df["largo"] * df["ancho"] * df["alto"] * df["cantidad"]
    fig = go.Figure(go.Pie(
        labels=df["nombre"], values=vals,
        hole=0.45,
        marker=dict(colors=px.colors.qualitative.Vivid),
        textinfo="label+percent",
    ))
    fig.update_layout(
        paper_bgcolor="rgba(10,10,30,1)",
        font=dict(color="white"),
        height=300,
        margin=dict(t=20, b=10),
    )
    return fig

def grafico_barras_comp(df: pd.DataFrame):
    df_sorted = df.sort_values("vol_total_m3", ascending=True)
    fig = px.bar(
        df_sorted, x="vol_total_m3", y="nombre", orientation="h",
        text="vol_total_m3", color="vol_total_m3",
        color_continuous_scale="Blues",
    )
    fig.update_traces(texttemplate="%{text:.4f} m³", textposition="outside")
    fig.update_layout(
        paper_bgcolor="rgba(10,10,30,1)",
        plot_bgcolor="rgba(10,10,30,1)",
        font=dict(color="white"),
        height=max(250, len(df) * 38),
        margin=dict(t=10, b=10),
        coloraxis_showscale=False,
        xaxis_title="Volumen total (m³)",
        yaxis_title="",
    )
    return fig


# ==================== TAB 1: NUEVA CUBIACIÓN ====================
def tab_nueva_cubicacion(db: DB):
    st.markdown("### 📦 Registrar Nueva Cubicación")

    # ── Datos del lote ──
    st.markdown("<div class='seccion-titulo'>📋 1. DATOS DEL LOTE</div>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    with c1:
        nombre_lote  = st.text_input("Nombre del lote / despacho *", placeholder="Ej: Despacho Semana 22", key="nc_nombre")
    with c2:
        responsable  = st.text_input("Responsable *", placeholder="Nombre del operario/logístico", key="nc_resp")
    with c3:
        cliente_proj = st.text_input("Cliente / Proyecto", placeholder="Ej: Proyecto Norte", key="nc_cli")

    # ── Configuración contenedor ──
    st.markdown("<div class='seccion-titulo'>🚛 2. CONTENEDOR / VEHÍCULO</div>", unsafe_allow_html=True)
    presets = {
        "Personalizado":                   (0, 0, 0),
        "20' Estándar (589×235×239 cm)":   (589, 235, 239),
        "40' Estándar (1200×235×239 cm)":  (1200, 235, 239),
        "40' High Cube (1200×235×270 cm)": (1200, 235, 270),
        "Furgoneta (300×180×180 cm)":      (300, 180, 180),
        "Camión mediano (600×240×220 cm)": (600, 240, 220),
    }
    cp1, cp2, cp3, cp4, cp5 = st.columns([3, 1, 1, 1, 2])
    with cp1:
        tipo_cont = st.selectbox("Tipo predefinido", list(presets.keys()), key="nc_tipo")
    dl, da, dh = presets[tipo_cont]
    with cp2:
        cont_l = st.number_input("Largo (cm)", value=float(dl) if dl else 589.0, min_value=1.0, key="nc_cl")
    with cp3:
        cont_a = st.number_input("Ancho (cm)", value=float(da) if da else 235.0, min_value=1.0, key="nc_ca")
    with cp4:
        cont_h = st.number_input("Alto (cm)",  value=float(dh) if dh else 239.0, min_value=1.0, key="nc_ch")
    with cp5:
        factor_vol = st.selectbox("Factor peso volumétrico (divisor)", [5000, 6000, 4000], key="nc_factor")

    contenedor = {"largo": cont_l, "ancho": cont_a, "alto": cont_h}

    # ── Productos ──
    st.markdown("<div class='seccion-titulo'>📦 3. PRODUCTOS A CUBICAR</div>", unsafe_allow_html=True)
    modo = st.radio("Modo de ingreso", ["Manual", "Cargar CSV/Excel"], horizontal=True, key="nc_modo")

    if "productos_temp" not in st.session_state:
        st.session_state.productos_temp = pd.DataFrame(
            columns=["nombre","largo","ancho","alto","peso_kg","cantidad"]
        )

    if modo == "Manual":
        with st.form("form_prod", clear_on_submit=True):
            fc1, fc2 = st.columns(2)
            pnom  = fc1.text_input("Nombre del producto", placeholder='Ej: Caja TV 55"')
            pcant = fc2.number_input("Cantidad", min_value=1, value=1)
            fc3, fc4, fc5, fc6 = st.columns(4)
            pl = fc3.number_input("Largo (cm)", min_value=0.1, value=40.0)
            pa = fc4.number_input("Ancho (cm)", min_value=0.1, value=30.0)
            ph = fc5.number_input("Alto (cm)",  min_value=0.1, value=20.0)
            pp = fc6.number_input("Peso (kg)",  min_value=0.0, value=1.0)
            agregar = st.form_submit_button("➕ Agregar producto", use_container_width=True)
            if agregar and pnom:
                nueva = pd.DataFrame([{"nombre":pnom,"largo":pl,"ancho":pa,"alto":ph,
                                        "peso_kg":pp,"cantidad":pcant}])
                st.session_state.productos_temp = pd.concat(
                    [st.session_state.productos_temp, nueva], ignore_index=True
                )
                st.success(f"✅ '{pnom}' agregado.")

    else:
        archivo = st.file_uploader("Sube CSV o Excel", type=["csv","xlsx"], key="nc_file")
        if archivo:
            df_up = pd.read_csv(archivo) if archivo.name.endswith(".csv") else pd.read_excel(archivo)
            cols_req = {"nombre","largo","ancho","alto","peso_kg","cantidad"}
            if cols_req.issubset(set(df_up.columns)):
                st.session_state.productos_temp = df_up[list(cols_req)].copy()
                st.success(f"✅ {len(df_up)} productos cargados.")
            else:
                st.error(f"Columnas requeridas: {cols_req}")

    # Plantilla descargable
    plantilla = pd.DataFrame([{"nombre":"Ejemplo","largo":40,"ancho":30,"alto":20,"peso_kg":2.5,"cantidad":5}])
    st.download_button("📥 Plantilla CSV", data=plantilla.to_csv(index=False).encode(),
                       file_name="plantilla_cubicaje.csv", mime="text/csv")

    df_temp = st.session_state.productos_temp
    if not df_temp.empty:
        st.markdown("#### Productos en cola")
        edited = st.data_editor(df_temp, use_container_width=True, num_rows="dynamic", key="nc_editor")
        st.session_state.productos_temp = edited

        col_limpiar, _ = st.columns([1, 4])
        with col_limpiar:
            if st.button("🗑️ Limpiar lista"):
                st.session_state.productos_temp = pd.DataFrame(
                    columns=["nombre","largo","ancho","alto","peso_kg","cantidad"]
                )
                st.rerun()

        # ── Preview en tiempo real ──
        df_calc = enriquecer_df(st.session_state.productos_temp, factor_vol)
        res = calcular_cubicaje(df_calc, contenedor)

        st.markdown("<div class='seccion-titulo'>📊 PREVIEW DE CUBICAJE</div>", unsafe_allow_html=True)
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.markdown(f"""<div class='metric-card'><div class='metric-value'>{int(df_calc['cantidad'].sum())}</div>
                        <div class='metric-label'>Unidades totales</div></div>""", unsafe_allow_html=True)
        k2.markdown(f"""<div class='metric-card'><div class='metric-value'>{res['vol_total_m3']:.3f}</div>
                        <div class='metric-label'>Vol. total m³</div></div>""", unsafe_allow_html=True)
        k3.markdown(f"""<div class='metric-card'><div class='metric-value'>{res['eficiencia_pct']:.1f}%</div>
                        <div class='metric-label'>Eficiencia contenedor</div></div>""", unsafe_allow_html=True)
        k4.markdown(f"""<div class='metric-card'><div class='metric-value'>{res['contenedores_nec']}</div>
                        <div class='metric-label'>Contenedores necesarios</div></div>""", unsafe_allow_html=True)
        k5.markdown(f"""<div class='metric-card'><div class='metric-value'>{(df_calc['peso_kg']*df_calc['cantidad']).sum():.1f}</div>
                        <div class='metric-label'>Peso real total kg</div></div>""", unsafe_allow_html=True)

        g1, g2 = st.columns(2)
        with g1:
            st.plotly_chart(grafico_torta(df_calc), use_container_width=True)
        with g2:
            st.plotly_chart(grafico_gauge(res["eficiencia_pct"]), use_container_width=True)

    # ── Observaciones y Guardar ──
    st.markdown("<div class='seccion-titulo'>💬 4. OBSERVACIONES Y GUARDAR</div>", unsafe_allow_html=True)
    obs = st.text_area("Observaciones / notas del despacho", height=80, key="nc_obs")
    st.divider()

    if st.button("💾 Guardar Cubicación", type="primary", use_container_width=True):
        errores = []
        if not nombre_lote.strip():
            errores.append("📦 **Nombre del lote** es obligatorio.")
        if not responsable.strip():
            errores.append("👷 **Responsable** es obligatorio.")
        if st.session_state.productos_temp.empty:
            errores.append("📋 Debes agregar al menos **un producto**.")
        if errores:
            for e in errores: st.error(e)
        else:
            df_calc = enriquecer_df(st.session_state.productos_temp, factor_vol)
            res     = calcular_cubicaje(df_calc, contenedor)
            datos_lote = {
                "nombre_lote":    nombre_lote.strip(),
                "responsable":    responsable.strip(),
                "cliente_proyecto": cliente_proj.strip(),
                "cont_tipo":      tipo_cont,
                "cont_largo":     cont_l,
                "cont_ancho":     cont_a,
                "cont_alto":      cont_h,
                "factor_vol":     factor_vol,
                "eficiencia_pct": res["eficiencia_pct"],
                "contenedores_nec": res["contenedores_nec"],
                "vol_total_m3":   round(res["vol_total_m3"], 4),
                "peso_total_kg":  round((df_calc["peso_kg"] * df_calc["cantidad"]).sum(), 2),
                "observaciones":  obs,
            }
            productos_list = []
            for _, row in df_calc.iterrows():
                productos_list.append({
                    "nombre":       row["nombre"],
                    "largo":        row["largo"],
                    "ancho":        row["ancho"],
                    "alto":         row["alto"],
                    "peso_kg":      row["peso_kg"],
                    "cantidad":     int(row["cantidad"]),
                    "vol_unit_m3":  round(row["vol_unit_m3"], 6),
                    "vol_total_m3": round(row["vol_total_m3"], 6),
                    "peso_vol_kg":  round(row["peso_vol_kg"], 3),
                    "peso_cobrable":round(row["peso_cobrable"], 3),
                })
            if db.guardar_lote(datos_lote, productos_list):
                st.success(f"✅ Cubicación **{nombre_lote}** guardada — Eficiencia: {res['eficiencia_pct']:.1f}% | "
                           f"Contenedores: {res['contenedores_nec']}")
                st.session_state.productos_temp = pd.DataFrame(
                    columns=["nombre","largo","ancho","alto","peso_kg","cantidad"]
                )
                st.balloons()


# ==================== TAB 2: HISTORIAL ====================
def tab_historial(db: DB):
    st.markdown("### 🔍 Historial de Cubicaciones")

    with st.expander("🛠️ Filtros", expanded=True):
        f1, f2, f3, f4 = st.columns(4)
        with f1: fi   = st.date_input("Desde", datetime.now().replace(day=1), key="h_fi")
        with f2: ff   = st.date_input("Hasta", datetime.now(), key="h_ff")
        with f3: ftrab = st.text_input("Responsable", key="h_resp")
        with f4: fcli  = st.text_input("Cliente / Proyecto", key="h_cli")

    df_hist = db.obtener_lotes(fi, ff,
                                ftrab if ftrab else None,
                                fcli  if fcli  else None)

    if df_hist.empty:
        st.warning("No hay registros con los filtros seleccionados.")
        return

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total lotes",          len(df_hist))
    k2.metric("Vol. total m³",        f"{df_hist['vol_total_m3'].sum():.3f}")
    k3.metric("Peso total kg",        f"{df_hist['peso_total_kg'].sum():.1f}")
    k4.metric("Eficiencia promedio",  f"{df_hist['eficiencia_pct'].mean():.1f}%")

    st.divider()

    # Descarga Excel
    col_e1, col_e2 = st.columns([2, 5])
    with col_e1:
        rep_nombre = st.text_input("Nombre del reporte", value="Cubicaje_Reporte", key="rep_nom")
    with col_e2:
        st.markdown("<br>", unsafe_allow_html=True)
        excel_data = generar_excel(df_hist, db)
        tz_col = pytz.timezone("America/Bogota")
        st.download_button(
            "⬇️ Descargar Excel",
            data=excel_data,
            file_name=f"{rep_nombre}_{datetime.now(tz_col).strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
        )

    st.divider()
    cols_tabla = ["id","fecha_registro","nombre_lote","responsable","cliente_proyecto",
                  "cont_tipo","eficiencia_pct","contenedores_nec","vol_total_m3",
                  "peso_total_kg","observaciones"]
    cols_ex = [c for c in cols_tabla if c in df_hist.columns]
    st.dataframe(df_hist[cols_ex], use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("🔎 Ver Detalle / Eliminar")

    df_hist["_label"] = df_hist.apply(
        lambda r: f"ID {r['id']} | {str(r.get('fecha_registro',''))[:16]} | {r['nombre_lote']} | {r.get('responsable','')}",
        axis=1
    )
    sel = st.selectbox("Seleccionar lote:", df_hist["_label"].tolist(), key="h_sel")

    if sel:
        vid = int(sel.split(" | ")[0].replace("ID ",""))
        row = df_hist[df_hist["id"] == vid].iloc[0]

        col_i1, col_i2, col_i3 = st.columns(3)
        with col_i1:
            st.info(f"**Lote:** {row['nombre_lote']}")
            st.write(f"**Responsable:** {row.get('responsable','')}")
            st.write(f"**Cliente/Proyecto:** {row.get('cliente_proyecto','')}")
        with col_i2:
            st.write(f"**Contenedor:** {row.get('cont_tipo','')}")
            st.write(f"**Dimensiones:** {row.get('cont_largo','')} × {row.get('cont_ancho','')} × {row.get('cont_alto','')} cm")
            st.write(f"**Factor vol.:** {row.get('factor_vol','')}")
        with col_i3:
            st.metric("Eficiencia",         f"{row.get('eficiencia_pct',0):.1f}%")
            st.metric("Contenedores usados", int(row.get('contenedores_nec', 0)))
            st.metric("Vol. total m³",       f"{row.get('vol_total_m3',0):.3f}")

        df_prod = db.obtener_productos_lote(vid)
        if not df_prod.empty:
            st.markdown("**Productos del lote:**")
            st.dataframe(df_prod.rename(columns={
                "nombre":"Producto","largo":"L(cm)","ancho":"A(cm)","alto":"H(cm)",
                "peso_kg":"Peso real(kg)","cantidad":"Cant.",
                "vol_unit_m3":"Vol/u m³","vol_total_m3":"Vol total m³",
                "peso_vol_kg":"Peso vol(kg)","peso_cobrable":"P.Cobrable(kg)"
            }).style.format({
                "Vol/u m³":"{:.4f}","Vol total m³":"{:.4f}",
                "Peso real(kg)":"{:.2f}","Peso vol(kg)":"{:.2f}","P.Cobrable(kg)":"{:.2f}"
            }), use_container_width=True)

            g1, g2 = st.columns(2)
            with g1:
                st.plotly_chart(grafico_torta(df_prod), use_container_width=True)
            with g2:
                st.plotly_chart(grafico_barras_comp(df_prod), use_container_width=True)

        if st.button("🗑️ Eliminar este lote", key=f"del_{vid}"):
            if db.eliminar_lote(vid):
                st.success("✅ Lote eliminado.")
                st.rerun()


# ==================== TAB 3: DASHBOARD ====================
def tab_dashboard(db: DB):
    st.markdown("### 📊 Dashboard de Cubicaje")
    try:
        col_r, _ = st.columns([2, 4])
        with col_r:
            rango = st.date_input(
                "Período",
                value=(datetime.now().replace(day=1), datetime.now()),
                key="dash_rango"
            )
        if not (isinstance(rango, (list, tuple)) and len(rango) == 2):
            st.info("Selecciona un rango de fechas completo.")
            return

        df_s = db.stats_dashboard(rango[0], rango[1])
        if df_s.empty:
            st.info("No hay datos en este período.")
            return

        total       = len(df_s)
        vol_total   = df_s["vol_total_m3"].sum()
        peso_total  = df_s["peso_total_kg"].sum()
        ef_prom     = df_s["eficiencia_pct"].mean()
        cont_total  = df_s["contenedores_nec"].sum()

        k1,k2,k3,k4,k5 = st.columns(5)
        k1.metric("📦 Total Lotes",           total)
        k2.metric("📐 Vol. Total m³",          f"{vol_total:.3f}")
        k3.metric("⚖️ Peso Total kg",           f"{peso_total:.1f}")
        k4.metric("📊 Eficiencia Promedio",     f"{ef_prom:.1f}%")
        k5.metric("🚛 Contenedores Utilizados", int(cont_total))

        st.divider()
        g1, g2 = st.columns(2)
        with g1:
            st.markdown("#### Volumen por Lote (m³)")
            df_vol = df_s.sort_values("vol_total_m3", ascending=True)
            fig_v = px.bar(df_vol, x="vol_total_m3", y="nombre_lote",
                           orientation="h", text="vol_total_m3",
                           color="vol_total_m3", color_continuous_scale="Blues")
            fig_v.update_traces(texttemplate="%{text:.3f}", textposition="outside")
            fig_v.update_layout(paper_bgcolor="rgba(0,0,0,0)", height=max(280, total*40),
                                 coloraxis_showscale=False, margin=dict(t=10,b=10),
                                 xaxis_title="m³", yaxis_title="")
            st.plotly_chart(fig_v, use_container_width=True)

        with g2:
            st.markdown("#### Eficiencia por Lote (%)")
            df_ef = df_s.sort_values("eficiencia_pct", ascending=True)
            fig_ef = px.bar(df_ef, x="eficiencia_pct", y="nombre_lote",
                            orientation="h", text="eficiencia_pct",
                            color="eficiencia_pct", color_continuous_scale="Greens",
                            range_x=[0, 100])
            fig_ef.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_ef.update_layout(paper_bgcolor="rgba(0,0,0,0)", height=max(280, total*40),
                                  coloraxis_showscale=False, margin=dict(t=10,b=10),
                                  xaxis_title="%", yaxis_title="")
            st.plotly_chart(fig_ef, use_container_width=True)

        st.divider()
        g3, g4 = st.columns(2)
        with g3:
            st.markdown("#### Lotes por Responsable")
            df_resp = df_s.groupby("responsable").size().reset_index(name="lotes")
            fig_r = px.pie(df_resp, values="lotes", names="responsable",
                           hole=0.4, color_discrete_sequence=px.colors.qualitative.Vivid)
            fig_r.update_layout(paper_bgcolor="rgba(0,0,0,0)", height=300, margin=dict(t=20,b=10))
            st.plotly_chart(fig_r, use_container_width=True)

        with g4:
            st.markdown("#### Unidades Totales por Lote")
            df_un = df_s.sort_values("total_unidades", ascending=True)
            fig_u = px.bar(df_un, x="total_unidades", y="nombre_lote",
                           orientation="h", text="total_unidades",
                           color="total_unidades", color_continuous_scale="Oranges")
            fig_u.update_traces(textposition="outside")
            fig_u.update_layout(paper_bgcolor="rgba(0,0,0,0)", height=max(280, total*40),
                                 coloraxis_showscale=False, margin=dict(t=10,b=10),
                                 xaxis_title="Unidades", yaxis_title="")
            st.plotly_chart(fig_u, use_container_width=True)

        st.divider()
        st.markdown("#### 📋 Tabla Resumen")
        cols_dash = ["nombre_lote","responsable","cliente_proyecto","fecha",
                     "eficiencia_pct","contenedores_nec","vol_total_m3",
                     "peso_total_kg","num_productos","total_unidades"]
        cols_ex = [c for c in cols_dash if c in df_s.columns]
        st.dataframe(df_s[cols_ex], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Error en dashboard: {e}")


# ==================== MAIN ====================
def main():
    st.markdown("""
    <div class="main-header">
        <h1>📦 SISTEMA DE CUBICAJE</h1>
        <p>Registro, cálculo y seguimiento de cubicaciones y despachos — SCA ZF</p>
    </div>
    """, unsafe_allow_html=True)

    if "db" not in st.session_state:
        st.session_state.db = DB()

    db = st.session_state.db

    tab1, tab2, tab3 = st.tabs(["📦 Nueva Cubicación", "🔍 Historial y Reportes", "📊 Dashboard"])
    with tab1: tab_nueva_cubicacion(db)
    with tab2: tab_historial(db)
    with tab3: tab_dashboard(db)


if __name__ == "__main__":
    main()
