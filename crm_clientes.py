import streamlit as st
import psycopg2
from psycopg2 import pool as pg_pool
import pandas as pd
from datetime import datetime, timedelta, date
import io
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import pytz

# ==================== CONFIGURACIÓN ====================
st.set_page_config(
    page_title="Inspecciones Preoperacionales",
    layout="wide",
    page_icon="🔧",
    initial_sidebar_state="collapsed"
)

# ==================== CREDENCIALES (TAL CUAL) ====================
SUPABASE_DB_URL = "postgresql://postgres.ogfenizdijcboekqhuhd:Conejito200$@aws-1-us-west-2.pooler.supabase.com:6543/postgres"

# ==================== CATÁLOGO DE MÁQUINAS ACTUALIZADO ====================
MAQUINAS = [
    "Bloquera", "Caldera", "Dobladora", "Enderezadora", "Enmalladora",
    "Molino", "Montacarga", "Paneladora", "Pantógrafo", "Preexpansora", "Soldador manual"
]

DIAS_SEMANA = ["LUN", "MAR", "MIE", "JUE", "VIE", "SAB", "DOM"]

# ==================== ÍTEMS DE INSPECCIÓN ====================
ITEMS_ANTES_USO = [
    "¿Tiene permiso el trabajador para utilizar la máquina?",
    "¿Ha sido capacitado el trabajador para utilizar la máquina?",
    "¿Se ha verificado que la presión del aire se encuentre en niveles adecuados?",
    "¿Se han hecho los ajustes a la máquina de acuerdo a la programación ingresada?",
    "¿Los botones y controles funcionan adecuadamente?",
    "¿Las luces indicadores están funcionando correctamente?",
    "¿Se han inspeccionado los sensores?",
    "¿Las paradas de emergencias funcionan correctamente?",
    "¿Se ha comprobado que los cilindros de las mordazas funcionen bien?",
    "¿Se ha comprobado que los cilindros de la guillotina funcionen bien?",
    "¿Se ha comprobado que los cilindros de la dobladora funcionen bien?",
    "¿Se ha comprobado que los cilindros de las alas funcionen bien?",
    "¿Se ha comprobado que los cilindros del carro tracción funcionen bien?"
]

ITEMS_EPP = [
    "¿Se ha inspeccionado el lugar de trabajo? (material combustible, riesgo de incendios, instalaciones, etc.)",
    "¿La iluminación del área de trabajo es adecuada?",
    "¿Cuenta con los elementos de protección personal? (protector de ojos, oídos, guantes y calzado)",
    "¿El trabajador está vestido apropiadamente? (Camisa manga larga, pantalón de dotación)",
    "¿Se evidencia el NO uso de joyas, relojes y ropa holgada?",
    "¿Se tiene el cabello recogido si lo tiene largo?",
]

ITEMS_ELECTRICA = [
    "¿Se ha verificado que el cable de alimentación está en buen estado?",
    "¿Se ha revisado que el enchufe se encuentre en buenas condiciones?",
    "¿El interruptor de encendido funciona correctamente?",
]

OPCIONES_INSPECCION = ["-", "C", "NC", "N/A"]  # '-' indica que aún no se evalúa ese día
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
    .badge-c   { background: #2ecc71; color: white; border-radius: 4px; padding: 2px 8px; font-weight: 700; font-size: 0.8rem; }
    .badge-nc  { background: #e74c3c; color: white; border-radius: 4px; padding: 2px 8px; font-weight: 700; font-size: 0.8rem; }
    .badge-na  { background: #95a5a6; color: white; border-radius: 4px; padding: 2px 8px; font-weight: 700; font-size: 0.8rem; }
    .campo-obligatorio { color: #e74c3c; font-weight: bold; }
</style>
""", unsafe_allow_html=True)


# ==================== BASE DE DATOS ====================
@st.cache_resource
def get_pool():
    try:
        connection_pool = pg_pool.SimpleConnectionPool(
            minconn=1, maxconn=5, dsn=SUPABASE_DB_URL,
            sslmode="require", connect_timeout=15, options="-c statement_timeout=30000"
        )
        return connection_pool
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
            
            # Tabla principal adaptada a rango de fechas
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
            
            # Tabla de ítems adaptada a LUN, MAR, MIE...
            cur.execute("""
                CREATE TABLE IF NOT EXISTS inspecciones_preop_items (
                    id SERIAL PRIMARY KEY,
                    inspeccion_id INTEGER REFERENCES inspecciones_preop(id) ON DELETE CASCADE,
                    seccion TEXT NOT NULL,
                    item_numero INTEGER NOT NULL,
                    descripcion TEXT NOT NULL,
                    lun TEXT DEFAULT '-',
                    mar TEXT DEFAULT '-',
                    mie TEXT DEFAULT '-',
                    jue TEXT DEFAULT '-',
                    vie TEXT DEFAULT '-',
                    sab TEXT DEFAULT '-',
                    dom TEXT DEFAULT '-'
                )
            """)
            
            # Script de migración segura si las tablas ya existían antes
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
                datos["fecha_ini"], datos["fecha_fin"], datos["maquina"], datos["modelo"], datos["marca"],
                datos["placa"], datos["trabajador"], datos["revisado_por"],
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
                datos["fecha_ini"], datos["fecha_fin"], datos["maquina"], datos["modelo"], datos["marca"],
                datos["placa"], datos["trabajador"], datos["revisado_por"],
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
            return True
        except Exception:
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
            import warnings
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
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return pd.read_sql("""
                    SELECT seccion, item_numero, descripcion, lun as "LUN", mar as "MAR", 
                           mie as "MIE", jue as "JUE", vie as "VIE", sab as "SAB", dom as "DOM"
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
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return pd.read_sql("""
                    SELECT inspeccion_id, seccion, item_numero, descripcion, 
                           lun as "LUN", mar as "MAR", mie as "MIE", jue as "JUE", 
                           vie as "VIE", sab as "SAB", dom as "DOM"
                    FROM inspecciones_preop_items
                    WHERE inspeccion_id = ANY(%s)
                    ORDER BY inspeccion_id, seccion, item_numero
                """, c, params=[ids])
        except Exception:
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

    # Configuramos la tabla para que sea estilo Excel
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

def contar_nc(items: list) -> int:
    nc = 0
    for it in items:
        for d in DIAS_SEMANA:
            if it[d] == "NC": nc += 1
    return nc


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

    tab1, tab2 = st.tabs(["📝 Registrar Semana", "🔍 Historial y Excel"])

    # ================== TAB 1: REGISTRAR ==================
    with tab1:
        st.markdown("### Registrar Inspección Preoperacional (Semanal)")

        st.markdown("<div class='seccion-titulo'>🏭 1. DATOS DEL EQUIPO</div>", unsafe_allow_html=True)
        d1, d2, d3, d4, d5 = st.columns([1.5, 1, 1, 1, 1])
        
        # Selección de rango de fechas
        with d1: 
            rango_fechas = st.date_input("📅 Rango de Fechas (Semana)", value=(date.today() - timedelta(days=date.today().weekday()), date.today() + timedelta(days=6 - date.today().weekday())), key="n_rango")
        with d2: maquina_sel  = st.selectbox("⚙️ Máquina", MAQUINAS, key="n_maquina")
        with d3: modelo_inp   = st.text_input("Modelo", placeholder="Ej: ZF-200X", key="n_modelo")
        with d4: marca_inp    = st.text_input("Marca", placeholder="Ej: SCA", key="n_marca")
        with d5: placa_inp    = st.text_input("Placa / Serie", placeholder="Ej: MQ-001", key="n_placa")

        st.markdown("<div class='seccion-titulo'>🔍 2. LISTA DE ACTIVIDADES (Llene los días trabajados)</div>", unsafe_allow_html=True)
        st.caption("Selecciona **C** = Cumple | **NC** = No Cumple | **N/A** = No Aplica | **-** = No trabajado")
        
        # Renderizamos las tablas editables (GRID)
        df_au = render_data_editor("ANTES DE SU USO", ITEMS_ANTES_USO, "grid_au")
        df_epp = render_data_editor("3A. ELEMENTOS DE PROTECCIÓN PERSONAL", ITEMS_EPP, "grid_epp")
        df_elec = render_data_editor("3B. SEGURIDAD ELÉCTRICA", ITEMS_ELECTRICA, "grid_elec")

        st.markdown("<div class='seccion-titulo'>📋 4. DATOS DE CONTROL</div>", unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        with c1: trabajador_inp = st.text_input("👷 Trabajador *", key="n_trab")
        with c2: revisado_inp   = st.text_input("👤 Revisado por *", key="n_rev")
        with c3: cliente_inp    = st.text_input("🏢 Cliente / Proyecto *", key="n_cli")
        with c4: resp_mant_inp  = st.text_input("🔧 Resp. Mantenimiento *", key="n_mant")

        e1, e2 = st.columns([1, 3])
        with e1: estado_inp = st.selectbox("🚦 Estado Final Semana", ESTADOS_INSPECCION, key="n_estado")
        with e2: obs_inp    = st.text_area("💬 Observaciones Generales de la Semana", height=70, key="n_obs")

        if st.button("💾 Guardar Inspección Semanal", type="primary", use_container_width=True):
            if len(rango_fechas) != 2:
                st.error("❌ Por favor selecciona la Fecha Inicial y Fecha Final.")
            elif not trabajador_inp or not revisado_inp or not cliente_inp or not resp_mant_inp:
                st.error("❌ Los campos de control (Trabajador, Revisado por, Cliente, Resp. Mantenimiento) son obligatorios.")
            else:
                items_form = empaquetar_items(df_au, df_epp, df_elec)
                datos = {
                    "fecha_ini": rango_fechas[0], "fecha_fin": rango_fechas[1], "maquina": maquina_sel,
                    "modelo": modelo_inp, "marca": marca_inp, "placa": placa_inp,
                    "trabajador": trabajador_inp.strip(), "revisado_por": revisado_inp.strip(),
                    "cliente_proyecto": cliente_inp.strip(), "responsable_mantenimiento": resp_mant_inp.strip(),
                    "estado": estado_inp.split(" ", 1)[1] if " " in estado_inp else estado_inp,
                    "observaciones": obs_inp,
                }
                if db.guardar_inspeccion(datos, items_form):
                    st.success("✅ Inspección Semanal guardada correctamente.")
                    st.balloons()

    # ================== TAB 2: HISTORIAL ==================
    with tab2:
        st.markdown("### 🔍 Historial de Inspecciones")
        df_hist = db.obtener_inspecciones()
        
        if not df_hist.empty:
            cols_ex = ["id","fecha_ini","fecha_fin","maquina","trabajador","revisado_por","estado"]
            st.dataframe(df_hist[cols_ex], use_container_width=True, hide_index=True)

            st.divider()
            df_hist["_label"] = df_hist.apply(lambda r: f"ID {r['id']} | Sem: {r['fecha_ini']} al {r['fecha_fin']} | {r['maquina']} | {r.get('trabajador','')}", axis=1)
            sel = st.selectbox("Seleccionar inspección para ver/editar:", [""] + df_hist["_label"].tolist())

            if sel:
                vid = int(sel.split(" | ")[0].replace("ID ", ""))
                row = df_hist[df_hist["id"] == vid].iloc[0]
                df_items_sel = db.obtener_items_inspeccion(vid)
                editando = st.session_state.editando_id == vid

                if not editando:
                    c1, c2 = st.columns(2)
                    c1.info(f"**Máquina:** {row['maquina']} | **Semana:** {row['fecha_ini']} al {row['fecha_fin']}")
                    c2.info(f"**Trabajador:** {row.get('trabajador','')} | **Estado:** {row.get('estado','')}")
                    
                    st.markdown("#### Detalle de días calificados:")
                    st.dataframe(df_items_sel, use_container_width=True, hide_index=True)

                    if st.button("✏️ Editar esta inspección"):
                        st.session_state.editando_id = vid
                        st.rerun()
                else:
                    st.warning(f"✏️ Editando ID {vid}")
                    # Recuperar dfs
                    df_au_prev   = df_items_sel[df_items_sel["seccion"] == "ANTES DE SU USO"]
                    df_epp_prev  = df_items_sel[df_items_sel["seccion"] == "ELEMENTOS DE PROTECCIÓN PERSONAL"]
                    df_elec_prev = df_items_sel[df_items_sel["seccion"] == "SEGURIDAD ELÉCTRICA"]

                    ec1, ec2 = st.columns(2)
                    e_rango = ec1.date_input("Fechas", value=(row["fecha_ini"], row["fecha_fin"]), key=f"e_rango_{vid}")
                    maq_idx = MAQUINAS.index(row["maquina"]) if row["maquina"] in MAQUINAS else 0
                    e_maq = ec2.selectbox("Máquina", MAQUINAS, index=maq_idx, key=f"e_maq_{vid}")

                    e_df_au = render_data_editor("ANTES DE SU USO", ITEMS_ANTES_USO, f"e_grid_au_{vid}", df_au_prev)
                    e_df_epp = render_data_editor("EPP", ITEMS_EPP, f"e_grid_epp_{vid}", df_epp_prev)
                    e_df_elec = render_data_editor("ELÉCTRICA", ITEMS_ELECTRICA, f"e_grid_elec_{vid}", df_elec_prev)

                    if st.button("💾 Guardar Cambios"):
                        if len(e_rango) == 2:
                            d_edit = {
                                "fecha_ini": e_rango[0], "fecha_fin": e_rango[1], "maquina": e_maq,
                                "modelo": row["modelo"], "marca": row["marca"], "placa": row["placa"],
                                "trabajador": row["trabajador"], "revisado_por": row["revisado_por"],
                                "cliente_proyecto": row["cliente_proyecto"], "responsable_mantenimiento": row["responsable_mantenimiento"],
                                "estado": row["estado"], "observaciones": row["observaciones"]
                            }
                            if db.actualizar_inspeccion(vid, d_edit, empaquetar_items(e_df_au, e_df_epp, e_df_elec)):
                                st.success("✅ Actualizado!")
                                st.session_state.editando_id = None
                                st.rerun()

if __name__ == "__main__":
    main()
