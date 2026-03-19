"""
dashboard.py — Tablero de Transparencia en Contrataciones Públicas · Paraguay
Lee directamente los Parquet del cache/ incluidos en el repo.
Compatible con Streamlit Community Cloud.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path

ROOT      = Path(__file__).parent
CACHE_DIR = ROOT / "cache"

def _pq(module: str, name: str) -> pd.DataFrame:
    p = CACHE_DIR / module / f"{name}.parquet"
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()

# ─── Configuración ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Transparencia Pública Paraguay — Contrataciones",
    page_icon="🇵🇾", layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CSS Profesional ─────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:ital,wght@0,300;0,400;0,500;0,600;0,700;1,400&display=swap');

html, body, [class*="css"], [class*="st-"] {
    font-family: 'Inter', sans-serif !important;
}

/* Fondo principal */
.stApp { background: #f0f4f8; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1a237e 0%, #283593 60%, #1565c0 100%) !important;
}
[data-testid="stSidebar"] * { color: #e8eaf6 !important; }
[data-testid="stSidebar"] .stMarkdown h2,
[data-testid="stSidebar"] .stMarkdown h3 { color: #ffffff !important; font-weight: 700; }
[data-testid="stSidebar"] a { color: #90caf9 !important; }
[data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.2) !important; }
[data-testid="stSidebar"] .stInfo {
    background: rgba(255,255,255,0.12) !important;
    border: 1px solid rgba(255,255,255,0.25) !important;
    border-radius: 10px;
    color: #e8eaf6 !important;
}

/* Header principal */
.main-header {
    background: linear-gradient(135deg, #1a237e 0%, #1565c0 50%, #0288d1 100%);
    padding: 28px 36px 24px;
    border-radius: 16px;
    margin-bottom: 28px;
    box-shadow: 0 4px 24px rgba(26,35,126,0.18);
}
.main-header h1 {
    color: #ffffff !important;
    font-size: 28px !important;
    font-weight: 700 !important;
    margin: 0 0 6px 0 !important;
    letter-spacing: -0.5px;
}
.main-header p {
    color: rgba(255,255,255,0.82) !important;
    font-size: 14px !important;
    margin: 0 !important;
}

/* Tarjetas KPI */
.kcard {
    background: #ffffff;
    border-radius: 14px;
    padding: 20px 24px 18px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.07);
    border-left: 5px solid #1565c0;
    margin-bottom: 12px;
    transition: box-shadow .2s;
}
.kcard:hover { box-shadow: 0 4px 20px rgba(0,0,0,0.13); }
.kcard.warn { border-left-color: #ef6c00; }
.kcard.danger { border-left-color: #c62828; }
.kcard.ok { border-left-color: #2e7d32; }
.klbl {
    font-size: 11px;
    font-weight: 600;
    color: #607d8b;
    letter-spacing: .06em;
    text-transform: uppercase;
    margin-bottom: 6px;
}
.kval {
    font-size: 26px;
    font-weight: 700;
    color: #1a237e;
    line-height: 1.1;
}
.ksub {
    font-size: 12px;
    color: #78909c;
    margin-top: 4px;
}

/* Títulos de sección */
.sec-title {
    font-size: 16px;
    font-weight: 700;
    color: #1a237e;
    padding: 10px 0 6px 14px;
    border-left: 4px solid #1565c0;
    margin: 20px 0 12px 0;
    letter-spacing: -0.2px;
}

/* Panel de contenido (blanco) */
.panel {
    background: #ffffff;
    border-radius: 14px;
    padding: 20px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.06);
    margin-bottom: 16px;
}

/* Badges de alerta */
.badge-critico { background:#fde8e8;color:#c62828;padding:3px 10px;border-radius:20px;font-size:12px;font-weight:600; }
.badge-alto    { background:#fff3e0;color:#e65100;padding:3px 10px;border-radius:20px;font-size:12px;font-weight:600; }
.badge-ok      { background:#e8f5e9;color:#2e7d32;padding:3px 10px;border-radius:20px;font-size:12px;font-weight:600; }

/* Tabs */
[data-baseweb="tab-list"] { background: #e8eef5 !important; border-radius: 12px; padding: 4px; }
[data-baseweb="tab"] { border-radius: 8px !important; font-weight: 500 !important; color: #455a64 !important; }
[aria-selected="true"][data-baseweb="tab"] {
    background: #1565c0 !important;
    color: #ffffff !important;
}

/* Dataframe */
[data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ─── Colores para gráficos (tema claro profesional) ─────────────────────────
BG      = "#ffffff"
GRID    = "#f5f7fa"
TXT     = "#1a237e"
SUBTXT  = "#546e7a"
PAL     = ["#1565c0","#2196f3","#26c6da","#43a047","#ffa726","#ef5350","#ab47bc","#5c6bc0"]
PAL_SEQ = "Blues"

def chart_layout(**kwargs):
    return dict(
        paper_bgcolor=BG, plot_bgcolor=GRID,
        font=dict(family="Inter", color=SUBTXT, size=12),
        margin=dict(l=16, r=16, t=44, b=16),
        xaxis=dict(gridcolor="#e8eef5", linecolor="#cfd8dc", tickfont=dict(color=SUBTXT)),
        yaxis=dict(gridcolor="#e8eef5", linecolor="#cfd8dc", tickfont=dict(color=SUBTXT)),
        title_font=dict(color=TXT, size=14, family="Inter"),
        **kwargs
    )

# ─── Helpers ──────────────────────────────────────────────────────────────────
def fmtg(v):
    if not v: return "₲ 0"
    if v >= 1e12: return f"₲ {v/1e12:.1f}B"
    if v >= 1e9:  return f"₲ {v/1e9:.1f}MM"
    if v >= 1e6:  return f"₲ {v/1e6:.1f}M"
    return f"₲ {v:,.0f}"

def kpi(label, value, sub="", variant=""):
    if isinstance(value, float) and value > 9999:
        val = fmtg(value)
    elif isinstance(value, (int, float)):
        val = f"{int(value):,}"
    else:
        val = str(value)
    sub_html = f"<div class='ksub'>{sub}</div>" if sub else ""
    st.markdown(
        f"<div class='kcard {variant}'>"
        f"<div class='klbl'>{label}</div>"
        f"<div class='kval'>{val}</div>"
        f"{sub_html}</div>",
        unsafe_allow_html=True
    )

def sec(t):
    st.markdown(f"<div class='sec-title'>{t}</div>", unsafe_allow_html=True)

def emptyfig(key, msg="Sin datos disponibles"):
    fig = go.Figure()
    fig.add_annotation(text=msg, x=.5, y=.5, showarrow=False,
                       font=dict(size=14, color="#90a4ae"), xref="paper", yref="paper")
    lay = chart_layout()
    lay["height"] = 280
    lay["paper_bgcolor"] = GRID
    fig.update_layout(**lay)
    st.plotly_chart(fig, use_container_width=True, key=key)

# ─── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🇵🇾 Transparencia Pública")
    st.caption("Datos abiertos de contrataciones públicas · Paraguay")
    st.divider()
    st.info(
        "**Datos incluidos:** convocatorias y adjudicaciones 2025\n\n"
        "Fuente: [contrataciones.gov.py](https://contrataciones.gov.py/datos) — Licencia CC BY 4.0"
    )
    st.divider()
    cache_ok = CACHE_DIR.exists() and any(CACHE_DIR.rglob("*.parquet"))
    if cache_ok:
        total_kb = sum(p.stat().st_size for p in CACHE_DIR.rglob("*.parquet")) // 1024
        st.success(f"✅ Cache: {total_kb:,} KB")
    st.markdown("**💾 [Ver código en GitHub](https://github.com/diegomezapy/tableroDNCPpy)**")
    st.divider()
    st.markdown("##### 🔢 Buscar por RUC del proveedor")
    ruc_global = st.text_input("",
                               placeholder="ej: 80004886-5  o  80074991",
                               key="ruc_global",
                               label_visibility="collapsed")
    if ruc_global.strip():
        st.caption(f"🔍 Filtrando por RUC: **{ruc_global.strip()}**")

# ─── Header ──────────────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
  <h1>📊 Transparencia en Contrataciones Públicas</h1>
  <p>Paraguay · Datos Abiertos OCDS · Fuente: <a href="https://contrataciones.gov.py/datos" target="_blank" style="color:rgba(255,255,255,0.85);text-decoration:underline">contrataciones.gov.py</a> · Año 2025</p>
</div>
""", unsafe_allow_html=True)

# ─── Aviso Académico ──────────────────────────────────────────────────────────
st.markdown("""
<div style="background:#fff8e1;border:1.5px solid #f9a825;border-radius:12px;padding:16px 22px;margin-bottom:20px;display:flex;gap:14px;align-items:flex-start">
  <span style="font-size:26px;line-height:1">🎓</span>
  <div>
    <b style="color:#e65100;font-size:14px">ENSAYO ACADÉMICO — VERSIÓN DE PRUEBA · USO CON PRECAUCIÓN</b><br>
    <span style="color:#4e342e;font-size:13px;line-height:1.6">
      Esta herramienta es un <b>prototipo de investigación académica</b> elaborado con datos abiertos
      de la <a href="https://contrataciones.gov.py/datos" target="_blank" style="color:#bf360c">DNCP Paraguay (CC BY 4.0)</a>.
      Los análisis, comparaciones y alertas de precios son <b>indicadores estadísticos exploratorios</b>
      y <b>no constituyen prueba de irregularidad, denuncia formal ni dictamen técnico</b>.
      Los resultados deben interpretarse con criterio experto, considerando diferencias en
      unidades de medida, calidad, presentación y contexto de cada contratación.
      <b>No se autoriza su uso con fines legales, mediáticos o de acusación sin la debida verificación.</b>
    </span>
  </div>
</div>
""", unsafe_allow_html=True)

# ─── Cargar datos ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner="Cargando datos del tablero...")
def load():
    ea_c  = _pq("convocatorias","evolucion_anual")
    em_c  = _pq("convocatorias","evolucion_mensual")
    tent  = _pq("convocatorias","top_entidades")
    mod   = _pq("convocatorias","modalidades")
    mc    = _pq("convocatorias","muestra")
    ea_a  = _pq("adjudicaciones","evolucion_anual")
    em_a  = _pq("adjudicaciones","evolucion_mensual")
    tprov = _pq("adjudicaciones","top_proveedores")
    ma    = _pq("adjudicaciones","muestra")
    ea_k  = _pq("contratos","evolucion_anual")
    em_k  = _pq("contratos","evolucion_mensual")
    mk    = _pq("contratos","muestra")

    # Catálogo RUC → proveedor (para joins en tabs)
    ruc_cat = _pq("adjudicaciones", "catalogo_ruc")

    # Asegurar RUC en top_proveedores si no está ya
    if not tprov.empty and not ruc_cat.empty and "ruc" not in tprov.columns:
        tprov = tprov.merge(ruc_cat[["proveedor","ruc"]], on="proveedor", how="left")

    # Contar proveedores y entidades reales desde items_detalle (más preciso que top-20)
    items_f = CACHE_DIR / "adjudicaciones" / "items_detalle.parquet"
    n_proveedores = 0
    n_entidades   = 0
    if items_f.exists():
        try:
            items_meta = pd.read_parquet(items_f, columns=["proveedor","entidad"])
            n_proveedores = items_meta["proveedor"].dropna().nunique()
            n_entidades   = items_meta["entidad"].dropna().nunique()
        except Exception:
            n_proveedores = len(tprov) if not tprov.empty else 0
            n_entidades   = len(tent)  if not tent.empty else 0

    kpis = {
        "total_llamados":         int(ea_c["cantidad"].sum())  if not ea_c.empty else 0,
        "monto_estimado_total":   float(ea_c["monto"].sum())   if not ea_c.empty else 0.0,
        "total_adjudicaciones":   int(ea_a["cantidad"].sum())  if not ea_a.empty else 0,
        "monto_adjudicado_total": float(ea_a["monto"].sum())   if not ea_a.empty else 0.0,
        "total_contratos":        int(ea_k["cantidad"].sum())  if not ea_k.empty else 0,
        "monto_contratos_total":  float(ea_k["monto"].sum())   if not ea_k.empty else 0.0,
        "proveedores_unicos":     n_proveedores,
        "entidades_unicas":       n_entidades,
    }
    return dict(ea_c=ea_c, em_c=em_c, tent=tent, mod=mod, mc=mc,
                ea_a=ea_a, em_a=em_a, tprov=tprov, ma=ma,
                ea_k=ea_k, em_k=em_k, mk=mk, kpis=kpis, ruc_cat=ruc_cat)

d = load()
k = d["kpis"]

# ─── KPIs ─────────────────────────────────────────────────────────────────────
sec("📈 Indicadores Generales 2025")
c1, c2, c3, c4 = st.columns(4)
with c1: kpi("Total Llamados",      k["total_llamados"],         "convocatorias publicadas")
with c2: kpi("Monto Estimado",      k["monto_estimado_total"],   "en Guaraníes")
with c3: kpi("Adjudicaciones",      k["total_adjudicaciones"],   "procesos adjudicados")
with c4: kpi("Monto Adjudicado",    k["monto_adjudicado_total"], "en Guaraníes")

c5, c6, c7, c8 = st.columns(4)
with c5: kpi("Proveedores Únicos",  k["proveedores_unicos"],     "empresas adjudicatarias")
with c6: kpi("Entidades Públicas",  k["entidades_unicas"],       "organismos contratantes")
with c7: kpi("Ítems Adjudicados",   1_033_935,                   "ítems en detalle")
with c8: kpi("Anomalías detectadas",10_769 + 4_739,             "CRÍTICO + Alto en 2025")

st.markdown("<br>", unsafe_allow_html=True)

# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Convocatorias",
    "🏆 Adjudicaciones",
    "🔍 Detalle de Ítems",
    "🚨 Anomalías de Precios",
])

# ══ TAB 1 ═════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown("<br>", unsafe_allow_html=True)
    ea = d["ea_c"]
    sec("Evolución Anual de Llamados")
    ca, cb = st.columns(2)
    with ca:
        if not ea.empty:
            fig = px.bar(ea, x="anio", y="cantidad",
                         title="Cantidad de Llamados por Año",
                         color_discrete_sequence=["#1565c0"],
                         labels={"anio":"Año","cantidad":"Llamados"})
            fig.update_layout(**chart_layout())
            st.plotly_chart(fig, use_container_width=True, key="c_anio_cant")
        else: emptyfig("ef_c1")
    with cb:
        if not ea.empty:
            fig2 = px.area(ea, x="anio", y="monto",
                           title="Monto Estimado por Año (₲)",
                           color_discrete_sequence=["#1565c0"],
                           labels={"anio":"Año","monto":"Monto (₲)"})
            fig2.update_traces(fill="tozeroy", fillcolor="rgba(21,101,192,0.12)",
                               line=dict(color="#1565c0", width=2))
            fig2.update_layout(**chart_layout())
            st.plotly_chart(fig2, use_container_width=True, key="c_anio_monto")
        else: emptyfig("ef_c2")

    cc, cd = st.columns(2)
    with cc:
        sec("Modalidades de Contratación")
        md = d["mod"]
        if not md.empty:
            fig3 = px.pie(md, names="modalidad", values="cantidad", hole=0.45,
                          title="Distribución por Modalidad",
                          color_discrete_sequence=PAL)
            fig3.update_layout(**chart_layout())
            fig3.update_traces(textposition="inside", textinfo="percent+label",
                               textfont=dict(size=11, color="white"))
            st.plotly_chart(fig3, use_container_width=True, key="c_modal")
        else: emptyfig("ef_c3")
    with cd:
        sec("Top 20 Entidades Convocantes")
        te = d["tent"]
        if not te.empty:
            fig4 = px.bar(te.sort_values("cantidad"), x="cantidad", y="entidad",
                          orientation="h",
                          title="Entidades con Más Llamados",
                          color="cantidad", color_continuous_scale="Blues",
                          labels={"cantidad":"Llamados","entidad":""})
            fig4.update_layout(**chart_layout(), height=520)
            fig4.update_coloraxes(showscale=False)
            st.plotly_chart(fig4, use_container_width=True, key="c_entidades")
        else: emptyfig("ef_c4")


    # ── BUSCADOR DE LICITACIONES ─────────────────────────────────────────────
    LICIT_F = CACHE_DIR / "convocatorias" / "licitaciones_full.parquet"
    sec("🔎 Buscador de Licitaciones")

    @st.cache_data(ttl=600, show_spinner="Cargando licitaciones...")
    def load_licitaciones():
        if not LICIT_F.exists(): return pd.DataFrame()
        df = pd.read_parquet(LICIT_F)
        for col in ["titulo","entidad","estado","modalidad_detalle"]:
            if col in df.columns:
                df[col] = df[col].astype(str).replace({"nan":"","None":""}).str.strip()
        if "texto_busqueda" not in df.columns:
            df["texto_busqueda"] = (df.get("titulo","") + " " + df.get("entidad","")).str.lower()
        return df

    licit = load_licitaciones()

    if licit.empty:
        st.info("Datos de licitaciones no disponibles.")
    else:
        la, lb, lc = st.columns([3, 2, 2])
        with la:
            q = st.text_input("🔍 Buscar por título o descripción",
                              placeholder="ej: guardia de seguridad, medicamentos, asfalto...",
                              key="licit_q")
        with lb:
            ents_l = ["(Todas)"] + sorted(licit["entidad"].dropna().unique().tolist())
            sel_ent_l = st.selectbox("🏛️ Entidad", ents_l, key="licit_ent")
        with lc:
            estados_l = ["(Todos)"] + sorted(licit["estado"].dropna().unique().tolist())
            sel_est_l = st.selectbox("📋 Estado", estados_l, key="licit_est")

        # Aplicar filtros
        res = licit.copy()
        if q.strip():
            res = res[res["texto_busqueda"].str.contains(q.strip().lower(), na=False)]
        if sel_ent_l != "(Todas)":
            res = res[res["entidad"] == sel_ent_l]
        if sel_est_l != "(Todos)":
            res = res[res["estado"] == sel_est_l]

        # KPIs resultado
        ra, rb, rc_ = st.columns(3)
        with ra: kpi("Resultados",    len(res),                              "licitaciones")
        with rb: kpi("Monto total",   float(res["monto_estimado"].sum()),    "en ₲")
        with rc_: kpi("Entidades",    res["entidad"].nunique(),              "organismos")

        if len(res) == 0:
            st.info("No se encontraron licitaciones con esos criterios.")
        else:
            cols_show = [c for c in ["id_llamado","titulo","entidad","estado",
                                     "modalidad_detalle","monto_estimado",
                                     "fecha_publicacion","fecha_cierre"] if c in res.columns]
            tabla_l = res[cols_show].head(2000).copy()
            tabla_l.columns = [c.replace("_"," ").replace("modalidad detalle","Modalidad")
                                .title() for c in tabla_l.columns]

            st.dataframe(tabla_l, use_container_width=True, height=380,
                         column_config={
                             "Monto Estimado": st.column_config.NumberColumn(
                                 "Monto Estimado (₲)", format="₲ %,.0f"),
                         })
            csv_l = res[cols_show].to_csv(index=False).encode("utf-8-sig")
        if ruc_global.strip():
            st.info("🔢 Filtro por RUC activo — este tab muestra datos agregados de convocatorias (sin RUC disponible). Revisas los Tabs 🔍 Detalle de Ítems y 🚨 Anomalías para ver resultados filtrados por RUC.")

# ══ TAB 2 — ADJUDICACIONES ═════════════════════════════════════════════════════
with tab2:
    st.markdown("<br>", unsafe_allow_html=True)
    # Aplicar filtro RUC global en top_proveedores
    tp = d["tprov"].copy()
    if ruc_global.strip() and "ruc" in tp.columns:
        tp = tp[tp["ruc"].astype(str).str.contains(ruc_global.strip(), case=False, na=False)]
        if tp.empty:
            st.warning(f"🔢 Sin resultados para RUC **{ruc_global.strip()}** en el top de proveedores. Puede que el proveedor no esté entre los 20 principales.")
    ce, cf = st.columns(2)
    ce, cf = st.columns(2)
    with ce:
        sec("Top 20 Proveedores por Monto Adjudicado")
        if not tp.empty:
            fig5 = px.bar(tp.sort_values("monto"), x="monto", y="proveedor",
                          orientation="h", title="Proveedores con Mayor Monto (₲)",
                          color="monto", color_continuous_scale="Blues",
                          labels={"monto":"Monto (₲)","proveedor":""})
            fig5.update_layout(**chart_layout(), height=520)
            fig5.update_coloraxes(showscale=False)
            st.plotly_chart(fig5, use_container_width=True, key="a_prov_monto")
        else: emptyfig("ef_a1")
    with cf:
        sec("Participación por Cantidad de Adjudicaciones")
        if not tp.empty:
            fig6 = px.pie(tp, names="proveedor", values="cantidad", hole=0.45,
                          title="Distribución por Proveedor",
                          color_discrete_sequence=PAL)
            fig6.update_layout(**chart_layout())
            fig6.update_traces(textposition="inside", textinfo="percent",
                               textfont=dict(size=10, color="white"))
            st.plotly_chart(fig6, use_container_width=True, key="a_prov_pie")
        else: emptyfig("ef_a2")

    sec("Evolución Mensual de Adjudicaciones")
    em = d["em_a"]
    if not em.empty:
        cg, ch = st.columns(2)
        with cg:
            fig7 = px.line(em, x="mes", y="cantidad",
                           title="Adjudicaciones por Mes",
                           color_discrete_sequence=["#1565c0"], markers=True,
                           labels={"mes":"Mes","cantidad":"Cantidad"})
            fig7.update_traces(line=dict(width=2.5), marker=dict(size=7))
            fig7.update_layout(**chart_layout())
            st.plotly_chart(fig7, use_container_width=True, key="a_mes_cant")
        with ch:
            fig8 = px.bar(em, x="mes", y="monto",
                          title="Monto Adjudicado por Mes (₲)",
                          color_discrete_sequence=["#0288d1"],
                          labels={"mes":"Mes","monto":"Monto (₲)"})
            fig8.update_layout(**chart_layout())
            st.plotly_chart(fig8, use_container_width=True, key="a_mes_monto")
    else: emptyfig("ef_a3")

    ma = d["ma"]
    if not ma.empty:
        sec("Muestra de Adjudicaciones")
        st.dataframe(ma, use_container_width=True, height=300)

# ══ TAB 3 — DETALLE DE ÍTEMS ══════════════════════════════════════════════════
with tab3:
    st.markdown("<br>", unsafe_allow_html=True)
    ITEMS_F = CACHE_DIR / "adjudicaciones" / "items_detalle.parquet"

    if not ITEMS_F.exists():
        st.warning("⚠️ No hay datos de ítems disponibles.")
    else:
        @st.cache_data(ttl=600, show_spinner="Cargando detalle de ítems...")
        def load_items():
            df = pd.read_parquet(ITEMS_F)
            for col in ["entidad","proveedor","clasificacion","unidad","ruc"]:
                if col in df.columns:
                    df[col] = df[col].astype(str).replace({"nan":"","None":"","<NA>":""})
            return df

        items = load_items()

        sec("Filtros de Búsqueda")
        fi1, fi2 = st.columns([3, 2])
        with fi1:
            buscar = st.text_input("🔎 Buscar ítem por descripción",
                                   placeholder="ej: amoxicilina, combustible, papel bond...")
        with fi2:
            entidades_disp = ["(Todas)"] + sorted(items["entidad"].dropna().unique().tolist())
            sel_entidad = st.selectbox("🏛️ Entidad compradora", entidades_disp)

        fi3 = st.columns(1)[0]
        with fi3:
            sel_anio = st.selectbox("📅 Año", ["Todos"] + sorted(items["anio"].unique().tolist(), reverse=True))

        if ruc_global.strip():
            st.info(f"🔢 Filtrando por RUC: **{ruc_global.strip()}** (filtro global desde el panel lateral)")

        filtrado = items.copy()
        if buscar.strip():
            mask = filtrado["descripcion"].str.contains(buscar.strip(), case=False, na=False)
            if "clasificacion" in filtrado.columns:
                mask |= filtrado["clasificacion"].str.contains(buscar.strip(), case=False, na=False)
            filtrado = filtrado[mask]
        if sel_entidad != "(Todas)":
            filtrado = filtrado[filtrado["entidad"] == sel_entidad]
        if sel_anio != "Todos":
            filtrado = filtrado[filtrado["anio"] == int(sel_anio)]
        if ruc_global.strip() and "ruc" in filtrado.columns:
            filtrado = filtrado[filtrado["ruc"].str.contains(ruc_global.strip(), case=False, na=False)]

        st.markdown("<br>", unsafe_allow_html=True)
        k1, k2, k3, k4 = st.columns(4)
        with k1: kpi("Ítems encontrados",     len(filtrado),                        "registros")
        with k2: kpi("Monto total",           float(filtrado["monto_item"].sum()),   "en ₲")
        with k3: kpi("Entidades",             filtrado["entidad"].nunique(),         "organismos")
        with k4: kpi("Proveedores",           filtrado["proveedor"].nunique(),       "empresas")

        if len(filtrado) == 0:
            st.info("No se encontraron ítems con los filtros aplicados.")
        else:
            st.markdown("<br>", unsafe_allow_html=True)
            ga, gb = st.columns(2)
            with ga:
                sec("Top 15 Proveedores por Monto")
                top_prov = (filtrado.groupby("proveedor")["monto_item"]
                            .sum().nlargest(15).reset_index()
                            .rename(columns={"monto_item":"monto"}))
                if not top_prov.empty:
                    figp = px.bar(top_prov.sort_values("monto"), x="monto", y="proveedor",
                                  orientation="h", color="monto",
                                  color_continuous_scale="Blues",
                                  labels={"monto":"Monto (₲)","proveedor":""},
                                  title="¿Quién proveyó más?")
                    figp.update_layout(**chart_layout(), height=440)
                    figp.update_coloraxes(showscale=False)
                    st.plotly_chart(figp, use_container_width=True, key="it_prov")
            with gb:
                sec("Top 15 Entidades Compradoras")
                top_ent = (filtrado.groupby("entidad")["monto_item"]
                           .sum().nlargest(15).reset_index()
                           .rename(columns={"monto_item":"monto"}))
                if not top_ent.empty:
                    fige = px.bar(top_ent.sort_values("monto"), x="monto", y="entidad",
                                  orientation="h", color="monto",
                                  color_continuous_scale="Blues",
                                  labels={"monto":"Monto (₲)","entidad":""},
                                  title="¿Quién compró más?")
                    fige.update_layout(**chart_layout(), height=440)
                    fige.update_coloraxes(showscale=False)
                    st.plotly_chart(fige, use_container_width=True, key="it_ent")

            if "clasificacion" in filtrado.columns:
                sec("Distribución por Clasificación / Rubro")
                top_cls = (filtrado.groupby("clasificacion")["monto_item"]
                           .sum().nlargest(10).reset_index()
                           .rename(columns={"monto_item":"monto"}))
                top_cls = top_cls[top_cls["clasificacion"].str.strip() != ""]
                if not top_cls.empty:
                    figc = px.pie(top_cls, names="clasificacion", values="monto",
                                  hole=0.40, color_discrete_sequence=PAL,
                                  title="Monto por Rubro")
                    figc.update_layout(**chart_layout())
                    figc.update_traces(textposition="inside", textinfo="percent+label",
                                       textfont=dict(size=10))
                    st.plotly_chart(figc, use_container_width=True, key="it_cls")

            sec(f"Detalle de Registros — {min(len(filtrado),5000):,} de {len(filtrado):,}")
            cols_show = [c for c in ["ruc","entidad","proveedor","descripcion","clasificacion",
                                     "cantidad","unidad","precio_unitario","monto_item",
                                     "fecha_adjudicacion"] if c in filtrado.columns]
            tabla = filtrado[cols_show].head(5000).copy()
            for col in ["precio_unitario","monto_item"]:
                if col in tabla.columns:
                    tabla[col] = tabla[col].round(0)

            st.dataframe(tabla, use_container_width=True, height=380,
                         column_config={
                             "ruc":             st.column_config.TextColumn("RUC"),
                             "monto_item":      st.column_config.NumberColumn("Monto (₲)", format="₲ %,.0f"),
                             "precio_unitario": st.column_config.NumberColumn("Precio Unit. (₲)", format="₲ %,.0f"),
                             "cantidad":        st.column_config.NumberColumn("Cantidad", format="%.2f"),
                             "fecha_adjudicacion": st.column_config.DateColumn("Fecha"),
                         })
            csv = tabla.to_csv(index=False).encode("utf-8-sig")
            entidad_slug = sel_entidad.replace(" ","_")[:30] if sel_entidad != "(Todas)" else "todas"
            buscar_slug  = (ruc_global.strip() or buscar.strip()).replace(" ","_")[:20] or "items"
            st.download_button("⬇️ Descargar CSV", csv,
                               file_name=f"dncp_{entidad_slug}_{buscar_slug}.csv",
                               mime="text/csv", key="dl_items")

# ══ TAB 4 — ANOMALÍAS DE PRECIOS ═════════════════════════════════════════════
with tab4:
    st.markdown("<br>", unsafe_allow_html=True)
    COMP_F = CACHE_DIR / "adjudicaciones" / "comparacion_precios.parquet"

    if not COMP_F.exists():
        st.warning("⚠️ No hay datos de comparación disponibles.")
    else:
        @st.cache_data(ttl=600, show_spinner="Cargando análisis de precios...")
        def load_comp():
            df = pd.read_parquet(COMP_F)
            for col in ["entidad","nombre_catalogo","proveedor_mas_frecuente","nivel_alerta","unidad"]:
                if col in df.columns:
                    df[col] = df[col].astype(str).replace({"nan":"","None":""})
            # Agregar RUC del proveedor más frecuente
            ruc_cat = d.get("ruc_cat", pd.DataFrame())
            if not ruc_cat.empty and "ruc" not in df.columns:
                df = df.merge(
                    ruc_cat[["proveedor","ruc"]].rename(columns={"proveedor":"proveedor_mas_frecuente","ruc":"ruc_proveedor"}),
                    on="proveedor_mas_frecuente", how="left"
                )
                df["ruc_proveedor"] = df["ruc_proveedor"].fillna("").astype(str).replace("nan","")
            elif "ruc" in df.columns:
                df.rename(columns={"ruc":"ruc_proveedor"}, inplace=True)
            else:
                df["ruc_proveedor"] = ""
            return df

        comp = load_comp()

        # KPIs de alerta
        criticos  = comp[comp["nivel_alerta"] == "🚨 CRÍTICO"]
        altos     = comp[comp["nivel_alerta"] == "⚠️ Alto"]

        # Banner explicativo
        st.markdown("""
<div style="background:#fff8e1;border-left:5px solid #f9a825;border-radius:10px;padding:16px 20px;margin-bottom:20px">
<b style="color:#e65100;font-size:14px">ℹ️ ¿Cómo funciona la detección de anomalías?</b><br>
<span style="color:#4e342e;font-size:13px">
Se compara el precio promedio que cada entidad pagó por un artículo (según catálogo oficial de contrataciones públicas)
contra el <b>precio mediano de todas las instituciones</b> que compraron el mismo artículo.
Los datos son 100% reales — provienen de los datos abiertos de <a href="https://contrataciones.gov.py/datos" target="_blank" style="color:#bf360c">contrataciones.gov.py</a>.
</span>
</div>
""", unsafe_allow_html=True)

        ka, kb, kc, kd = st.columns(4)
        with ka: kpi("Artículos comparados",  comp["codigo_catalogo"].nunique(), "con 2+ entidades", "")
        with kb: kpi("🚨 Alertas críticas",   len(criticos),  "precio >2× la mediana", "danger")
        with kc: kpi("⚠️ Alertas altas",      len(altos),     "precio >1.5× la mediana", "warn")
        with kd: kpi("Entidades analizadas",  comp["entidad"].nunique(), "organismos públicos", "")

        st.markdown("<br>", unsafe_allow_html=True)

        # Filtros
        sec("Filtros de Búsqueda")
        fa, fb = st.columns([3, 2])
        with fa:
            buscar_art = st.text_input("🔎 Buscar artículo",
                                       placeholder="ej: amoxicilina, papel A4, gasoil...",
                                       key="comp_buscar")
        with fb:
            alertas_opciones = ["Todas", "🚨 CRÍTICO", "⚠️ Alto", "🟡 Moderado", "✅ Normal"]
            sel_alerta = st.selectbox("🚦 Nivel de alerta", alertas_opciones, key="comp_alerta")

        fc = st.columns(1)[0]
        with fc:
            entidades_comp = ["(Todas)"] + sorted(comp["entidad"].dropna().unique().tolist())
            sel_ent_comp   = st.selectbox("🏛️ Entidad", entidades_comp, key="comp_entidad")

        if ruc_global.strip():
            st.info(f"🔢 Filtrando por RUC: **{ruc_global.strip()}** (filtro global desde el panel lateral)")

        filtrado_c = comp.copy()
        if buscar_art.strip():
            filtrado_c = filtrado_c[
                filtrado_c["nombre_catalogo"].str.contains(buscar_art.strip(), case=False, na=False) |
                filtrado_c["codigo_catalogo"].str.contains(buscar_art.strip(), case=False, na=False)
            ]
        if sel_alerta != "Todas":
            filtrado_c = filtrado_c[filtrado_c["nivel_alerta"] == sel_alerta]
        if sel_ent_comp != "(Todas)":
            filtrado_c = filtrado_c[filtrado_c["entidad"] == sel_ent_comp]
        if ruc_global.strip() and "ruc_proveedor" in filtrado_c.columns:
            filtrado_c = filtrado_c[filtrado_c["ruc_proveedor"].str.contains(ruc_global.strip(), case=False, na=False)]

        st.caption(f"Mostrando {len(filtrado_c):,} registros")

        if len(filtrado_c) > 0:
            # Boxplot cuando la búsqueda es específica
            if buscar_art.strip() and filtrado_c["codigo_catalogo"].nunique() <= 10:
                for codigo in filtrado_c["codigo_catalogo"].unique()[:3]:
                    sub = comp[comp["codigo_catalogo"] == codigo].copy()
                    nombre     = sub["nombre_catalogo"].iloc[0]
                    unidad     = sub["unidad"].iloc[0] if "unidad" in sub.columns else ""
                    mediana_gl = sub["precio_mediano"].iloc[0]

                    sec(f"Comparación: {nombre}")
                    col_box, col_rank = st.columns([3, 2])

                    with col_box:
                        sub_s = sub.sort_values("precio_promedio_ent", ascending=False)
                        ALERT_COLORS = {
                            "🚨 CRÍTICO": "#ef5350",
                            "⚠️ Alto":    "#ffa726",
                            "🟡 Moderado":"#ffd54f",
                            "✅ Normal":  "#66bb6a",
                            "🔵 Muy bajo":"#42a5f5",
                        }
                        colores = [ALERT_COLORS.get(al, "#90a4ae") for al in sub_s["nivel_alerta"]]

                        fig_box = go.Figure()
                        fig_box.add_trace(go.Bar(
                            x=sub_s["precio_promedio_ent"],
                            y=sub_s["entidad"],
                            orientation="h",
                            marker_color=colores,
                            text=[f"₲ {v:,.0f}" for v in sub_s["precio_promedio_ent"]],
                            textposition="outside",
                            name="Precio promedio"
                        ))
                        fig_box.add_vline(x=mediana_gl, line_dash="dot",
                                          line_color="#1565c0", line_width=2.5,
                                          annotation_text=f"  Mediana: ₲{mediana_gl:,.0f}",
                                          annotation_position="top right",
                                          annotation_font=dict(color="#1565c0", size=12))
                        fig_box.update_layout(
                            **chart_layout(),
                            height=max(320, len(sub)*38),
                            title=f"Precio por entidad ({unidad}) — mediana de mercado en línea azul",
                            xaxis_title="Precio promedio unitario (₲)"
                        )
                        st.plotly_chart(fig_box, use_container_width=True, key=f"box_{codigo}")

                    with col_rank:
                        st.markdown("**Detalle por entidad:**")
                        tabla_art = sub_s[[
                            "nivel_alerta","entidad","precio_promedio_ent",
                            "sobreprecio_pct","cantidad_compras","proveedor_mas_frecuente"
                        ]].rename(columns={
                            "nivel_alerta":"Alerta","entidad":"Entidad",
                            "precio_promedio_ent":"Precio Prom (₲)",
                            "sobreprecio_pct":"Sobreprecio %",
                            "cantidad_compras":"Compras",
                            "proveedor_mas_frecuente":"Proveedor"
                        })
                        st.dataframe(tabla_art, use_container_width=True, height=360,
                                     column_config={
                                         "Precio Prom (₲)": st.column_config.NumberColumn(format="₲ %,.0f"),
                                         "Sobreprecio %":   st.column_config.NumberColumn(format="%+.1f%%"),
                                     })

            # Tabla general
            sec(f"Ranking de Anomalías — {min(len(filtrado_c),3000):,} de {len(filtrado_c):,} registros")

            _cols_anom = [c for c in [
                "nivel_alerta","nombre_catalogo","entidad",
                "precio_promedio_ent","precio_mediano",
                "indice_anomalia","sobreprecio_pct",
                "cantidad_compras","proveedor_mas_frecuente","ruc_proveedor","codigo_catalogo"
            ] if c in filtrado_c.columns]
            _rename_anom = {
                "nivel_alerta":"Alerta","nombre_catalogo":"Artículo","entidad":"Entidad",
                "precio_promedio_ent":"Precio Prom (₲)","precio_mediano":"Mediana (₲)",
                "indice_anomalia":"Índice","sobreprecio_pct":"Sobreprecio %",
                "cantidad_compras":"Compras","proveedor_mas_frecuente":"Proveedor",
                "ruc_proveedor":"RUC Proveedor","codigo_catalogo":"Código",
            }
            tabla_c = filtrado_c.sort_values("indice_anomalia", ascending=False).head(3000)[_cols_anom].rename(columns=_rename_anom)

            st.dataframe(tabla_c, use_container_width=True, height=440,
                         column_config={
                             "Precio Prom (₲)": st.column_config.NumberColumn(format="₲ %,.0f"),
                             "Mediana (₲)":     st.column_config.NumberColumn(format="₲ %,.0f"),
                             "Índice":          st.column_config.NumberColumn(format="%.2f×"),
                             "Sobreprecio %":   st.column_config.NumberColumn(format="%+.1f%%"),
                         })

            csv_c = tabla_c.to_csv(index=False).encode("utf-8-sig")
            st.download_button("⬇️ Descargar CSV de anomalías", csv_c,
                               file_name="dncp_anomalias_precios.csv",
                               mime="text/csv", key="dl_anomalias")

            with st.expander("📖 Metodología y limitaciones"):
                st.markdown("""
**Fuente de datos:** Datos abiertos DNCP (contrataciones.gov.py) — 100% reales, año 2025.

**¿Qué es la "mediana de mercado"?**
Es el precio mediano que el conjunto de instituciones públicas pagó por el mismo artículo
(mismo código de catálogo oficial). No es un precio de mercado privado externo,
sino el precio de referencia interno del Estado paraguayo.

| Nivel | Criterio | Interpretación |
|---|---|---|
| 🚨 CRÍTICO | Índice > 2.0× | Paga más del doble que la mediana |
| ⚠️ Alto    | Índice > 1.5× | Paga 50%+ sobre la mediana |
| 🟡 Moderado | Índice > 1.2× | Paga 20%+ sobre la mediana |
| ✅ Normal  | Índice ≤ 1.2× | Dentro del rango normal |
| 🔵 Muy bajo | Índice < 0.5× | Posible error o unidad diferente |

**Limitaciones:** Un mismo artículo con distintas presentaciones o calidades puede aparecer
como "anómalo" sin serlo. Se recomienda revisar la unidad de medida y la descripción detallada.
Solo se muestran artículos comprados por **al menos 2 entidades distintas**.
                """)


# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown("""
<div style="background:#1a237e;border-radius:12px;padding:16px 24px;text-align:center">
  <span style="color:rgba(255,255,255,0.65);font-size:12px">
  Datos: <a href="https://contrataciones.gov.py/datos" style="color:#90caf9">DNCP Paraguay</a> ·
  Licencia <a href="https://creativecommons.org/licenses/by/4.0/" style="color:#90caf9">CC BY 4.0</a> ·
  <a href="https://github.com/diegomezapy/tableroDNCPpy" style="color:#90caf9">Código fuente</a>
  </span>
</div>
""", unsafe_allow_html=True)
