"""
dashboard.py — Tablero de Contrataciones Públicas DNCP Paraguay
================================================================
Lee ÚNICAMENTE los Parquet pre-agregados (pocos MB) generados por
processor.py. Nunca toca los CSV crudos en tiempo de ejecución.

Flujo recomendado:
  1. python downloader.py --years 2025
  2. python processor.py  --years 2025
  3. streamlit run dashboard.py
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

from processor import (
    kpis_generales,
    get_evolucion_anual_conv,
    get_evolucion_mensual_conv,
    get_top_entidades,
    get_modalidades,
    get_muestra_conv,
    get_top_proveedores,
    get_evolucion_mensual_adj,
    get_muestra_adj,
    get_evolucion_anual_cont,
    get_evolucion_mensual_cont,
    get_muestra_cont,
    process_all,
    CACHE_DIR,
    DATA_DIR,
)
from downloader import download_all, MODULES


# ─────────────────────────────────────────────────────────────────────────────
# Página
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DNCP Paraguay — Tablero de Contrataciones",
    page_icon="🇵🇾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.kpi-card {
    background: linear-gradient(135deg,#1e2130,#252840);
    border:1px solid #2e3250; border-radius:12px;
    padding:18px 22px; margin-bottom:8px; position:relative; overflow:hidden;
}
.kpi-card::before {
    content:''; position:absolute; top:0; left:0; right:0; height:3px;
    background:linear-gradient(90deg,#4f6ef7,#a78bfa); border-radius:12px 12px 0 0;
}
.kpi-label { font-size:11px; font-weight:600; color:#8892b0; letter-spacing:.08em; text-transform:uppercase; margin-bottom:5px; }
.kpi-value { font-size:24px; font-weight:700; color:#e2e8f0; }
.kpi-sub   { font-size:11px; color:#4ade80; margin-top:3px; }
.section-title {
    font-size:17px; font-weight:600; color:#a5b4fc;
    border-left:4px solid #4f6ef7; padding-left:12px; margin:22px 0 10px 0;
}
</style>
""", unsafe_allow_html=True)

BG, GRID, TEXT = "#0f1117", "#1e2130", "#e2e8f0"
PALETTE = px.colors.qualitative.Bold
BASE_LAYOUT = dict(
    paper_bgcolor=BG, plot_bgcolor=GRID,
    font=dict(family="Inter", color=TEXT, size=12),
    margin=dict(l=20, r=20, t=40, b=20),
    xaxis=dict(gridcolor="#2e3250", linecolor="#2e3250"),
    yaxis=dict(gridcolor="#2e3250", linecolor="#2e3250"),
)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def fmt_gs(v):
    if not v: return "₲ 0"
    if v >= 1e12: return f"₲ {v/1e12:.1f} B"
    if v >= 1e9:  return f"₲ {v/1e9:.1f} MM"
    if v >= 1e6:  return f"₲ {v/1e6:.1f} M"
    return f"₲ {v:,.0f}"

def kpi(label, value, sub=""):
    val = fmt_gs(value) if isinstance(value, float) and value > 9999 else f"{value:,}" if isinstance(value, (int,float)) else str(value)
    st.markdown(f"""<div class="kpi-card"><div class="kpi-label">{label}</div>
    <div class="kpi-value">{val}</div>
    {"<div class='kpi-sub'>"+sub+"</div>" if sub else ""}</div>""", unsafe_allow_html=True)

def section(t): st.markdown(f'<div class="section-title">{t}</div>', unsafe_allow_html=True)

def empty_fig(msg="Sin datos"):
    fig = go.Figure()
    fig.add_annotation(text=msg, x=.5, y=.5, showarrow=False,
                       font=dict(size=15, color="#8892b0"), xref="paper", yref="paper")
    fig.update_layout(**BASE_LAYOUT, height=300)
    return fig

# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🇵🇾 DNCP Paraguay")
    st.caption("Datos Abiertos · CC BY 4.0")
    st.divider()

    # ── 1. Descargar datos ──────────────────────────────────────────────────
    st.markdown("### ⬇️ Descargar datos")
    all_years = list(range(2010, 2027))
    dl_years  = st.multiselect("Años a descargar", all_years, default=[2024, 2025])
    dl_mods   = st.multiselect("Módulos", list(MODULES.keys()), default=list(MODULES.keys()))
    dl_force  = st.checkbox("Forzar re-descarga")

    if st.button("🚀 Descargar", use_container_width=True, type="primary"):
        if not dl_years:
            st.error("Seleccioná al menos un año.")
        else:
            with st.spinner("Descargando..."):
                download_all(dl_years, dl_mods, force=dl_force)
            st.success("✅ Descarga finalizada")

    st.divider()

    # ── 2. Procesar (genera cache Parquet) ──────────────────────────────────
    st.markdown("### ⚙️ Procesar datos")
    proc_force = st.checkbox("Re-procesar cache")
    if st.button("🔄 Procesar → Cache", use_container_width=True):
        avail = sorted([int(p.name) for p in DATA_DIR.iterdir() if p.is_dir() and p.name.isdigit()]) if DATA_DIR.exists() else []
        if not avail:
            st.error("Primero descargá datos.")
        else:
            with st.spinner(f"Procesando años {avail} en chunks..."):
                process_all(years=avail, force=proc_force)
            st.success("✅ Cache listo. Recargando...")
            st.cache_data.clear()
            st.rerun()

    # Mostrar estado del cache
    cache_ok = CACHE_DIR.exists() and any(CACHE_DIR.rglob("*.parquet"))
    if cache_ok:
        total_kb = sum(p.stat().st_size for p in CACHE_DIR.rglob("*.parquet")) // 1024
        st.success(f"Cache listo · {total_kb:,} KB")
    else:
        st.warning("Sin cache — procesá primero")

    st.divider()
    st.markdown("[Datos DNCP](https://contrataciones.gov.py/datos) · Actualización horaria")

# ─────────────────────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("# 📊 Tablero de Contrataciones Públicas")
st.caption("Dirección Nacional de Contrataciones Públicas — Paraguay · OCDS V3")
st.divider()

if not cache_ok:
    st.info("👈 Usá el panel lateral para descargar y procesar los datos primero.")
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# Cargar datos desde cache (solo Parquet, mínima RAM)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner=False)
def load_cache():
    return {
        "kpis":         kpis_generales(),
        "ev_anual_c":   get_evolucion_anual_conv(),
        "ev_mes_c":     get_evolucion_mensual_conv(),
        "top_ent":      get_top_entidades(),
        "modal":        get_modalidades(),
        "muestra_c":    get_muestra_conv(),
        "top_prov":     get_top_proveedores(),
        "ev_mes_a":     get_evolucion_mensual_adj(),
        "muestra_a":    get_muestra_adj(),
        "ev_anual_k":   get_evolucion_anual_cont(),
        "ev_mes_k":     get_evolucion_mensual_cont(),
        "muestra_k":    get_muestra_cont(),
    }

data = load_cache()
kpis = data["kpis"]

# ─────────────────────────────────────────────────────────────────────────────
# KPIs
# ─────────────────────────────────────────────────────────────────────────────
section("📈 Indicadores Generales")
c1, c2, c3, c4 = st.columns(4)
with c1: kpi("Total Llamados",    kpis["total_llamados"],       "convocatorias")
with c2: kpi("Monto Estimado",    kpis["monto_estimado_total"], "en ₲")
with c3: kpi("Adjudicaciones",    kpis["total_adjudicaciones"], "procesos adjudicados")
with c4: kpi("Monto Adjudicado",  kpis["monto_adjudicado_total"], "en ₲")

c5, c6, c7, c8 = st.columns(4)
with c5: kpi("Contratos",         kpis["total_contratos"],      "firmados")
with c6: kpi("Monto Contratos",   kpis["monto_contratos_total"],"en ₲")
with c7: kpi("Proveedores Únicos",kpis["proveedores_unicos"],   "empresas adjudicatarias")
with c8: kpi("Entidades Públicas",kpis["entidades_unicas"],     "organismos contratantes")

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📋 Convocatorias", "🏆 Adjudicaciones", "📄 Contratos"])

# ══════════════ TAB 1 ══════════════════════════════════════════════════════
with tab1:
    ea = data["ev_anual_c"]
    section("Evolución Anual")
    col_a, col_b = st.columns(2)
    with col_a:
        if not ea.empty:
            fig = px.bar(ea, x="anio", y="cantidad", title="Llamados por Año",
                         color="cantidad", color_continuous_scale="Blues",
                         labels={"anio":"Año","cantidad":"Llamados"})
            fig.update_layout(**BASE_LAYOUT); fig.update_coloraxes(showscale=False)
            st.plotly_chart(fig, use_container_width=True)
        else: st.plotly_chart(empty_fig(), use_container_width=True, key="ef_conv_anual_a")
    with col_b:
        if not ea.empty:
            fig2 = px.area(ea, x="anio", y="monto", title="Monto Estimado por Año (₲)",
                           color_discrete_sequence=["#4f6ef7"],
                           labels={"anio":"Año","monto":"Monto (₲)"})
            fig2.update_traces(fill="tozeroy", fillcolor="rgba(79,110,247,0.2)")
            fig2.update_layout(**BASE_LAYOUT)
            st.plotly_chart(fig2, use_container_width=True)
        else: st.plotly_chart(empty_fig(), use_container_width=True, key="ef_conv_anual_b")

    col_c, col_d = st.columns(2)
    with col_c:
        section("Modalidades de Contratación")
        md = data["modal"]
        if not md.empty:
            fig3 = px.pie(md, names="modalidad", values="cantidad", hole=0.4,
                          title="Distribución por Modalidad", color_discrete_sequence=PALETTE)
            fig3.update_layout(**BASE_LAYOUT)
            fig3.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig3, use_container_width=True)
        else: st.plotly_chart(empty_fig(), use_container_width=True, key="ef_conv_modal")

    with col_d:
        section("Top 20 Entidades Convocantes")
        te = data["top_ent"]
        if not te.empty:
            fig4 = px.bar(te.sort_values("cantidad"), x="cantidad", y="entidad",
                          orientation="h", title="Top Entidades por N° de Llamados",
                          color="cantidad", color_continuous_scale="Viridis",
                          labels={"cantidad":"Llamados","entidad":""})
            fig4.update_layout(**BASE_LAYOUT, height=500)
            fig4.update_coloraxes(showscale=False)
            st.plotly_chart(fig4, use_container_width=True)
        else: st.plotly_chart(empty_fig(), use_container_width=True, key="ef_conv_ent")

    mc = data["muestra_c"]
    if not mc.empty:
        section("📋 Muestra de Convocatorias (primeras 500 filas)")
        st.dataframe(mc, use_container_width=True, height=320,
                     column_config={"monto_estimado": st.column_config.NumberColumn("Monto (₲)", format="₲ %,.0f"),
                                    "fecha_publicacion": st.column_config.DateColumn("Fecha")})

# ══════════════ TAB 2 ══════════════════════════════════════════════════════
with tab2:
    tp = data["top_prov"]
    col_e, col_f = st.columns(2)
    with col_e:
        section("Top 20 Proveedores por Monto Adjudicado")
        if not tp.empty:
            fig5 = px.bar(tp.sort_values("monto"), x="monto", y="proveedor",
                          orientation="h", title="Top Proveedores (₲)",
                          color="monto", color_continuous_scale="Plasma",
                          labels={"monto":"Monto (₲)","proveedor":""})
            fig5.update_layout(**BASE_LAYOUT, height=500)
            fig5.update_coloraxes(showscale=False)
            st.plotly_chart(fig5, use_container_width=True)
        else: st.plotly_chart(empty_fig(), use_container_width=True, key="ef_adj_prov")

    with col_f:
        section("Participación por Cantidad")
        if not tp.empty:
            fig6 = px.pie(tp, names="proveedor", values="cantidad", hole=0.4,
                          title="Adjudicaciones por Proveedor", color_discrete_sequence=PALETTE)
            fig6.update_layout(**BASE_LAYOUT)
            fig6.update_traces(textposition="inside", textinfo="percent")
            st.plotly_chart(fig6, use_container_width=True)
        else: st.plotly_chart(empty_fig(), use_container_width=True, key="ef_adj_pie")

    section("Evolución Mensual")
    em = data["ev_mes_a"]
    if not em.empty:
        ca, cb = st.columns(2)
        with ca:
            fig7 = px.line(em, x="mes", y="cantidad", title="Adjudicaciones por Mes",
                           color_discrete_sequence=["#a78bfa"], markers=True,
                           labels={"mes":"Mes","cantidad":"Cantidad"})
            fig7.update_layout(**BASE_LAYOUT)
            st.plotly_chart(fig7, use_container_width=True)
        with cb:
            fig8 = px.bar(em, x="mes", y="monto", title="Monto Adjudicado por Mes (₲)",
                          color="monto", color_continuous_scale="Sunset",
                          labels={"mes":"Mes","monto":"Monto (₲)"})
            fig8.update_layout(**BASE_LAYOUT); fig8.update_coloraxes(showscale=False)
            st.plotly_chart(fig8, use_container_width=True)
    else: st.plotly_chart(empty_fig(), use_container_width=True, key="ef_adj_mes")

    ma = data["muestra_a"]
    if not ma.empty:
        section("📋 Muestra de Adjudicaciones")
        st.dataframe(ma, use_container_width=True, height=320,
                     column_config={"monto_adjudicado": st.column_config.NumberColumn("Monto (₲)", format="₲ %,.0f"),
                                    "fecha_adjudicacion": st.column_config.DateColumn("Fecha")})

# ══════════════ TAB 3 ══════════════════════════════════════════════════════
with tab3:
    ek = data["ev_anual_k"]
    col_g, col_h = st.columns(2)
    with col_g:
        section("Contratos por Año")
        if not ek.empty:
            fig9 = px.bar(ek, x="anio", y="cantidad", title="N° Contratos por Año",
                          color="cantidad", color_continuous_scale="Teal",
                          labels={"anio":"Año","cantidad":"Contratos"})
            fig9.update_layout(**BASE_LAYOUT); fig9.update_coloraxes(showscale=False)
            st.plotly_chart(fig9, use_container_width=True)
        else: st.plotly_chart(empty_fig(), use_container_width=True, key="ef_cont_anual")

    with col_h:
        section("Monto por Año")
        if not ek.empty:
            fig10 = px.area(ek, x="anio", y="monto", title="Monto Total Contratos (₲)",
                            color_discrete_sequence=["#34d399"],
                            labels={"anio":"Año","monto":"Monto (₲)"})
            fig10.update_traces(fill="tozeroy", fillcolor="rgba(52,211,153,0.2)")
            fig10.update_layout(**BASE_LAYOUT)
            st.plotly_chart(fig10, use_container_width=True)
        else: st.plotly_chart(empty_fig(), use_container_width=True, key="ef_cont_monto")

    section("Evolución Mensual de Contratos")
    emk = data["ev_mes_k"]
    if not emk.empty:
        fig11 = px.line(emk, x="mes", y="monto", title="Monto Mensual Contratos (₲)",
                        color_discrete_sequence=["#34d399"], markers=True,
                        labels={"mes":"Mes","monto":"Monto (₲)"})
        fig11.update_layout(**BASE_LAYOUT)
        st.plotly_chart(fig11, use_container_width=True)
    else: st.plotly_chart(empty_fig(), use_container_width=True, key="ef_cont_mes")

    mk = data["muestra_k"]
    if not mk.empty:
        section("📋 Muestra de Contratos")
        st.dataframe(mk, use_container_width=True, height=320,
                     column_config={"monto_contrato": st.column_config.NumberColumn("Monto (₲)", format="₲ %,.0f"),
                                    "fecha_firma": st.column_config.DateColumn("Fecha Firma")})

# ─────────────────────────────────────────────────────────────────────────────
st.divider()
st.markdown("""<div style='text-align:center;color:#4a5568;font-size:12px;padding:12px 0'>
Datos: <a href='https://contrataciones.gov.py/datos' style='color:#4f6ef7'>DNCP Paraguay</a> ·
Licencia <a href='https://creativecommons.org/licenses/by/4.0/' style='color:#4f6ef7'>CC BY 4.0</a>
</div>""", unsafe_allow_html=True)
