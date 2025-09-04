# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# Painel de Produção por Vistoriador (Streamlit) - múltiplas planilhas/mês
#  - Lê 1+ planilhas do Google Sheets (IDs/URLs, separadas por linha)
#  - Junta dados dos meses e preserva metas por mês (MESREF)
#  - Mantém layout e cálculos do seu app original
# ------------------------------------------------------------

import os, re, json
from datetime import datetime, date

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =============== Config & título ===============
st.set_page_config(page_title="🧰 Produção por Vistoriador - Starcheck (Multi-meses)", layout="wide")
st.title("🧰 Painel de Produção por Vistoriador - Starcheck")

st.markdown("""
<style>
  .notranslate { unicode-bidi: plaintext; }
  .hero { background-color:#f0f2f6; padding:15px; border-radius:12px; margin-bottom:18px; box-shadow:0 1px 3px rgba(0,0,0,.10); }
  .card-container { display:flex; gap:18px; margin:12px 0 22px; flex-wrap:wrap; }
  .card { background:#f5f5f5; padding:18px 20px; border-radius:12px; box-shadow:0 2px 6px rgba(0,0,0,.10); text-align:center; min-width:200px; flex:1; }
  .card h4 { color:#cc3300; margin:0 0 8px; font-size:16px; font-weight:700; }
  .card h2 { margin:0; font-size:26px; font-weight:800; color:#222; }
  .section-title { font-size:20px; font-weight:800; margin:22px 0 8px; }
</style>
""", unsafe_allow_html=True)

def _nt(txt: str) -> str:
    return f"<span class='notranslate' translate='no'>{txt}</span>"

st.markdown("""
<div class="hero">
  <h4 style="color:#cc3300; margin:0;">📌 Regras do Painel</h4>
  <ul style="margin:6px 0 0 18px;">
    <li><b>Vistoriador</b> = Perito (se vazio, usa Digitador).</li>
    <li><b>Revistoria</b> = 2ª ocorrência em diante do mesmo <b>CHASSI</b> (ordenado pela Data).</li>
    <li><b>Líquido</b> = Vistorias − Revistorias.</li>
    <li>Preço é ignorado.</li>
  </ul>
</div>
""", unsafe_allow_html=True)

# =============== Conexão Google Sheets ===============
SERVICE_EMAIL = None

def _load_sa_info():
    try:
        block = st.secrets["gcp_service_account"]
    except Exception as e:
        st.error("Não encontrei [gcp_service_account] no .streamlit/secrets.toml.")
        st.stop()
    if "json_path" in block:  # opcional: quando o segredo é um caminho de arquivo
        path = block["json_path"]
        if not os.path.isabs(path):
            path = os.path.join(os.path.dirname(__file__), path)
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f), f"file:{path}"
    return dict(block), "dict"

def _gs_client():
    global SERVICE_EMAIL
    info, _ = _load_sa_info()
    SERVICE_EMAIL = info.get("client_email", "(sem client_email)")
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scopes)
    return gspread.authorize(creds)

# util: aceita URL ou ID
def _extract_sheet_id(s: str) -> str:
    s = s.strip()
    if s == "": return ""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", s)
    return m.group(1) if m else s

# =============== Lê uma planilha (dados + metas) ===============
def parse_date_any(x):
    if pd.isna(x) or str(x).strip() == "": return pd.NaT
    s = str(x).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    try:
        return pd.to_datetime(s, errors="coerce").date()
    except:
        return pd.NaT

def _upper_strip(x): 
    return str(x).upper().strip() if pd.notna(x) else ""

def read_one_spreadsheet(gs, sheet_id: str):
    sh = gs.open_by_key(sheet_id)
    ws = sh.sheet1
    df = pd.DataFrame(ws.get_all_records())  # aba principal (Página1)
    # tenta ler aba METAS
    try:
        metas_ws = sh.worksheet("METAS")
        dfm = pd.DataFrame(metas_ws.get_all_records())
    except Exception:
        dfm = pd.DataFrame()

    # padroniza colunas principais
    if not df.empty:
        df.columns = [c.strip().upper() for c in df.columns]
        # nomes mínimos
        cu = "UNIDADE" if "UNIDADE" in df.columns else None
        cd = "DATA" if "DATA" in df.columns else None
        cc = "CHASSI" if "CHASSI" in df.columns else None
        cp = "PERITO" if "PERITO" in df.columns else None
        cg = "DIGITADOR" if "DIGITADOR" in df.columns else None
        need = [cu, cd, cc, (cp or cg)]
        if any(x is None for x in need):
            raise RuntimeError("Planilha sem colunas mínimas: UNIDADE, DATA, CHASSI, PERITO/DIGITADOR")

        df[cu] = df[cu].map(_upper_strip)
        df[cc] = df[cc].map(_upper_strip)
        df["__DATA__"] = df[cd].apply(parse_date_any)

        # vistoriador
        if cp and cg:
            df["VISTORIADOR"] = np.where(df[cp].astype(str).str.strip()!="",
                                         df[cp].map(_upper_strip),
                                         df[cg].map(_upper_strip))
        elif cp:
            df["VISTORIADOR"] = df[cp].map(_upper_strip)
        else:
            df["VISTORIADOR"] = df[cg].map(_upper_strip)

        # ordena por data/chassi e marca revistoria
        df = df.sort_values(["__DATA__", cc], kind="mergesort").reset_index(drop=True)
        df["__ORD__"] = df.groupby(cc).cumcount()
        df["IS_REV"] = (df["__ORD__"] >= 1).astype(int)

        # remove unidades inválidas
        ban = {"POSTO CÓDIGO", "POSTO CODIGO", "CÓDIGO", "CODIGO", "", "—", "NAN"}
        df = df[~df[cu].isin(ban)].copy()

        # MESREF (MM/AAAA) derivado da __DATA__
        def mk_mesref(d):
            if isinstance(d, date):
                return f"{d.month:02d}/{d.year}"
            return ""
        df["MESREF"] = df["__DATA__"].apply(mk_mesref)

    # padroniza METAS
    if not dfm.empty:
        dfm.columns = [c.strip().upper() for c in dfm.columns]
        # normaliza nomes flexíveis
        ren = {}
        for cand in ["META_MENSAL", "META MEN SAL", "META_MEN SAL", "META_MEN.SAL", "META MENSA"]:
            if cand in dfm.columns: ren[cand] = "META_MENSAL"
        for cand in ["DIAS UTEIS", "DIAS ÚTEIS", "DIAS_UTEIS"]:
            if cand in dfm.columns: ren[cand] = "DIAS_UTEIS"
        dfm = dfm.rename(columns=ren)

        for col in ["VISTORIADOR","UNIDADE","TIPO"]:
            if col in dfm.columns:
                dfm[col] = dfm[col].astype(str).map(_upper_strip)
        dfm["META_MENSAL"] = pd.to_numeric(dfm.get("META_MENSAL", 0), errors="coerce").fillna(0).astype(int)
        dfm["DIAS_UTEIS"]  = pd.to_numeric(dfm.get("DIAS_UTEIS", 0), errors="coerce").fillna(0).astype(int)

        # MESREF da meta:
        # 1) se existir colunas MES/ANO na aba METAS (opcional)
        # 2) senão, usamos o MESREF mais frequente na própria planilha de dados
        if "MESREF" not in dfm.columns:
            if "MES" in dfm.columns and "ANO" in dfm.columns:
                def build_mesref(row):
                    try:
                        m = int(row["MES"]); a = int(row["ANO"])
                        return f"{m:02d}/{a}"
                    except: return ""
                dfm["MESREF"] = dfm.apply(build_mesref, axis=1)
            else:
                fallback = ""
                if not df.empty and "MESREF" in df.columns:
                    freq = df["MESREF"].value_counts()
                    fallback = freq.idxmax() if len(freq) else ""
                dfm["MESREF"] = fallback

    # título (útil só para debug/rodapé)
    title = sh.title

    return df, dfm, title

# =============== Entrada dos arquivos (IDs/URLs) ===============
st.subheader("Conexão com a Base — Arquivos (meses)")
st.caption("Cole 1 ID/URL por linha (pode colar as duas URLs dos meses 08 e 09).")

default_urls = (
    "https://docs.google.com/spreadsheets/d/14Bm5H9C20LqABklE3FniGjKM4-angZQ2fPRV7Uqm0GI/edit?gid=0\n"
    "https://docs.google.com/spreadsheets/d/1jmAuTM-4sGOlUChEvR5prX-7POqucnrcW77cKZ0YRVY/edit?gid=0"
)
urls = st.text_area("Planilhas (uma por linha):", value=default_urls, height=80)

btn = st.button("🔗 Juntar todos os arquivos", use_container_width=True)
if not btn:
    st.stop()

# =============== Carrega tudo ===============
try:
    gs = _gs_client()
except Exception as e:
    st.error("Falha ao autenticar no Google. Compartilhe as planilhas com: **{}**".format(SERVICE_EMAIL or "(conta de serviço)"))
    st.stop()

sheet_ids = [ _extract_sheet_id(x) for x in urls.splitlines() if _extract_sheet_id(x) ]
if not sheet_ids:
    st.error("Informe pelo menos 1 ID/URL de planilha.")
    st.stop()

dfs = []
dfm_all = []
titles = []
for sid in sheet_ids:
    try:
        dfi, dfmi, ttl = read_one_spreadsheet(gs, sid)
        if not dfi.empty:
            dfi["ORIGEM"] = ttl
            dfs.append(dfi)
        if not dfmi.empty:
            dfmi["ORIGEM"] = ttl
            dfm_all.append(dfmi)
        titles.append(ttl)
    except Exception as e:
        st.error(f"Erro lendo planilha {sid}: {e}")

if not dfs:
    st.error("Nenhum dado lido das planilhas fornecidas.")
    st.stop()

df = pd.concat(dfs, ignore_index=True)
df_metas_all = pd.concat(dfm_all, ignore_index=True) if dfm_all else pd.DataFrame()

st.success("✅ Conectado a: " + " | ".join(titles))

# =============== Filtros (igual ao seu app) ===============
col_unid = "UNIDADE"; col_chassi = "CHASSI"

def _init_state():
    st.session_state.setdefault("unids_tmp", [])
    st.session_state.setdefault("vists_tmp", [])
_init_state()

unidades_opts = sorted([u for u in df[col_unid].dropna().unique()])
vist_opts = sorted([v for v in df["VISTORIADOR"].dropna().unique() if v])

def cb_sel_all_unids(): st.session_state.unids_tmp = unidades_opts[:] ; st.rerun()
def cb_clear_unids():   st.session_state.unids_tmp = [] ; st.rerun()
def cb_sel_all_vists(): st.session_state.vists_tmp = vist_opts[:] ; st.rerun()
def cb_clear_vists():   st.session_state.vists_tmp = [] ; st.rerun()

st.subheader("🔎 Filtros")
colU1, colU2 = st.columns([4,2])
with colU1:
    st.multiselect("Unidades", options=unidades_opts, key="unids_tmp")
with colU2:
    b1,b2 = st.columns(2)
    b1.button("Selecionar todas (Unid.)", use_container_width=True, on_click=cb_sel_all_unids)
    b2.button("Limpar (Unid.)", use_container_width=True, on_click=cb_clear_unids)

# faixa de datas
datas_validas = [d for d in df["__DATA__"] if isinstance(d, date)]
dmin = min(datas_validas) if datas_validas else date.today()
dmax = max(datas_validas) if datas_validas else date.today()
st.session_state.setdefault("dt_ini", dmin)
st.session_state.setdefault("dt_fim", dmax)

colD1, colD2 = st.columns(2)
with colD1:
    st.date_input("Data inicial", key="dt_ini", format="DD/MM/YYYY")
with colD2:
    st.date_input("Data final",   key="dt_fim", format="DD/MM/YYYY")

colV1, colV2 = st.columns([4,2])
with colV1:
    st.multiselect("Vistoriadores", options=vist_opts, key="vists_tmp")
with colV2:
    c1,c2 = st.columns(2)
    c1.button("Selecionar todos", use_container_width=True, on_click=cb_sel_all_vists)
    c2.button("Limpar",            use_container_width=True, on_click=cb_clear_vists)

# aplica filtros
view = df.copy()
if st.session_state.unids_tmp:
    view = view[view[col_unid].isin(st.session_state.unids_tmp)]
if st.session_state.dt_ini and st.session_state.dt_fim:
    view = view[(view["__DATA__"] >= st.session_state.dt_ini) & (view["__DATA__"] <= st.session_state.dt_fim)]
if st.session_state.vists_tmp:
    view = view[view["VISTORIADOR"].isin(st.session_state.vists_tmp)]

if view.empty:
    st.info("Nenhum registro para os filtros aplicados.")

# =============== KPIs ===============
vistorias_total   = int(len(view))
revistorias_total = int(view["IS_REV"].sum()) if not view.empty else 0
liq_total         = vistorias_total - revistorias_total
pct_rev           = (100*revistorias_total/vistorias_total) if vistorias_total else 0.0

cards = [
    ("Vistorias (geral)",   f"{vistorias_total:,}".replace(",", ".")),
    ("Vistorias líquidas",  f"{liq_total:,}".replace(",", ".")),
    (_nt("Revistorias"),    f"{revistorias_total:,}".replace(",", ".")),
    (_nt("% Revistorias"),  f"{pct_rev:,.1f}%".replace(",", "X").replace(".", ",").replace("X",".")),
]
st.markdown('<div class="card-container">' + "".join([f"<div class='card'><h4>{t}</h4><h2>{v}</h2></div>" for t,v in cards]) + "</div>", unsafe_allow_html=True)

# =============== Resumo por Vistoriador (com METAS por MESREF) ===============
st.markdown("<div class='section-title'>📋 Resumo por Vistoriador</div>", unsafe_allow_html=True)

grp = (view
       .groupby(["VISTORIADOR"], dropna=False)
       .agg(VISTORIAS=("IS_REV","size"),
            REVISTORIAS=("IS_REV","sum"),
            DIAS_ATIVOS=("__DATA__", lambda s: s.dropna().nunique()),
            UNIDADES=(col_unid, lambda s: s.dropna().nunique()))
       .reset_index())
grp["LIQUIDO"] = grp["VISTORIAS"] - grp["REVISTORIAS"]

# dias úteis passados por vistoriador
def _is_workday(d): return isinstance(d, date) and d.weekday()<5
wd = (view[view["__DATA__"].apply(_is_workday)]
      .groupby("VISTORIADOR")["__DATA__"].nunique().reset_index().rename(columns={"__DATA__":"DIAS_PASSADOS"}))
grp = grp.merge(wd, on="VISTORIADOR", how="left").fillna({"DIAS_PASSADOS":0})

# === junta METAS por MESREF ===
# Para cada VISTORIADOR no período filtrado, descobrimos a lista de MESREFs presentes e casamos com a METAS MESREF correspondente.
if not df_metas_all.empty:
    # reduz metas às colunas úteis
    metas_cols = [c for c in ["VISTORIADOR","UNIDADE","TIPO","META_MENSAL","DIAS_UTEIS","MESREF"] if c in df_metas_all.columns]
    metas = df_metas_all[metas_cols].copy()
    metas["VISTORIADOR"] = metas["VISTORIADOR"].map(_upper_strip)
else:
    metas = pd.DataFrame(columns=["VISTORIADOR","UNIDADE","TIPO","META_MENSAL","DIAS_UTEIS","MESREF"])

# Para projeções/atingimentos no mês corrente, criamos uma base mensal do "view"
# (ou seja: só os dados dentro dos filtros) agrupada por VISTORIADOR+MESREF
if not view.empty:
    view["MESREF"] = view["MESREF"].fillna("")
    base_mes = (view.groupby(["VISTORIADOR","MESREF"], dropna=False)
                    .agg(VISTORIAS=("IS_REV","size"), REVISTORIAS=("IS_REV","sum"))
                    .reset_index())
    base_mes["LIQUIDO"] = base_mes["VISTORIAS"] - base_mes["REVISTORIAS"]
else:
    base_mes = pd.DataFrame(columns=["VISTORIADOR","MESREF","VISTORIAS","REVISTORIAS","LIQUIDO"])

# para o quadro principal (Resumo por Vistoriador), vamos usar a META do MESREF mais recente que aparece para cada vistoriador no período filtrado
if not base_mes.empty and not metas.empty:
    # pega o MESREF mais recente (por ordem de data real) para cada vistoriador nos dados filtrados
    # mapeando MESREF -> (ano,mes) para ordenar
    def _ym_from_mesref(m):
        try:
            mm,aa = m.split("/")
            return (int(aa), int(mm))
        except: return (0,0)
    last_mesref = (base_mes.assign(_YM=base_mes["MESREF"].map(_ym_from_mesref))
                            .sort_values("_YM")
                            .groupby("VISTORIADOR").tail(1)[["VISTORIADOR","MESREF"]])
    # escolhe meta do mesmo MESREF
    metas_pick = (metas[["VISTORIADOR","TIPO","META_MENSAL","DIAS_UTEIS","MESREF"]]
                  .merge(last_mesref, on=["VISTORIADOR","MESREF"], how="right"))
else:
    metas_pick = pd.DataFrame(columns=["VISTORIADOR","TIPO","META_MENSAL","DIAS_UTEIS","MESREF"])

grp = grp.merge(metas_pick.drop_duplicates("VISTORIADOR"), on="VISTORIADOR", how="left")

# cálculos
grp["META_MENSAL"] = pd.to_numeric(grp.get("META_MENSAL",0), errors="coerce").fillna(0)
grp["DIAS_UTEIS"]  = pd.to_numeric(grp.get("DIAS_UTEIS",0),  errors="coerce").fillna(0)

grp["META_DIA"]        = np.where(grp["DIAS_UTEIS"]>0, grp["META_MENSAL"]/grp["DIAS_UTEIS"], 0.0)
grp["FALTANTE_MES"]    = np.maximum(grp["META_MENSAL"] - grp["LIQUIDO"], 0)
grp["DIAS_RESTANTES"]  = np.maximum(grp["DIAS_UTEIS"] - grp["DIAS_PASSADOS"], 0)
grp["NECESSIDADE_DIA"] = np.where(grp["DIAS_RESTANTES"]>0, grp["FALTANTE_MES"]/grp["DIAS_RESTANTES"], 0.0)
grp["MEDIA_DIA_ATUAL"] = np.where(grp["DIAS_PASSADOS"]>0, grp["LIQUIDO"]/grp["DIAS_PASSADOS"], 0.0)
grp["PROJECAO_MES"]    = (grp["LIQUIDO"] + grp["MEDIA_DIA_ATUAL"]*grp["DIAS_RESTANTES"]).round(0)
grp["TENDENCIA_%"]     = np.where(grp["META_MENSAL"]>0, (grp["PROJECAO_MES"]/grp["META_MENSAL"])*100, np.nan)

# formatação
def chip_tend(p):
    if pd.isna(p): return "—"
    p=float(p)
    if p>=100: return f"{p:.0f}% 🚀"
    if p>=95:  return f"{p:.0f}% 💪"
    if p>=85:  return f"{p:.0f}% 😬"
    return f"{p:.0f}% 😟"
def chip_nec(x):
    try: v=float(x)
    except: return "—"
    return "0 ✅" if v<=0 else f"{int(round(v))} 🔥"

fmt = grp.copy()
fmt["TIPO"] = fmt.get("TIPO","").replace({"MOVEL":"MÓVEL"}).fillna("")
fmt["TIPO"] = fmt["TIPO"].map({"FIXO":"🏢 FIXO", "MÓVEL":"🚗 MÓVEL"}).fillna("—")
for c in ["META_MENSAL","DIAS_UTEIS","VISTORIAS","REVISTORIAS","LIQUIDO","FALTANTE_MES","PROJECAO_MES"]:
    if c in fmt.columns:
        fmt[c] = fmt[c].map(lambda x: "—" if pd.isna(x) else f"{int(x)}")
fmt["META_DIA"]        = fmt["META_DIA"].map(lambda x: f"{x:,.1f}".replace(",", "X").replace(".", ",").replace("X","."))
fmt["NECESSIDADE_DIA"] = grp["NECESSIDADE_DIA"].apply(chip_nec)
fmt["TENDÊNCIA"]       = grp["TENDENCIA_%"].apply(chip_tend)

cols_show = ["VISTORIADOR","UNIDADE","TIPO","META_MENSAL","DIAS_UTEIS","META_DIA",
             "VISTORIAS","REVISTORIAS","LIQUIDO","FALTANTE_MES","NECESSIDADE_DIA","TENDÊNCIA","PROJECAO_MES","MESREF"]
st.dataframe(fmt[cols_show], use_container_width=True, hide_index=True)

# =============== Evolução diária ===============
st.markdown("<div class='section-title'>📈 Evolução diária</div>", unsafe_allow_html=True)
if not view.empty:
    daily = (view.groupby("__DATA__", dropna=False)
                  .agg(VISTORIAS=("IS_REV","size"), REVISTORIAS=("IS_REV","sum"))
                  .reset_index().sort_values("__DATA__"))
    daily["LIQUIDO"] = daily["VISTORIAS"] - daily["REVISTORIAS"]
    melt = daily.melt(id_vars="__DATA__", value_vars=["VISTORIAS","REVISTORIAS","LIQUIDO"],
                      var_name="Métrica", value_name="Valor")
    chart = (alt.Chart(melt).mark_line(point=True)
                .encode(x=alt.X("__DATA__:T", title="Data"),
                        y=alt.Y("Valor:Q", title="Quantidade"),
                        color=alt.Color("Métrica:N", title="Métrica"),
                        tooltip=[alt.Tooltip("__DATA__:T","Data"),
                                 alt.Tooltip("Métrica:N"),
                                 alt.Tooltip("Valor:Q")])
                .properties(height=360))
    st.altair_chart(chart, use_container_width=True)
else:
    st.caption("Sem dados no período selecionado.")

# =============== Produção por Unidade (Líquido) ===============
st.markdown("<div class='section-title'>🏙️ Produção por Unidade (Líquido)</div>", unsafe_allow_html=True)
if not view.empty:
    byu = (view.groupby(col_unid, dropna=False)
                .agg(liq=("IS_REV", lambda s: s.size - s.sum()))
                .reset_index().sort_values("liq", ascending=False))
    bar = (alt.Chart(byu).mark_bar()
            .encode(x=alt.X(f"{col_unid}:N", sort='-y', title="Unidade",
                            axis=alt.Axis(labelAngle=-30)),
                    y=alt.Y("liq:Q", title="Líquido"),
                    tooltip=[alt.Tooltip(f"{col_unid}:N","Unidade"),
                             alt.Tooltip("liq:Q","Líquido")])
            .properties(height=420))
    st.altair_chart(bar, use_container_width=True)
else:
    st.caption("Sem dados de unidades para o período.")

# =============== Auditoria: chassis com múltiplas vistorias ===============
st.markdown("<div class='section-title'>🕵️ Chassis com múltiplas vistorias</div>", unsafe_allow_html=True)
if not view.empty:
    dup = (view.groupby(col_chassi, dropna=False)
                .agg(QTD=("VISTORIADOR","size"),
                     PRIMEIRA_DATA=("**DATA**".replace("*","_"), "min"),  # só para evitar highlight
                     ULTIMA_DATA=("__DATA__", "max"))
                .reset_index())
    dup = dup[dup["QTD"]>=2].sort_values("QTD", ascending=False)
    if len(dup):
        # quem fez 1ª e última
        first_map = (view.sort_values(["__DATA__"])
                        .drop_duplicates(subset=[col_chassi], keep="first")
                        .set_index(col_chassi)["VISTORIADOR"].to_dict())
        last_map  = (view.sort_values(["__DATA__"])
                        .drop_duplicates(subset=[col_chassi], keep="last")
                        .set_index(col_chassi)["VISTORIADOR"].to_dict())
        dup["PRIMEIRO_VIST"] = dup[col_chassi].map(first_map)
        dup["ULTIMO_VIST"]   = dup[col_chassi].map(last_map)
        st.dataframe(dup, use_container_width=True, hide_index=True)
    else:
        st.caption("Nenhum chassi com múltiplas vistorias dentro dos filtros.")
else:
    st.caption("Nenhum chassi com múltiplas vistorias dentro dos filtros.")

# =============== Consolidado do Mês + Rankings (por MESREF do período) ===============
TOP_LABEL = "TOP BOX"; BOTTOM_LABEL = "BOTTOM BOX"
st.markdown("---")
st.markdown("<div class='section-title'>🧮 Consolidado do Mês + Ranking por Vistoriador</div>", unsafe_allow_html=True)

# Escolhe o MESREF do último dia presente no filtro
datas_ok = [d for d in view["__DATA__"] if isinstance(d, date)]
if not datas_ok:
    st.info("Sem datas dentro dos filtros atuais para montar o consolidado do mês.")
else:
    ref = sorted(datas_ok)[-1]
    mesref = f"{ref.month:02d}/{ref.year}"
    view_mes = view[view["MESREF"]==mesref].copy()

    prod_mes = (view_mes.groupby("VISTORIADOR", dropna=False)
                .agg(VISTORIAS=("IS_REV","size"),
                     REVISTORIAS=("IS_REV","sum"))
                .reset_index())
    prod_mes["LIQUIDO"] = prod_mes["VISTORIAS"] - prod_mes["REVISTORIAS"]

    # metas do MESREF específico
    if not metas.empty:
        metas_join = metas[metas["MESREF"]==mesref][["VISTORIADOR","TIPO","META_MENSAL"]].copy()
    else:
        metas_join = pd.DataFrame(columns=["VISTORIADOR","TIPO","META_MENSAL"])

    base_mes = prod_mes.merge(metas_join, on="VISTORIADOR", how="left")
    base_mes["TIPO"] = base_mes.get("TIPO","").astype(str).str.upper().replace({"MOVEL":"MÓVEL"}).replace("", "—")
    base_mes["META_MENSAL"] = pd.to_numeric(base_mes.get("META_MENSAL",0), errors="coerce").fillna(0)

    base_mes["ATING_%"] = np.where(base_mes["META_MENSAL"]>0,
                                   (base_mes["VISTORIAS"]/base_mes["META_MENSAL"])*100, np.nan)

    meta_tot = int(base_mes["META_MENSAL"].sum())
    vist_tot = int(base_mes["VISTORIAS"].sum())
    rev_tot  = int(base_mes["REVISTORIAS"].sum())
    liq_tot  = int(base_mes["LIQUIDO"].sum())
    ating_g  = (vist_tot/meta_tot*100) if meta_tot>0 else np.nan

    def chip_pct(p):
        if pd.isna(p): return "—"
        p=float(p)
        if p>=110: emo="🏆"
        elif p>=100: emo="🚀"
        elif p>=90:  emo="💪"
        elif p>=80:  emo="😬"
        else:        emo="😟"
        return f"{p:.0f}% {emo}"

    cards_mes = [
        ("Mês de referência", mesref),
        ("Meta (soma)", f"{meta_tot:,}".replace(",", ".")),
        ("Vistorias (geral)", f"{vist_tot:,}".replace(",", ".")),
        (_nt("Revistorias"), f"{rev_tot:,}".replace(",", ".")),
        ("Líquido", f"{liq_tot:,}".replace(",", ".")),
        ("% Ating. (sobre geral)", chip_pct(ating_g)),
    ]
    st.markdown('<div class="card-container">' + "".join([f"<div class='card'><h4>{t}</h4><h2>{v}</h2></div>" for t,v in cards_mes]) + "</div>", unsafe_allow_html=True)

    def chip_pct_row(p):
        if pd.isna(p): return "—"
        p=float(p)
        if p>=110: emo="🏆"
        elif p>=100: emo="🚀"
        elif p>=90:  emo="💪"
        elif p>=80:  emo="😬"
        else:        emo="😟"
        return f"{p:.0f}% {emo}"

    def render_ranking(df_sub, titulo):
        if df_sub.empty:
            st.caption(f"Sem dados para {titulo} em {mesref}.")
            return
        rk = df_sub[df_sub["META_MENSAL"]>0].copy()
        if rk.empty:
            st.caption(f"Ninguém com META cadastrada para {titulo}.")
            return
        rk = rk.sort_values("ATING_%", ascending=False)

        top = rk.head(5).copy()
        top["🏅"] = ["🥇","🥈","🥉","🏅","🏅"][:len(top)]
        top_fmt = pd.DataFrame({
            " ": top["🏅"],
            "Vistoriador": top["VISTORIADOR"],
            "Meta (mês)": top["META_MENSAL"].map(lambda x: f"{int(x):,}".replace(",", ".")),
            "Vistorias (geral)": top["VISTORIAS"].map(int),
            "Revistorias": top["REVISTORIAS"].map(int),
            "Líquido": top["LIQUIDO"].map(int),
            "% Ating. (geral/meta)": top["ATING_%"].map(chip_pct_row),
        })

        bot = rk.tail(5).sort_values("ATING_%", ascending=True).copy()
        bad = ["🆘","🪫","🐢","⚠️","⚠️"][:len(bot)]
        bot["⚠️"] = bad
        bot_fmt = pd.DataFrame({
            " ": bot["⚠️"],
            "Vistoriador": bot["VISTORIADOR"],
            "Meta (mês)": bot["META_MENSAL"].map(lambda x: f"{int(x):,}".replace(",", ".")),
            "Vistorias (geral)": bot["VISTORIAS"].map(int),
            "Revistorias": bot["REVISTORIAS"].map(int),
            "Líquido": bot["LIQUIDO"].map(int),
            "% Ating. (geral/meta)": bot["ATING_%"].map(chip_pct_row),
        })

        c1,c2 = st.columns(2)
        with c1:
            st.markdown(f"**{_nt(TOP_LABEL)} — {mesref}**", unsafe_allow_html=True)
            st.dataframe(top_fmt, use_container_width=True, hide_index=True)
        with c2:
            st.markdown(f"**{_nt(BOTTOM_LABEL)} — {mesref}**", unsafe_allow_html=True)
            st.dataframe(bot_fmt, use_container_width=True, hide_index=True)

    st.markdown("#### 🏢 FIXO")
    render_ranking(base_mes[base_mes["TIPO"]=="FIXO"], "vistoriadores FIXO")

    st.markdown("#### 🚗 MÓVEL")
    render_ranking(base_mes[base_mes["TIPO"].isin(["MÓVEL","MOVEL"])], "vistoriadores MÓVEL")
