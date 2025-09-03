# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# Painel de Produção por Vistoriador (Streamlit) - versão visual (emojis)
# - Lê planilhas de uma PASTA do Google Drive (ID ou URL)
# - Você escolhe o arquivo (mês) OU "todos os arquivos (juntar)"
# - Reseta filtros ao trocar de arquivo
# - Correções para sábados/domingos, merges, METAS opcional
# ------------------------------------------------------------

import os, json, re, requests
from datetime import datetime, date

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =========================
# Config & título
# =========================
st.set_page_config(page_title="🧰 Produção por Vistoriador - Starcheck", layout="wide")
st.title("🧰 Painel de Produção por Vistoriador - Starcheck")

# --- proteção contra auto-tradução do navegador (Chrome/Edge) ---
st.markdown("""
<style>
  .notranslate { unicode-bidi: plaintext; }
  .hero { background-color:#f0f2f6; padding:15px; border-radius:12px; margin-bottom:18px; box-shadow:0 1px 3px rgba(0,0,0,.10); }
  .card-container { display:flex; gap:18px; margin:12px 0 22px; flex-wrap:wrap; }
  .card { background:#f5f5f5; padding:18px 20px; border-radius:12px; box-shadow:0 2px 6px rgba(0,0,0,.10); text-align:center; min-width:200px; flex:1; }
  .card h4 { color:#cc3300; margin:0 0 8px; font-size:16px; font-weight:700; }
  .card h2 { margin:0; font-size:26px; font-weight:800; color:#222; }
  .section-title { font-size:20px; font-weight:800; margin:22px 0 8px; }
  .small { color:#7b7b7b; font-size:13px; }
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

# =========================
# PASTA DO DRIVE (cole ID ou URL)
# =========================
FOLDER_ID_OR_URL = "https://drive.google.com/drive/folders/1rDeXts0WRA-lvx_FhqottTPEYf3Iqsql"
SERVICE_EMAIL = "(carregando...)"

# =========================
# Autenticação & helpers
# =========================
def _load_sa_info():
    """Carrega credenciais do Service Account de st.secrets['gcp_service_account']."""
    try:
        block = st.secrets["gcp_service_account"]
    except Exception as e:
        st.error("Não encontrei [gcp_service_account] no .streamlit/secrets.toml.")
        with st.expander("Detalhes"):
            st.exception(e)
        st.stop()

    if "json_path" in block:
        path = block["json_path"]
        if not os.path.isabs(path):
            path = os.path.join(os.path.dirname(__file__), path)
        try:
            with open(path, "r", encoding="utf-8") as f:
                info = json.load(f)
            return info, f"file:{path}"
        except Exception as e:
            st.error(f"Não consegui abrir o JSON: {path}")
            with st.expander("Detalhes"):
                st.exception(e)
            st.stop()
    return dict(block), "dict"

def _resolve_folder_id(val: str | None) -> str | None:
    """Aceita ID OU URL da pasta e devolve só o ID."""
    if not val:
        return None
    s = str(val).strip()
    m = re.search(r'/folders/([a-zA-Z0-9_-]+)', s)
    if m:
        return m.group(1)
    if re.fullmatch(r'[a-zA-Z0-9_-]{10,}', s):
        return s
    return None

@st.cache_data(ttl=300)
def listar_planilhas_da_pasta(folder_id_or_url: str) -> list[dict]:
    """
    Lista Google Sheets dentro da pasta usando a API do Drive.
    Retorna lista de dicts: {id, name, modifiedTime, createdTime}.
    Ignora atalhos.
    """
    info, _ = _load_sa_info()
    scopes = [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/spreadsheets.readonly",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scopes)
    token = creds.get_access_token().access_token

    fid = _resolve_folder_id(folder_id_or_url)
    if not fid:
        raise RuntimeError("FOLDER_ID inválido. Cole o ID OU a URL da pasta do Drive.")

    url = "https://www.googleapis.com/drive/v3/files"
    # mimeType de Google Sheets (não atalho)
    q = (
        f"'{fid}' in parents "
        "and trashed=false "
        "and (mimeType='application/vnd.google-apps.spreadsheet')"
    )
    params = {
        "q": q,
        "fields": "files(id,name,modifiedTime,createdTime)",
        "orderBy": "modifiedTime desc",
        "pageSize": 1000,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    files = r.json().get("files", [])
    return files

def conectar_gsheets(sheet_id: str):
    """Conecta em uma planilha (sheet_id) e devolve (worksheet sheet1, dataframe)."""
    global SERVICE_EMAIL
    info, _ = _load_sa_info()
    SERVICE_EMAIL = info.get("client_email", "(sem client_email)")
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scopes)
        client = gspread.authorize(creds)
        sh = client.open_by_key(sheet_id)
        ws = sh.sheet1
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        return ws, df
    except Exception as e:
        st.error("Não consegui ler a planilha.")
        st.info(f"Compartilhe com: **{SERVICE_EMAIL}** (Leitor/Editor).")
        with st.expander("Erro ao abrir a planilha"):
            st.exception(e)
        st.stop()

# --- Lê a aba METAS (opcional) ---
def ler_aba_metas(worksheet_handle) -> pd.DataFrame | None:
    """Tenta ler a worksheet 'METAS' da planilha escolhida."""
    if worksheet_handle is None:
        return None
    try:
        metas_ws = worksheet_handle.spreadsheet.worksheet("METAS")
    except Exception:
        return None

    metas = pd.DataFrame(metas_ws.get_all_records())
    if metas.empty:
        return metas

    metas.columns = [c.strip().upper() for c in metas.columns]
    ren = {}
    for cand in ["META_MENSAL", "META MEN SAL", "META_MEN SAL", "META_MEN.SAL", "META MENSA"]:
        if cand in metas.columns: ren[cand] = "META_MENSAL"
    for cand in ["DIAS UTEIS", "DIAS ÚTEIS", "DIAS_UTEIS"]:
        if cand in metas.columns: ren[cand] = "DIAS_UTEIS"
    metas = metas.rename(columns=ren)

    metas["VISTORIADOR"] = metas["VISTORIADOR"].astype(str).str.upper().str.strip()
    if "UNIDADE" in metas.columns:
        metas["UNIDADE"] = metas["UNIDADE"].astype(str).str.upper().str.strip()
    metas["TIPO"] = metas.get("TIPO", "").astype(str).str.upper().str.strip()

    metas["META_MENSAL"] = pd.to_numeric(metas.get("META_MENSAL", 0), errors="coerce").fillna(0).astype(int)
    metas["DIAS_UTEIS"]  = pd.to_numeric(metas.get("DIAS_UTEIS", 0),  errors="coerce").fillna(0).astype(int)
    return metas

# =========================
# Conexão / escolha do mês (arquivo da pasta)
# =========================
st.markdown("#### Conexão com a Base — Planilhas na Pasta do Drive")

# lista arquivos
try:
    arquivos = listar_planilhas_da_pasta(FOLDER_ID_OR_URL)
except Exception as e:
    st.error("Não consegui ler as planilhas da pasta.")
    st.info(
        "Verifique:\n"
        "1) Se o FOLDER_ID é o ID ou a URL da pasta;\n"
        f"2) Se a pasta/arquivos estão compartilhados com **{SERVICE_EMAIL}** (Leitor/Editor);\n"
        "3) Se os arquivos são **Google Sheets** (não atalhos)."
    )
    with st.expander("Detalhes do erro"):
        st.exception(e)
    st.stop()

if not arquivos:
    st.error("Não encontrei Google Sheets na pasta (confira permissões e se não são atalhos).")
    st.stop()

# Selectbox para escolher o arquivo (mês) OU juntar todos
opcoes = ["🧩 TODOS OS ARQUIVOS (juntar)"] + [f"{a['modifiedTime'][:10]} — {a['name']}" for a in arquivos]
idx_default = 1  # default = o mais recente
escolha = st.selectbox("Arquivo (mês) da pasta", opcoes, index=idx_default)

# carrega planilhas
df_raw_list = []
metas_list = []
sheet_ids_usados = []

def _load_one(a):
    ws, df0 = conectar_gsheets(a["id"])
    metas0 = ler_aba_metas(ws)
    return ws, df0, metas0

if escolha.startswith("🧩"):
    for a in arquivos:
        try:
            ws, df0, metas0 = _load_one(a)
            if not df0.empty:
                df0["__SRC_NOME__"] = a["name"]
                df0["__SRC_ID__"] = a["id"]
                df_raw_list.append(df0)
                sheet_ids_usados.append(a["id"])
            if metas0 is not None and not metas0.empty:
                metas0["__SRC_ID__"] = a["id"]
                metas_list.append(metas0)
        except Exception as e:
            with st.expander(f"Erro ao ler {a['name']}"):
                st.exception(e)
else:
    escolhido = arquivos[opcoes.index(escolha) - 1]  # -1 por causa do "todos"
    ws, df0, metas0 = _load_one(escolhido)
    if not df0.empty:
        df0["__SRC_NOME__"] = escolhido["name"]
        df0["__SRC_ID__"] = escolhido["id"]
        df_raw_list.append(df0)
        sheet_ids_usados.append(escolhido["id"])
    if metas0 is not None and not metas0.empty:
        metas0["__SRC_ID__"] = escolhido["id"]
        metas_list.append(metas0)

# junta dados
if len(df_raw_list) == 0:
    st.error("Consegui abrir, mas a(s) planilha(s) não retornaram linhas.")
    st.stop()

df_raw = pd.concat(df_raw_list, ignore_index=True, sort=False)
df_metas = pd.concat(metas_list, ignore_index=True, sort=False) if metas_list else None

# Mensagem de conexão OK
if escolha.startswith("🧩"):
    st.success(f"✅ Conectado: {len(sheet_ids_usados)} planilhas (juntas).")
else:
    st.success(f"✅ Conectado: {escolhido['name']} (modificado em {escolhido['modifiedTime'][:10]})")

# =========================
# Limpeza e padronização
# =========================
def _upper_strip(x):
    return str(x).upper().strip() if pd.notna(x) else ""

def parse_date_any(x):
    if pd.isna(x) or x == "": return pd.NaT
    s = str(x).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try: return datetime.strptime(s, fmt).date()
        except: pass
    try: return pd.to_datetime(s).date()
    except: return pd.NaT

df = df_raw.copy()
df.columns = [c.strip().upper() for c in df.columns]

col_unid  = "UNIDADE"   if "UNIDADE"   in df.columns else None
col_data  = "DATA"      if "DATA"      in df.columns else None
col_chassi= "CHASSI"    if "CHASSI"    in df.columns else None
col_perito= "PERITO"    if "PERITO"    in df.columns else None
col_digit = "DIGITADOR" if "DIGITADOR" in df.columns else None

required = [col_unid, col_data, col_chassi, (col_perito or col_digit)]
if any(c is None for c in required):
    st.error("A(s) planilha(s) precisam conter as colunas: UNIDADE, DATA, CHASSI, PERITO/DIGITADOR.")
    st.stop()

# Normalizar
df[col_unid]   = df[col_unid].map(_upper_strip)
df[col_chassi] = df[col_chassi].map(_upper_strip)
df["__DATA__"] = df[col_data].apply(parse_date_any)

# VISTORIADOR
if col_perito and col_digit:
    df["VISTORIADOR"] = np.where(
        df[col_perito].astype(str).str.strip() != "",
        df[col_perito].map(_upper_strip),
        df[col_digit].map(_upper_strip)
    )
elif col_perito:
    df["VISTORIADOR"] = df[col_perito].map(_upper_strip)
else:
    df["VISTORIADOR"] = df[col_digit].map(_upper_strip)

# Revistoria (ordem por data + chassi)
df = df.sort_values(["__DATA__", col_chassi], kind="mergesort").reset_index(drop=True)
df["__ORD__"] = df.groupby(col_chassi).cumcount()
df["IS_REV"] = (df["__ORD__"] >= 1).astype(int)

# Remover "POSTO CÓDIGO/CODIGO" e valores vazios de unidade
BAN_UNIDS = {"POSTO CÓDIGO", "POSTO CODIGO", "CÓDIGO", "CODIGO", "", "—", "NAN"}
df = df[~df[col_unid].isin(BAN_UNIDS)].copy()

# =========================
# Reset de filtros ao trocar de arquivo
# =========================
def _reset_filters_for_df(df_base: pd.DataFrame):
    unidades_opts = sorted([u for u in df_base[col_unid].dropna().unique()])
    vist_opts = sorted([v for v in df_base["VISTORIADOR"].dropna().unique() if v])

    datas_validas = [d for d in df_base["__DATA__"] if isinstance(d, date)]
    dmin = min(datas_validas) if datas_validas else date.today()
    dmax = max(datas_validas) if datas_validas else date.today()

    st.session_state["unids_tmp"] = unidades_opts[:]      # seleciona todas por padrão? deixe vazio
    st.session_state["vists_tmp"] = []                    # vazio
    st.session_state["dt_ini"] = dmin
    st.session_state["dt_fim"] = dmax
    st.session_state["__data_token__"] = ",".join(sheet_ids_usados) or "ALL"

# se mudou o arquivo, reseta datas/filtros
token_atual = ",".join(sheet_ids_usados) or "ALL"
if st.session_state.get("__data_token__") != token_atual:
    _reset_filters_for_df(df)

# =========================
# Estado / Callbacks dos filtros
# =========================
def _init_state():
    st.session_state.setdefault("unids_tmp", [])
    st.session_state.setdefault("vists_tmp", [])
    st.session_state.setdefault("dt_ini", None)
    st.session_state.setdefault("dt_fim", None)
_init_state()

unidades_opts = sorted([u for u in df[col_unid].dropna().unique()])
vist_opts = sorted([v for v in df["VISTORIADOR"].dropna().unique() if v])

def cb_sel_all_vists():
    st.session_state.vists_tmp = vist_opts[:]
    st.rerun()

def cb_clear_vists():
    st.session_state.vists_tmp = []
    st.rerun()

def cb_sel_all_unids():
    st.session_state.unids_tmp = unidades_opts[:]
    st.rerun()

def cb_clear_unids():
    st.session_state.unids_tmp = []
    st.rerun()

# =========================
# Filtros (UI)
# =========================
st.subheader("🔎 Filtros")

colU1, colU2 = st.columns([4,2])
with colU1:
    st.multiselect(
        "Unidades",
        options=unidades_opts,
        key="unids_tmp",
        help="Selecione as unidades desejadas"
    )
with colU2:
    b1, b2 = st.columns(2)
    b1.button("Selecionar todas (Unid.)", use_container_width=True, on_click=cb_sel_all_unids)
    b2.button("Limpar (Unid.)", use_container_width=True, on_click=cb_clear_unids)

colD1, colD2 = st.columns(2)
with colD1:
    st.date_input("Data inicial", key="dt_ini", format="DD/MM/YYYY")
with colD2:
    st.date_input("Data final", key="dt_fim", format="DD/MM/YYYY")

colV1, colV2 = st.columns([4,2])
with colV1:
    st.multiselect(
        "Vistoriadores",
        options=vist_opts,
        key="vists_tmp",
        help="Filtra pela(s) pessoa(s)."
    )
with colV2:
    b3, b4 = st.columns(2)
    b3.button("Selecionar todos", use_container_width=True, on_click=cb_sel_all_vists)
    b4.button("Limpar", use_container_width=True, on_click=cb_clear_vists)

if st.button("🔄 Atualizar dados (recarregar)"):
    st.cache_data.clear()
    st.rerun()

# =========================
# Aplicar filtros aos dados
# =========================
view = df.copy()
if st.session_state.unids_tmp:
    view = view[view[col_unid].isin(st.session_state.unids_tmp)]
if st.session_state.dt_ini and st.session_state.dt_fim:
    view = view[(view["__DATA__"] >= st.session_state.dt_ini) & (view["__DATA__"] <= st.session_state.dt_fim)]
if st.session_state.vists_tmp:
    view = view[view["VISTORIADOR"].isin(st.session_state.vists_tmp)]

if view.empty:
    st.info("Nenhum registro para os filtros aplicados.")

# =========================
# KPIs (cartões)
# =========================
vistorias_total   = int(len(view))
revistorias_total = int(view["IS_REV"].sum()) if not view.empty else 0
liq_total         = int(vistorias_total - revistorias_total)
pct_rev           = (100 * revistorias_total / vistorias_total) if vistorias_total else 0.0

cards = [
    ("Vistorias (geral)",   f"{vistorias_total:,}".replace(",", ".")),
    ("Vistorias líquidas",  f"{liq_total:,}".replace(",", ".")),
    (_nt("Revistorias"),    f"{revistorias_total:,}".replace(",", ".")),
    (_nt("% Revistorias"),  f"{pct_rev:,.1f}%".replace(",", "X").replace(".", ",").replace("X", ".")),
]

st.markdown(
    '<div class="card-container">' +
    "".join([f"<div class='card'><h4>{t}</h4><h2>{v}</h2></div>" for t, v in cards]) +
    "</div>", unsafe_allow_html=True
)

# =========================
# Resumo por Vistoriador
# =========================
st.markdown("<div class='section-title'>📋 Resumo por Vistoriador</div>", unsafe_allow_html=True)

grp = (view
       .groupby("VISTORIADOR", dropna=False)
       .agg(
            VISTORIAS=("IS_REV", "size"),
            REVISTORIAS=("IS_REV", "sum"),
            DIAS_ATIVOS=("__DATA__", lambda s: s.dropna().nunique()),
            UNIDADES=(col_unid, lambda s: s.dropna().nunique()),
       )
       .reset_index())

grp["LIQUIDO"]  = grp["VISTORIAS"] - grp["REVISTORIAS"]

# ---- dias úteis passados por vistoriador (robusto p/ sábado/domingo)
def _is_workday(d):
    return isinstance(d, date) and d.weekday() < 5  # 0..4 = seg–sex

def _calc_wd_passados(df_view: pd.DataFrame) -> pd.DataFrame:
    if df_view.empty or "__DATA__" not in df_view.columns or "VISTORIADOR" not in df_view.columns:
        return pd.DataFrame(columns=["VISTORIADOR", "DIAS_PASSADOS"])
    mask = df_view["__DATA__"].apply(_is_workday)
    if not mask.any():
        vists = df_view["VISTORIADOR"].dropna().unique()
        return pd.DataFrame({"VISTORIADOR": vists, "DIAS_PASSADOS": np.zeros(len(vists), dtype=int)})
    out = (df_view.loc[mask]
           .groupby("VISTORIADOR")["__DATA__"]
           .nunique()
           .reset_index()
           .rename(columns={"__DATA__": "DIAS_PASSADOS"}))
    out["DIAS_PASSADOS"] = out["DIAS_PASSADOS"].astype(int)
    return out

wd_passados = _calc_wd_passados(view)
grp = grp.merge(wd_passados, on="VISTORIADOR", how="left")
grp["DIAS_PASSADOS"] = grp["DIAS_PASSADOS"].fillna(0).astype(int)

# ---- junta METAS (se existir a aba)
if df_metas is not None and len(df_metas):
    metas_cols = [c for c in ["VISTORIADOR", "UNIDADE", "TIPO", "META_MENSAL", "DIAS_UTEIS"] if c in df_metas.columns]
    metas_use = df_metas[metas_cols].copy()
    # padroniza
    metas_use["VISTORIADOR"] = metas_use["VISTORIADOR"].astype(str).str.upper().str.strip()
    if "UNIDADE" in metas_use.columns:
        metas_use["UNIDADE"] = metas_use["UNIDADE"].astype(str).str.upper().str.strip()
    if "TIPO" in metas_use.columns:
        metas_use["TIPO"] = metas_use["TIPO"].astype(str).str.upper().replace({"MOVEL": "MÓVEL"})
    for c in ["META_MENSAL", "DIAS_UTEIS"]:
        if c in metas_use.columns:
            metas_use[c] = pd.to_numeric(metas_use[c], errors="coerce").fillna(0)
    grp = grp.merge(metas_use, on="VISTORIADOR", how="left")

    grp["UNIDADE"] = grp.get("UNIDADE", "").fillna("")
    grp["TIPO"]     = grp.get("TIPO", "").fillna("")
    for c in ["META_MENSAL", "DIAS_UTEIS"]:
        grp[c] = pd.to_numeric(grp.get(c), errors="coerce").fillna(0)
    grp["META_MENSAL"] = grp["META_MENSAL"].astype(int)
    grp["DIAS_UTEIS"]  = grp["DIAS_UTEIS"].astype(int)
else:
    grp["TIPO"] = ""
    grp["META_MENSAL"] = 0
    grp["DIAS_UTEIS"]  = 0

# ---- cálculos de meta/dia, faltante, necessidade/dia, projeção e tendência
grp["META_DIA"] = np.where(grp["DIAS_UTEIS"]>0, grp["META_MENSAL"]/grp["DIAS_UTEIS"], 0.0)
grp["FALTANTE_MES"] = np.maximum(grp["META_MENSAL"] - grp["LIQUIDO"], 0)
grp["DIAS_RESTANTES"] = np.maximum(grp["DIAS_UTEIS"] - grp["DIAS_PASSADOS"], 0)
grp["NECESSIDADE_DIA"] = np.where(grp["DIAS_RESTANTES"]>0,
                                  grp["FALTANTE_MES"]/grp["DIAS_RESTANTES"], 0.0)
grp["MEDIA_DIA_ATUAL"] = np.where(grp["DIAS_PASSADOS"]>0, grp["LIQUIDO"]/grp["DIAS_PASSADOS"], 0.0)

for c in ["LIQUIDO", "MEDIA_DIA_ATUAL", "DIAS_RESTANTES"]:
    grp[c] = pd.to_numeric(grp[c], errors="coerce")

grp["LIQUIDO"]         = grp["LIQUIDO"].fillna(0)
grp["MEDIA_DIA_ATUAL"] = grp["MEDIA_DIA_ATUAL"].fillna(0)
grp["DIAS_RESTANTES"]  = grp["DIAS_RESTANTES"].fillna(0).clip(lower=0)

grp["PROJECAO_MES"] = (grp["LIQUIDO"] + grp["MEDIA_DIA_ATUAL"] * grp["DIAS_RESTANTES"]).round(0)
grp["TENDENCIA_%"] = np.where(grp["META_MENSAL"]>0, (grp["PROJECAO_MES"]/grp["META_MENSAL"])*100, np.nan)

# ---- ordenação
grp = grp.sort_values(["PROJECAO_MES","LIQUIDO"], ascending=[False, False])

# ---- formatação (com emojis)
fmt = grp.copy()

def chip_tend(p):
    if pd.isna(p): return "—"
    p = float(p)
    if p >= 100: return f"{p:.0f}% 🚀"
    if p >= 95:  return f"{p:.0f}% 💪"
    if p >= 85:  return f"{p:.0f}% 😬"
    return f"{p:.0f}% 😟"

def chip_nec(x):
    try:
        v = float(x)
    except:
        return "—"
    return "0 ✅" if v <= 0 else f"{int(round(v))} 🔥"

fmt["TIPO"] = fmt["TIPO"].map({"FIXO": "🏢 FIXO", "MÓVEL": "🚗 MÓVEL", "MOVEL": "🚗 MÓVEL"}).fillna("—")
fmt["META_MENSAL"]      = fmt["META_MENSAL"].map(lambda x: f"{int(x):,}".replace(",", "."))
fmt["DIAS_UTEIS"]       = fmt["DIAS_UTEIS"].map(lambda x: f"{int(x)}")
fmt["META_DIA"]         = fmt["META_DIA"].map(lambda x: f"{x:,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))
fmt["VISTORIAS"]        = fmt["VISTORIAS"].map(lambda x: f"{int(x)}")
fmt["REVISTORIAS"]      = fmt["REVISTORIAS"].map(lambda x: f"{int(x)}")
fmt["LIQUIDO"]          = fmt["LIQUIDO"].map(lambda x: f"{int(x)}")
fmt["FALTANTE_MES"]     = fmt["FALTANTE_MES"].map(lambda x: f"{int(x)}")
fmt["NECESSIDADE_DIA"]  = grp["NECESSIDADE_DIA"].apply(chip_nec)
fmt["TENDÊNCIA"]        = grp["TENDENCIA_%"].apply(chip_tend)
fmt["PROJECAO_MES"]     = fmt["PROJECAO_MES"].map(lambda x: "—" if pd.isna(x) else f"{int(round(x))}")

cols_show = [
    "VISTORIADOR", "UNIDADE", "TIPO",
    "META_MENSAL", "DIAS_UTEIS", "META_DIA",
    "VISTORIAS", "REVISTORIAS", "LIQUIDO",
    "FALTANTE_MES", "NECESSIDADE_DIA", "TENDÊNCIA", "PROJECAO_MES"
]

if fmt.empty:
    st.caption("Sem registros para os filtros aplicados.")
else:
    st.dataframe(fmt[cols_show], use_container_width=True, hide_index=True)
    csv = fmt[cols_show].to_csv(index=False).encode("utf-8-sig")
    st.download_button("⬇️ Baixar resumo (CSV)", data=csv, file_name="resumo_vistoriador.csv", mime="text/csv")

# =========================
# Evolução diária
# =========================
st.markdown("<div class='section-title'>📈 Evolução diária</div>", unsafe_allow_html=True)

if view.empty:
    st.caption("Sem dados no período selecionado.")
else:
    daily = (view
             .groupby("__DATA__", dropna=False)
             .agg(VISTORIAS=("IS_REV","size"),
                  REVISTORIAS=("IS_REV","sum"))
             .reset_index())
    daily = daily[pd.notna(daily["__DATA__"])].sort_values("__DATA__")
    daily["LIQUIDO"] = daily["VISTORIAS"] - daily["REVISTORIAS"]

    daily_melt = daily.melt(id_vars="__DATA__", value_vars=["VISTORIAS","REVISTORIAS","LIQUIDO"],
                            var_name="Métrica", value_name="Valor")

    if daily_melt.empty:
        st.caption("Sem evolução diária para exibir.")
    else:
        line = (alt.Chart(daily_melt)
                .mark_line(point=True)
                .encode(
                    x=alt.X("__DATA__:T", title="Data"),
                    y=alt.Y("Valor:Q", title="Quantidade"),
                    color=alt.Color("Métrica:N", title="Métrica"),
                    tooltip=[alt.Tooltip("__DATA__:T", title="Data"),
                             alt.Tooltip("Métrica:N", title="Métrica"),
                             alt.Tooltip("Valor:Q", title="Valor")]
                )
                .properties(height=360))
        st.altair_chart(line, use_container_width=True)

# =========================
# Produção por Unidade (Líquido)
# =========================
st.markdown("<div class='section-title'>🏙️ Produção por Unidade (Líquido)</div>", unsafe_allow_html=True)

if view.empty:
    st.caption("Sem dados de unidades para o período.")
else:
    by_unid = (view.groupby(col_unid, dropna=False)
                    .agg(liq=("IS_REV", lambda s: s.size - s.sum()))
                    .reset_index()
                    .sort_values("liq", ascending=False))

    if by_unid.empty:
        st.caption("Sem produção por unidade dentro dos filtros.")
    else:
        bar_unid = (alt.Chart(by_unid)
                    .mark_bar()
                    .encode(
                        x=alt.X(f"{col_unid}:N", sort='-y', title="Unidade",
                                axis=alt.Axis(labelAngle=-30)),
                        y=alt.Y("liq:Q", title="Líquido"),
                        tooltip=[alt.Tooltip(f"{col_unid}:N", title="Unidade"),
                                 alt.Tooltip("liq:Q", title="Líquido")]
                    )
                    .properties(height=420))
        st.altair_chart(bar_unid, use_container_width=True)

# =========================
# Auditoria – Chassis com múltiplas vistorias
# =========================
st.markdown("<div class='section-title'>🕵️ Chassis com múltiplas vistorias</div>", unsafe_allow_html=True)

if view.empty:
    st.caption("Nenhum chassi com múltiplas vistorias dentro dos filtros.")
else:
    dup = (view.groupby(col_chassi, dropna=False)
                .agg(QTD=("VISTORIADOR","size"),
                     PRIMEIRA_DATA=("__DATA__", "min"),
                     ULTIMA_DATA=("__DATA__", "max"))
                .reset_index())
    dup = dup[dup["QTD"] >= 2].sort_values("QTD", ascending=False)

    if len(dup) == 0:
        st.caption("Nenhum chassi com múltiplas vistorias dentro dos filtros.")
    else:
        first_map = (view.sort_values(["__DATA__"])
                        .drop_duplicates(subset=[col_chassi], keep="first")
                        .set_index(col_chassi)["VISTORIADOR"]
                        .to_dict())
        last_map = (view.sort_values(["__DATA__"])
                        .drop_duplicates(subset=[col_chassi], keep="last")
                        .set_index(col_chassi)["VISTORIADOR"]
                        .to_dict())
        dup["PRIMEIRO_VIST"] = dup[col_chassi].map(first_map)
        dup["ULTIMO_VIST"]   = dup[col_chassi].map(last_map)

        st.dataframe(dup, use_container_width=True, hide_index=True)

# =========================
# 🧮 CONSOLIDADO DO MÊS + RANKING MENSAL (TOP/BOTTOM)
# =========================
TOP_LABEL = "TOP BOX"
BOTTOM_LABEL = "BOTTOM BOX"

st.markdown("---")
st.markdown("<div class='section-title'>🧮 Consolidado do Mês + Ranking por Vistoriador</div>", unsafe_allow_html=True)

datas_ok = [d for d in view["__DATA__"] if isinstance(d, date)]
if len(datas_ok) == 0:
    st.info("Sem datas dentro dos filtros atuais para montar o consolidado do mês.")
else:
    ref = sorted(datas_ok)[-1]
    ref_ano, ref_mes = ref.year, ref.month
    mes_label = f"{ref_mes:02d}/{ref_ano}"
    mask_mes = view["__DATA__"].apply(lambda d: isinstance(d, date) and d.year == ref_ano and d.month == ref_mes)
    view_mes = view[mask_mes].copy()

    # produção mensal por vistoriador
    prod_mes = (view_mes
        .groupby("VISTORIADOR", dropna=False)
        .agg(
            VISTORIAS=("IS_REV", "size"),
            REVISTORIAS=("IS_REV", "sum")
        ).reset_index())
    prod_mes["LIQUIDO"] = prod_mes["VISTORIAS"] - prod_mes["REVISTORIAS"]

    # metas por vistoriador (TIPO, META_MENSAL)
    if df_metas is not None and len(df_metas):
        metas_join = df_metas[["VISTORIADOR", "TIPO", "META_MENSAL"]].copy() if "TIPO" in df_metas.columns else df_metas[["VISTORIADOR", "META_MENSAL"]].copy()
    else:
        metas_join = pd.DataFrame(columns=["VISTORIADOR", "TIPO", "META_MENSAL"])

    base_mes = prod_mes.merge(metas_join, on="VISTORIADOR", how="left")
    base_mes["TIPO"] = base_mes.get("TIPO", "").astype(str).str.upper().replace({"MOVEL":"MÓVEL"}).replace("", "—")
    base_mes["META_MENSAL"] = pd.to_numeric(base_mes.get("META_MENSAL", 0), errors="coerce").fillna(0)

    # % de atingimento (sobre o REALIZADO GERAL)
    base_mes["ATING_%"] = np.where(base_mes["META_MENSAL"] > 0,
                                   (base_mes["VISTORIAS"] / base_mes["META_MENSAL"]) * 100,
                                   np.nan)

    # Cartões do consolidado
    meta_tot = int(base_mes["META_MENSAL"].sum())
    vist_tot = int(base_mes["VISTORIAS"].sum())
    rev_tot  = int(base_mes["REVISTORIAS"].sum())
    liq_tot  = int(base_mes["LIQUIDO"].sum())
    ating_g  = (vist_tot / meta_tot * 100) if meta_tot > 0 else np.nan

    def chip_pct(p):
        if pd.isna(p): return "—"
        p = float(p)
        if p >= 110: emo = "🏆"
        elif p >= 100: emo = "🚀"
        elif p >= 90: emo = "💪"
        elif p >= 80: emo = "😬"
        else: emo = "😟"
        return f"{p:.0f}% {emo}"

    cards_mes = [
        ("Mês de referência", mes_label),
        ("Meta (soma)", f"{meta_tot:,}".replace(",", ".")),
        ("Vistorias (geral)", f"{vist_tot:,}".replace(",", ".")),
        (_nt("Revistorias"), f"{rev_tot:,}".replace(",", ".")),
        ("Líquido", f"{liq_tot:,}".replace(",", ".")),
        ("% Ating. (sobre geral)", chip_pct(ating_g)),
    ]
    st.markdown(
        '<div class="card-container">' +
        "".join([f"<div class=\'card\'><h4>{t}</h4><h2>{v}</h2></div>" for t, v in cards_mes]) +
        "</div>", unsafe_allow_html=True
    )

    def chip_pct_row(p):
        if pd.isna(p): return "—"
        p = float(p)
        if p >= 110: emo = "🏆"
        elif p >= 100: emo = "🚀"
        elif p >= 90: emo = "💪"
        elif p >= 80: emo = "😬"
        else: emo = "😟"
        return f"{p:.0f}% {emo}"

    def render_ranking(df_sub, titulo):
        if len(df_sub) == 0:
            st.caption(f"Sem dados para {titulo} em {mes_label}.")
            return

        rk = df_sub[df_sub["META_MENSAL"] > 0].copy()
        if len(rk) == 0:
            st.caption(f"Ninguém com META cadastrada para {titulo}.")
            return

        rk = rk.sort_values("ATING_%", ascending=False)

        # TOP 5
        top = rk.head(5).copy()
        medals = ["🥇", "🥈", "🥉", "🏅", "🏅"]
        top["🏅"] = [medals[i] if i < len(medals) else "🏅" for i in range(len(top))]
        top_fmt = pd.DataFrame({
            " ": top["🏅"],
            "Vistoriador": top["VISTORIADOR"],
            "Meta (mês)": top["META_MENSAL"].map(lambda x: f"{int(x):,}".replace(",", ".")),
            "Vistorias (geral)": top["VISTORIAS"].map(int),
            "Revistorias": top["REVISTORIAS"].map(int),
            "Líquido": top["LIQUIDO"].map(int),
            "% Ating. (geral/meta)": top["ATING_%"].map(chip_pct_row),
        })

        # BOTTOM 5
        bot = rk.tail(5).sort_values("ATING_%", ascending=True).copy()
        badgies = ["🆘", "🪫", "🐢", "⚠️", "⚠️"]
        bot["⚠️"] = [badgies[i] if i < len(badgies) else "⚠️" for i in range(len(bot))]
        bot_fmt = pd.DataFrame({
            " ": bot["⚠️"],
            "Vistoriador": bot["VISTORIADOR"],
            "Meta (mês)": bot["META_MENSAL"].map(lambda x: f"{int(x):,}".replace(",", ".")),
            "Vistorias (geral)": bot["VISTORIAS"].map(int),
            "Revistorias": bot["REVISTORIAS"].map(int),
            "Líquido": bot["LIQUIDO"].map(int),
            "% Ating. (geral/meta)": bot["ATING_%"].map(chip_pct_row),
        })

        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**{_nt(TOP_LABEL)} — {mes_label}**", unsafe_allow_html=True)
            st.dataframe(top_fmt, use_container_width=True, hide_index=True)
        with c2:
            st.markdown(f"**{_nt(BOTTOM_LABEL)} — {mes_label}**", unsafe_allow_html=True)
            st.dataframe(bot_fmt, use_container_width=True, hide_index=True)

    st.markdown("#### 🏢 FIXO")
    render_ranking(base_mes[base_mes["TIPO"] == "FIXO"], "vistoriadores FIXO")

    st.markdown("#### 🚗 MÓVEL")
    render_ranking(base_mes[base_mes["TIPO"].isin(["MÓVEL", "MOVEL"])], "vistoriadores MÓVEL")

# =========================
# 📅 RANKING DO DIA POR VISTORIADOR (TOP/BOTTOM)
# =========================
TOP_LABEL = "TOP BOX"
BOTTOM_LABEL = "BOTTOM BOX"

st.markdown("---")
st.markdown("<div class='section-title'>📅 Ranking do Dia por Vistoriador</div>", unsafe_allow_html=True)

dates_avail = sorted([d for d in view["__DATA__"] if isinstance(d, date)])
if not dates_avail:
    st.info("Sem datas dentro dos filtros atuais para montar o ranking diário.")
else:
    default_day = dates_avail[-1]
    rank_day = st.date_input(
        "Dia para o ranking",
        value=st.session_state.get("rank_day_sel", default_day),
        format="DD/MM/YYYY",
        key="rank_day_sel",
    )

    if rank_day in dates_avail:
        used_day = rank_day
        info_msg = None
    else:
        cands = [d for d in dates_avail if d <= rank_day]
        used_day = cands[-1] if cands else dates_avail[-1]
        info_msg = f"Sem dados em {rank_day.strftime('%d/%m/%Y')}. Exibindo {used_day.strftime('%d/%m/%Y')}."

    dia_label = used_day.strftime("%d/%m/%Y")
    if info_msg:
        st.caption(info_msg)
    st.caption(f"Dia exibido no ranking: **{dia_label}**")

    view_dia = view[view["__DATA__"] == used_day].copy()

    # produção do dia por vistoriador
    prod_dia = (view_dia.groupby("VISTORIADOR", dropna=False)
                .agg(VISTORIAS_DIA=("IS_REV", "size"),
                     REVISTORIAS_DIA=("IS_REV", "sum"))
                .reset_index())
    prod_dia["LIQUIDO_DIA"] = prod_dia["VISTORIAS_DIA"] - prod_dia["REVISTORIAS_DIA"]

    # metas (para META_DIA) vindas da aba METAS
    if (df_metas is not None) and len(df_metas):
        metas_join = df_metas[["VISTORIADOR", "TIPO", "META_MENSAL", "DIAS_UTEIS"]].copy() if set(["TIPO","DIAS_UTEIS"]).issubset(df_metas.columns) else pd.DataFrame(columns=["VISTORIADOR","TIPO","META_MENSAL","DIAS_UTEIS"])
    else:
        metas_join = pd.DataFrame(columns=["VISTORIADOR", "TIPO", "META_MENSAL", "DIAS_UTEIS"])

    base_dia = prod_dia.merge(metas_join, on="VISTORIADOR", how="left")
    base_dia["TIPO"] = base_dia.get("TIPO", "").astype(str).str.upper().replace({"MOVEL": "MÓVEL"}).replace("", "—")
    for c in ["META_MENSAL", "DIAS_UTEIS"]:
        base_dia[c] = pd.to_numeric(base_dia.get(c, 0), errors="coerce").fillna(0)

    base_dia["META_DIA"] = np.where(base_dia["DIAS_UTEIS"] > 0,
                                    base_dia["META_MENSAL"] / base_dia["DIAS_UTEIS"],
                                    0.0)
    base_dia["ATING_DIA_%"] = np.where(base_dia["META_DIA"] > 0,
                                       (base_dia["VISTORIAS_DIA"] / base_dia["META_DIA"]) * 100,
                                       np.nan)

    def chip_pct_row_dia(p):
        if pd.isna(p): return "—"
        p = float(p)
        if p >= 110: emo = "🏆"
        elif p >= 100: emo = "🚀"
        elif p >= 90: emo = "💪"
        elif p >= 80: emo = "😬"
        else: emo = "😟"
        return f"{p:.0f}% {emo}"

    def render_ranking_dia(df_sub, titulo):
        if df_sub.empty:
            st.caption(f"Sem dados para {titulo} em {dia_label}.")
            return

        rk = df_sub[df_sub["META_DIA"] > 0].copy()
        if rk.empty:
            st.caption(f"Ninguém com META do dia cadastrada para {titulo}.")
            return

        rk = rk.sort_values("ATING_DIA_%", ascending=False)

        # TOP 5
        top = rk.head(5).copy()
        medals = ["🥇", "🥈", "🥉", "🏅", "🏅"]
        top["🏅"] = [medals[i] if i < len(medals) else "🏅" for i in range(len(top))]
        top_fmt = pd.DataFrame({
            " ": top["🏅"],
            "Vistoriador": top["VISTORIADOR"],
            "Meta (dia)": top["META_DIA"].map(lambda x: int(round(x))),
            "Vistorias (dia)": top["VISTORIAS_DIA"].map(int),
            "Revistorias": top["REVISTORIAS_DIA"].map(int),
            "Líquido (dia)": top["LIQUIDO_DIA"].map(int),
            "% Ating. (dia)": top["ATING_DIA_%"].map(chip_pct_row_dia),
        })

        # BOTTOM 5
        bot = rk.tail(5).sort_values("ATING_DIA_%", ascending=True).copy()
        badgies = ["🆘", "🪫", "🐢", "⚠️", "⚠️"]
        bot["⚠️"] = [badgies[i] if i < len(badgies) else "⚠️" for i in range(len(bot))]
        bot_fmt = pd.DataFrame({
            " ": bot["⚠️"],
            "Vistoriador": bot["VISTORIADOR"],
            "Meta (dia)": bot["META_DIA"].map(lambda x: int(round(x))),
            "Vistorias (dia)": bot["VISTORIAS_DIA"].map(int),
            "Revistorias": bot["REVISTORIAS_DIA"].map(int),
            "Líquido (dia)": bot["LIQUIDO_DIA"].map(int),
            "% Ating. (dia)": bot["ATING_DIA_%"].map(chip_pct_row_dia),
        })

        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**{_nt(TOP_LABEL)}**", unsafe_allow_html=True)
            st.dataframe(top_fmt, use_container_width=True, hide_index=True)
        with c2:
            st.markdown(f"**{_nt(BOTTOM_LABEL)}**", unsafe_allow_html=True)
            st.dataframe(bot_fmt, use_container_width=True, hide_index=True)

    st.markdown("#### 🏢 FIXO")
    render_ranking_dia(base_dia[base_dia["TIPO"] == "FIXO"], "vistoriadores FIXO")

    st.markdown("#### 🚗 MÓVEL")
    render_ranking_dia(base_dia[base_dia["TIPO"].isin(["MÓVEL", "MOVEL"])], "vistoriadores MÓVEL")
