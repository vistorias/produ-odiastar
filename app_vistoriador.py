# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# Painel de Produção por Vistoriador (Streamlit)
# - Lê várias planilhas dentro de uma PASTA no Google Drive
# - Pode juntar todos os meses ou escolher um arquivo
# - Respeita META por MÊS (merge por VISTORIADOR + REF_MONTH)
# - Mantém o visual e cálculos do seu código "perfeito"
# ------------------------------------------------------------

import os, re, json
from datetime import datetime, date

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =========================
# Config & título
# =========================
st.set_page_config(page_title="🧰 Produção por Vistoriador - Starcheck", layout="wide")
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
# IDs e segredos
# =========================
# -> A PASTA que você mostrou nas imagens (pode trocar aqui):
FOLDER_ID = "1rDeXts0WRA-lvx_FhqottTPEYf3IqsqI"

SERVICE_EMAIL = None

def _load_sa_info():
    """Carrega as credenciais do service account de st.secrets."""
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

def _auth_clients():
    """Autoriza Google (Sheets + Drive)."""
    global SERVICE_EMAIL
    info, _ = _load_sa_info()
    SERVICE_EMAIL = info.get("client_email", "(sem client_email)")
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scopes)
    gs_client = gspread.authorize(creds)
    drive_client = build("drive", "v3", credentials=creds)
    return gs_client, drive_client

def _drive_list_sheets_in_folder(drive, folder_id: str) -> list[dict]:
    """
    Lista Google Sheets dentro da pasta (inclui atalhos que apontam para Sheets).
    Retorna lista de dicts: {"id": <spreadsheetId>, "name": <nome>}
    """
    files_out = []
    page_token = None
    # buscamos tanto spreadsheet quanto shortcut (atalho)
    q = (
        f"'{folder_id}' in parents and trashed=false and "
        "("
        "mimeType='application/vnd.google-apps.spreadsheet' or "
        "mimeType='application/vnd.google-apps.shortcut'"
        ")"
    )
    fields = "nextPageToken, files(id, name, mimeType, shortcutDetails)"
    try:
        while True:
            resp = drive.files().list(
                q=q,
                fields=fields,
                pageSize=1000,
                pageToken=page_token,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                orderBy="modifiedTime desc"
            ).execute()
            for f in resp.get("files", []):
                mime = f.get("mimeType")
                if mime == "application/vnd.google-apps.shortcut":
                    sd = f.get("shortcutDetails", {}) or {}
                    tgt_mime = sd.get("targetMimeType")
                    tgt_id = sd.get("targetId")
                    if tgt_mime == "application/vnd.google-apps.spreadsheet" and tgt_id:
                        files_out.append({"id": tgt_id, "name": f.get("name", "(atalho)")})
                elif mime == "application/vnd.google-apps.spreadsheet":
                    files_out.append({"id": f["id"], "name": f.get("name", "")})
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
    except HttpError as e:
        raise RuntimeError(f"Falha ao listar arquivos da pasta ({folder_id}). {e}")
    return files_out

# =========================
# Leitura das planilhas (dados + METAS)
# =========================
def parse_date_any(x):
    if pd.isna(x) or x == "":
        return pd.NaT
    s = str(x).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    try:
        return pd.to_datetime(s).date()
    except:
        return pd.NaT

def _upper_strip(x):
    return str(x).upper().strip() if pd.notna(x) else ""

def _read_one_spreadsheet(gs: gspread.Client, sid: str) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Lê um spreadsheet:
      - dados da sheet1 (Página1)
      - aba METAS (se existir)
    Retorna: (df_dados, df_metas, title)
    """
    sh = gs.open_by_key(sid)
    title = sh.title

    ws = sh.sheet1
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    # padroniza colunas necessárias
    df.columns = [c.strip().upper() for c in df.columns]
    # checagem mínima
    for needed in ["UNIDADE", "DATA", "CHASSI"]:
        if needed not in df.columns:
            raise RuntimeError(f"A planilha '{title}' não contém a coluna obrigatória: {needed}")

    # data + vistoriador
    df["__DATA__"] = df["DATA"].apply(parse_date_any)
    if "PERITO" in df.columns and "DIGITADOR" in df.columns:
        df["VISTORIADOR"] = np.where(
            df["PERITO"].astype(str).str.strip() != "",
            df["PERITO"].map(_upper_strip),
            df["DIGITADOR"].map(_upper_strip)
        )
    elif "PERITO" in df.columns:
        df["VISTORIADOR"] = df["PERITO"].map(_upper_strip)
    elif "DIGITADOR" in df.columns:
        df["VISTORIADOR"] = df["DIGITADOR"].map(_upper_strip)
    else:
        df["VISTORIADOR"] = ""

    df["UNIDADE"] = df["UNIDADE"].map(_upper_strip)
    df["CHASSI"]  = df["CHASSI"].map(_upper_strip)

    # remover unidades inválidas
    ban = {"POSTO CÓDIGO", "POSTO CODIGO", "CÓDIGO", "CODIGO", "", "—", "NAN"}
    df = df[~df["UNIDADE"].isin(ban)].copy()

    # Revistoria
    df = df.sort_values(["__DATA__", "CHASSI"], kind="mergesort").reset_index(drop=True)
    df["__ORD__"] = df.groupby("CHASSI").cumcount()
    df["IS_REV"]  = (df["__ORD__"] >= 1).astype(int)

    # REF_MONTH: YYYY-MM (depende da DATA de cada linha)
    def _ref_month(d):
        if isinstance(d, date):
            return f"{d.year}-{d.month:02d}"
        return ""
    df["REF_MONTH"] = df["__DATA__"].map(_ref_month)

    # METAS (opcional)
    try:
        metas_ws = sh.worksheet("METAS")
        dfm = pd.DataFrame(metas_ws.get_all_records())
    except Exception:
        dfm = pd.DataFrame()

    if not dfm.empty:
        dfm.columns = [c.strip().upper() for c in dfm.columns]
        # normaliza nomes
        ren = {}
        for cand in ["META_MENSAL", "META MEN SAL", "META_MEN SAL", "META_MEN.SAL", "META MENSA"]:
            if cand in dfm.columns:
                ren[cand] = "META_MENSAL"
        for cand in ["DIAS UTEIS", "DIAS ÚTEIS", "DIAS_UTEIS"]:
            if cand in dfm.columns:
                ren[cand] = "DIAS_UTEIS"
        dfm = dfm.rename(columns=ren)
        # campos
        dfm["VISTORIADOR"] = dfm["VISTORIADOR"].map(_upper_strip)
        if "UNIDADE" in dfm.columns:
            dfm["UNIDADE"] = dfm["UNIDADE"].astype(str).map(_upper_strip)
        dfm["TIPO"] = dfm.get("TIPO", "").astype(str).map(_upper_strip)
        dfm["META_MENSAL"] = pd.to_numeric(dfm.get("META_MENSAL", 0), errors="coerce").fillna(0).astype(int)
        dfm["DIAS_UTEIS"]  = pd.to_numeric(dfm.get("DIAS_UTEIS", 0), errors="coerce").fillna(0).astype(int)

        # REF_MONTH das metas:
        # 1) tenta extrair do nome do arquivo (ex.: "09/2025 - Planilha...")
        # 2) se não achar, usa o menor mês presente nos dados
        m = re.search(r"(\d{2})/(\d{4})", title)
        if m:
            mm, yyyy = int(m.group(1)), int(m.group(2))
        else:
            # pega primeiro mês válido do df
            dates_ok = [d for d in df["__DATA__"] if isinstance(d, date)]
            if dates_ok:
                first = sorted(dates_ok)[0]
                yyyy, mm = first.year, first.month
            else:
                yyyy, mm = (date.today().year, date.today().month)

        dfm["REF_MONTH"] = f"{yyyy}-{mm:02d}"
    else:
        # sem metas
        dfm = pd.DataFrame(columns=["VISTORIADOR", "UNIDADE", "TIPO", "META_MENSAL", "DIAS_UTEIS", "REF_MONTH"])

    return df, dfm, title

# =========================
# UI: Conexão e escolha dos arquivos
# =========================
st.markdown("### Conexão com a Base — Planilhas na Pasta do Drive")
gs_client, drive_client = _auth_clients()

with st.expander("Arquivo (mês) da pasta", expanded=True):
    colA, colB = st.columns([3, 2])
    with colA:
        st.caption("Pasta do Drive")
        st.write(FOLDER_ID)

    try:
        files = _drive_list_sheets_in_folder(drive_client, FOLDER_ID)
    except Exception as e:
        st.error("Não consegui ler as planilhas da pasta.")
        st.info(f"Verifique se a pasta/arquivos estão compartilhados com **{SERVICE_EMAIL}** (Leitor/Editor) e se não são atalhos quebrados.")
        with st.expander("Detalhes do erro"):
            st.exception(e)
        st.stop()

    if not files:
        st.error("Não encontrei Google Sheets na pasta (confira permissões e se não são atalhos).")
        st.stop()

    # opções para o selectbox
    opts = ["🧩 TODOS OS ARQUIVOS (juntar)"] + [f["name"] for f in files]
    choice = st.selectbox("Escolha um arquivo ou junte todos:", options=opts, index=0)

# =========================
# Montagem do DataFrame final (dados + metas por mês)
# =========================
def _assemble(choice_label: str):
    if choice_label == "🧩 TODOS OS ARQUIVOS (juntar)":
        dfs = []
        metas = []
        for f in files:
            dfi, dmf, ttl = _read_one_spreadsheet(gs_client, f["id"])
            dfs.append(dfi)
            metas.append(dmf)
        big = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        big_m = pd.concat(metas, ignore_index=True) if metas else pd.DataFrame()
        src_label = "TODOS (juntados)"
    else:
        # acha o id correspondente ao nome
        sel = next((x for x in files if x["name"] == choice_label), None)
        if not sel:
            raise RuntimeError("Arquivo selecionado não encontrado.")
        dfi, dmf, ttl = _read_one_spreadsheet(gs_client, sel["id"])
        big, big_m = dfi, dmf
        src_label = ttl
    return big, big_m, src_label

try:
    df, df_metas_all, fonte = _assemble(choice)
    st.success(f"✅ Conectado: {fonte}")
except Exception as e:
    st.error("Falha ao montar os dados.")
    with st.expander("Detalhes"):
        st.exception(e)
    st.stop()

# =========================
# A PARTIR DAQUI: MESMA LÓGICA DO SEU CÓDIGO "PERFEITO"
#  (apenas pequenos ajustes para usar df_metas_all por REF_MONTH)
# =========================

# colunas base já padronizadas na leitura
col_unid   = "UNIDADE"
col_data   = "__DATA__"
col_chassi = "CHASSI"

# =========================
# Filtros (UI)
# =========================
def _init_state():
    st.session_state.setdefault("unids_tmp", [])
    st.session_state.setdefault("vists_tmp", [])
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

st.markdown("## 🔎 Filtros")

colU1, colU2 = st.columns([4,2])
with colU1:
    st.multiselect("Unidades", options=unidades_opts, key="unids_tmp")
with colU2:
    b1, b2 = st.columns(2)
    b1.button("Selecionar todas (Unid.)", use_container_width=True, on_click=cb_sel_all_unids)
    b2.button("Limpar (Unid.)", use_container_width=True, on_click=cb_clear_unids)

datas_validas = [d for d in df[col_data] if isinstance(d, date)]
dmin = min(datas_validas) if datas_validas else date.today()
dmax = max(datas_validas) if datas_validas else date.today()

if "dt_ini" not in st.session_state:
    st.session_state["dt_ini"] = dmin
if "dt_fim" not in st.session_state:
    st.session_state["dt_fim"] = dmax

colD1, colD2 = st.columns(2)
with colD1:
    st.date_input("Data inicial", key="dt_ini", format="DD/MM/YYYY")
with colD2:
    st.date_input("Data final", key="dt_fim", format="DD/MM/YYYY")

colV1, colV2 = st.columns([4,2])
with colV1:
    st.multiselect("Vistoriadores", options=vist_opts, key="vists_tmp")
with colV2:
    b3, b4 = st.columns(2)
    b3.button("Selecionar todos", use_container_width=True, on_click=cb_sel_all_vists)
    b4.button("Limpar", use_container_width=True, on_click=cb_clear_vists)

# =========================
# Aplicar filtros
# =========================
view = df.copy()
if st.session_state.unids_tmp:
    view = view[view[col_unid].isin(st.session_state.unids_tmp)]
if st.session_state.dt_ini and st.session_state.dt_fim:
    view = view[(view[col_data] >= st.session_state.dt_ini) & (view[col_data] <= st.session_state.dt_fim)]
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
            DIAS_ATIVOS=(col_data, lambda s: s.dropna().nunique()),
            UNIDADES=(col_unid, lambda s: s.dropna().nunique()),
       )
       .reset_index())

grp["LIQUIDO"] = grp["VISTORIAS"] - grp["REVISTORIAS"]

def _is_workday(d):
    return isinstance(d, date) and d.weekday() < 5

def _calc_wd_passados(df_view: pd.DataFrame) -> pd.DataFrame:
    if df_view.empty:
        return pd.DataFrame(columns=["VISTORIADOR", "DIAS_PASSADOS"])
    mask = df_view[col_data].apply(_is_workday)
    if not mask.any():
        vists = df_view["VISTORIADOR"].dropna().unique()
        return pd.DataFrame({"VISTORIADOR": vists, "DIAS_PASSADOS": np.zeros(len(vists), dtype=int)})
    out = (df_view.loc[mask]
           .groupby("VISTORIADOR")[col_data]
           .nunique()
           .reset_index()
           .rename(columns={col_data: "DIAS_PASSADOS"}))
    out["DIAS_PASSADOS"] = out["DIAS_PASSADOS"].astype(int)
    return out

wd_passados = _calc_wd_passados(view)
grp = grp.merge(wd_passados, on="VISTORIADOR", how="left")
grp["DIAS_PASSADOS"] = grp["DIAS_PASSADOS"].fillna(0).astype(int)

# === METAS por mês (merge por REF_MONTH)
# cria REF_MONTH na view (para conseguir cruzar com metas do mês correto)
view["_REF_MONTH"] = view["REF_MONTH"].fillna("")
# metas consolidadas (de todas as planilhas lidas)
metas_all = df_metas_all.copy()

if metas_all is not None and len(metas_all):
    metas_cols = [c for c in ["VISTORIADOR", "UNIDADE", "TIPO", "META_MENSAL", "DIAS_UTEIS", "REF_MONTH"] if c in metas_all.columns]
    # média e projeções dependem da quantidade de dias úteis do mês da meta; então
    # vamos calcular por VISTORIADOR usando o REF_MONTH MAIS RECENTE dentro dos dados filtrados
    # 1) Para cada vistoriador, identificar os REF_MONTH presentes em 'view'
    v_rm = (view.groupby(["VISTORIADOR", "_REF_MONTH"]).size().reset_index(name="n"))
    # 2) pegar o último REF_MONTH (ordem cronológica) para cada vistoriador dentro do filtro
    def _max_ref_month(s):
        # 'YYYY-MM' ordena lexicograficamente
        s = [x for x in s if x]
        return max(s) if s else ""
    v_rm_last = v_rm.groupby("VISTORIADOR")["_REF_MONTH"].apply(_max_ref_month).reset_index().rename(columns={"_REF_MONTH":"REF_MONTH"})
    # 3) juntar com metas daquele REF_MONTH
    metas_join = v_rm_last.merge(metas_all[metas_cols], on=["VISTORIADOR","REF_MONTH"], how="left")
    grp = grp.merge(metas_join.drop(columns=["REF_MONTH"]), on="VISTORIADOR", how="left")

    grp["UNIDADE"] = grp.get("UNIDADE", "").fillna("")
    grp["TIPO"]     = grp.get("TIPO", "").fillna("")
    for c in ["META_MENSAL", "DIAS_UTEIS"]:
        grp[c] = pd.to_numeric(grp.get(c, 0), errors="coerce").fillna(0)

    grp["META_MENSAL"] = grp["META_MENSAL"].astype(int)
    grp["DIAS_UTEIS"]  = grp["DIAS_UTEIS"].astype(int)
else:
    grp["TIPO"] = ""
    grp["META_MENSAL"] = 0
    grp["DIAS_UTEIS"]  = 0

# cálculos
grp["META_DIA"] = np.where(grp["DIAS_UTEIS"]>0, grp["META_MENSAL"]/grp["DIAS_UTEIS"], 0.0)
grp["FALTANTE_MES"] = np.maximum(grp["META_MENSAL"] - grp["LIQUIDO"], 0)
grp["DIAS_RESTANTES"] = np.maximum(grp["DIAS_UTEIS"] - grp["DIAS_PASSADOS"], 0)
grp["NECESSIDADE_DIA"] = np.where(grp["DIAS_RESTANTES"]>0, grp["FALTANTE_MES"]/grp["DIAS_RESTANTES"], 0.0)
grp["MEDIA_DIA_ATUAL"] = np.where(grp["DIAS_PASSADOS"]>0, grp["LIQUIDO"]/grp["DIAS_PASSADOS"], 0.0)

for c in ["LIQUIDO", "MEDIA_DIA_ATUAL", "DIAS_RESTANTES"]:
    grp[c] = pd.to_numeric(grp[c], errors="coerce")

grp["LIQUIDO"]         = grp["LIQUIDO"].fillna(0)
grp["MEDIA_DIA_ATUAL"] = grp["MEDIA_DIA_ATUAL"].fillna(0)
grp["DIAS_RESTANTES"]  = grp["DIAS_RESTANTES"].fillna(0).clip(lower=0)

grp["PROJECAO_MES"] = (grp["LIQUIDO"] + grp["MEDIA_DIA_ATUAL"] * grp["DIAS_RESTANTES"]).round(0)
grp["TENDENCIA_%"] = np.where(grp["META_MENSAL"]>0, (grp["PROJECAO_MES"]/grp["META_MENSAL"])*100, np.nan)

grp = grp.sort_values(["PROJECAO_MES","LIQUIDO"], ascending=[False, False])

# formatação (com emojis)
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
             .groupby(col_data, dropna=False)
             .agg(VISTORIAS=("IS_REV","size"),
                  REVISTORIAS=("IS_REV","sum"))
             .reset_index())
    daily = daily[pd.notna(daily[col_data])].sort_values(col_data)
    daily["LIQUIDO"] = daily["VISTORIAS"] - daily["REVISTORIAS"]

    daily_melt = daily.melt(id_vars=col_data, value_vars=["VISTORIAS","REVISTORIAS","LIQUIDO"],
                            var_name="Métrica", value_name="Valor")

    if daily_melt.empty:
        st.caption("Sem evolução diária para exibir.")
    else:
        line = (alt.Chart(daily_melt)
                .mark_line(point=True)
                .encode(
                    x=alt.X(f"{col_data}:T", title="Data"),
                    y=alt.Y("Valor:Q", title="Quantidade"),
                    color=alt.Color("Métrica:N", title="Métrica"),
                    tooltip=[alt.Tooltip(f"{col_data}:T", title="Data"),
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
                     PRIMEIRA_DATA=(col_data, "min"),
                     ULTIMA_DATA=(col_data, "max"))
                .reset_index())
    dup = dup[dup["QTD"] >= 2].sort_values("QTD", ascending=False)

    if len(dup) == 0:
        st.caption("Nenhum chassi com múltiplas vistorias dentro dos filtros.")
    else:
        first_map = (view.sort_values([col_data])
                        .drop_duplicates(subset=[col_chassi], keep="first")
                        .set_index(col_chassi)["VISTORIADOR"].to_dict())
        last_map = (view.sort_values([col_data])
                        .drop_duplicates(subset=[col_chassi], keep="last")
                        .set_index(col_chassi)["VISTORIADOR"].to_dict())
        dup["PRIMEIRO_VIST"] = dup[col_chassi].map(first_map)
        dup["ULTIMO_VIST"]   = dup[col_chassi].map(last_map)
        st.dataframe(dup, use_container_width=True, hide_index=True)

# =========================
# Consolidado do mês + ranking (usa o mês da ÚLTIMA data do filtro)
# =========================
TOP_LABEL = "TOP BOX"
BOTTOM_LABEL = "BOTTOM BOX"

st.markdown("---")
st.markdown("<div class='section-title'>🧮 Consolidado do Mês + Ranking por Vistoriador</div>", unsafe_allow_html=True)

datas_ok = [d for d in view[col_data] if isinstance(d, date)]
if len(datas_ok) == 0:
    st.info("Sem datas dentro dos filtros atuais para montar o consolidado do mês.")
else:
    ref = sorted(datas_ok)[-1]
    ref_ano, ref_mes = ref.year, ref.month
    mes_label = f"{ref_mes:02d}/{ref_ano}"
    mask_mes = view[col_data].apply(lambda d: isinstance(d, date) and d.year == ref_ano and d.month == ref_mes)
    view_mes = view[mask_mes].copy()

    prod_mes = (view_mes.groupby("VISTORIADOR", dropna=False)
        .agg(VISTORIAS=("IS_REV", "size"),
             REVISTORIAS=("IS_REV", "sum"))
        .reset_index())
    prod_mes["LIQUIDO"] = prod_mes["VISTORIAS"] - prod_mes["REVISTORIAS"]

    # juntar metas do mês correto (REF_MONTH do ref)
    ref_month_key = f"{ref_ano}-{ref_mes:02d}"
    metas_join = df_metas_all[df_metas_all["REF_MONTH"] == ref_month_key][["VISTORIADOR","TIPO","META_MENSAL"]].copy() if len(df_metas_all) else pd.DataFrame(columns=["VISTORIADOR","TIPO","META_MENSAL"])

    base_mes = prod_mes.merge(metas_join, on="VISTORIADOR", how="left")
    base_mes["TIPO"] = base_mes["TIPO"].astype(str).map(_upper_strip).replace({"MOVEL": "MÓVEL"}).replace("", "—")
    base_mes["META_MENSAL"] = pd.to_numeric(base_mes["META_MENSAL"], errors="coerce").fillna(0)

    base_mes["ATING_%"] = np.where(base_mes["META_MENSAL"] > 0,
                                   (base_mes["VISTORIAS"] / base_mes["META_MENSAL"]) * 100,
                                   np.nan)

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
        "".join([f"<div class='card'><h4>{t}</h4><h2>{v}</h2></div>" for t, v in cards_mes]) +
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
# Ranking do DIA (igual ao seu)
# =========================
st.markdown("---")
st.markdown("<div class='section-title'>📅 Ranking do Dia por Vistoriador</div>", unsafe_allow_html=True)

dates_avail = sorted([d for d in view[col_data] if isinstance(d, date)])
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

    view_dia = view[view[col_data] == used_day].copy()

    prod_dia = (view_dia.groupby("VISTORIADOR", dropna=False)
                .agg(VISTORIAS_DIA=("IS_REV", "size"),
                     REVISTORIAS_DIA=("IS_REV", "sum"))
                .reset_index())
    prod_dia["LIQUIDO_DIA"] = prod_dia["VISTORIAS_DIA"] - prod_dia["REVISTORIAS_DIA"]

    # metas do mês referente ao dia escolhido
    ref_month_key = f"{used_day.year}-{used_day.month:02d}"
    if len(df_metas_all):
        metas_join = df_metas_all[df_metas_all["REF_MONTH"] == ref_month_key][["VISTORIADOR", "TIPO", "META_MENSAL", "DIAS_UTEIS"]].copy()
    else:
        metas_join = pd.DataFrame(columns=["VISTORIADOR", "TIPO", "META_MENSAL", "DIAS_UTEIS"])

    base_dia = prod_dia.merge(metas_join, on="VISTORIADOR", how="left")
    base_dia["TIPO"] = base_dia["TIPO"].astype(str).str.upper().replace({"MOVEL": "MÓVEL"}).replace("", "—")
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
