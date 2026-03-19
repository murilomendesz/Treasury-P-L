"""
treasury.py — Book de Tesouraria Automatizado
Autor  : Murilo Mendes | murilomnds99@gmail.com

Fontes de dados (100% BACEN SGS — gratuito, sem bloqueio):
    432  → Selic meta % a.a.
    1178 → Selic % a.m.  (VNA LFT — meses fechados)
    11   → Fator Selic diário (VNA LFT — dias correntes)
    433  → IPCA % a.m.   (VNA NTN-B)
    13522→ IPCA acumulado 12m
    226  → Taxa LTN % a.a.      ← substitui API Tesouro Direto
    227  → Taxa NTN-F % a.a.    ← substitui API Tesouro Direto
    228  → Taxa NTN-B % a.a. real ← substitui API Tesouro Direto
    1190 → Spread LFT % a.a.    ← substitui API Tesouro Direto

Execução:
    python treasury.py              → roda uma vez
    python treasury.py --scheduler  → agenda diária às 09:05
"""

import os, csv, sys, time, logging, smtplib, requests, schedule, calendar
import pandas as pd
import xlwings as xw
import matplotlib.pyplot as plt
from datetime          import datetime, date, timedelta
from email.mime.text   import MIMEText
from email.mime.multipart import MIMEMultipart
from bizdays            import Calendar

# ─────────────────────────────────────────────────────────────────────────────
# 1. CONFIGURAÇÕES
# ─────────────────────────────────────────────────────────────────────────────

EXCEL_PATH    = "carteira_legado.xlsx"
HISTORICO_CSV = "historico.csv"
LOG_FILE      = "treasury.log"
DATA_COMPRA   = date(2026, 3, 7)
DV01_LIMITE   = 10_000

EMAIL_ATIVO     = False
EMAIL_REMETENTE = "seuemail@gmail.com"
EMAIL_SENHA     = "sua_senha_de_app"
EMAIL_DESTINO   = "seuemail@gmail.com"

# Paleta visual uniforme ao paper DXY
_AZUL_ESC  = "#1a1a2e"
_AZUL_CLAR = "#63b3ed"
_VERDE     = "#2d6a4f"
_VERMELHO  = "#c0392b"
_AMARELO   = "#f39c12"
_SUBTEXT   = "#555577"
_BG        = "#ffffff"

# ─────────────────────────────────────────────────────────────────────────────
# 2. LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# 3. CALENDÁRIO ANBIMA
# ─────────────────────────────────────────────────────────────────────────────

try:
    cal = Calendar.load("ANBIMA")
except Exception:
    cal = Calendar.load("Brazil/ANBIMA")

def dias_uteis(d1: date, d2: date) -> int:
    return cal.bizdays(d1, d2)

# ─────────────────────────────────────────────────────────────────────────────
# 4. MAPEAMENTO DA CARTEIRA
# ─────────────────────────────────────────────────────────────────────────────

CARTEIRA_CONFIG = {
    "LTN 01/07/2026"   : {"tipo": "LTN",   "venc": "2026-07-01"},
    "LTN 01/01/2028"   : {"tipo": "LTN",   "venc": "2028-01-01"},
    "NTN-F 01/01/2031" : {"tipo": "NTN-F", "venc": "2031-01-01"},
    "NTN-B 15/05/2029" : {"tipo": "NTN-B", "venc": "2029-05-15"},
    "NTN-B 15/05/2035" : {"tipo": "NTN-B", "venc": "2035-05-15"},
    "LFT 01/03/2027"   : {"tipo": "LFT",   "venc": "2027-03-01"},
    "LFT 01/09/2030"   : {"tipo": "LFT",   "venc": "2030-09-01"},
}

# Quantidades padrão por título — proporcionais ao PU para notional equivalente (~R$960k cada).
# Referência: LTN 01/07/2026 (PU ≈ R$966) com 1.000 unidades.
# Para alterar, edite os valores abaixo. O Excel refletirá automaticamente na próxima execução.
QUANTIDADES = {
    "LTN 01/07/2026"   : 1_000,   # PU ≈ R$    966  →  1.000 × R$966  ≈ R$966k
    "LTN 01/01/2028"   : 1_000,   # PU ≈ R$    805  →  1.000 × R$805  ≈ R$805k
    "NTN-F 01/01/2031" : 1_000,   # PU ≈ R$    919  →  1.000 × R$919  ≈ R$919k
    "NTN-B 15/05/2029" :   200,   # PU ≈ R$  4.580  →    200 × R$4580 ≈ R$916k
    "NTN-B 15/05/2035" :   200,   # PU ≈ R$  4.330  →    200 × R$4330 ≈ R$866k
    "LFT 01/03/2027"   :    50,   # PU ≈ R$ 18.560  →     50 × R$18560 ≈ R$928k
    "LFT 01/09/2030"   :    50,   # PU ≈ R$ 18.380  →     50 × R$18380 ≈ R$919k
}

# ─────────────────────────────────────────────────────────────────────────────
# 5. COLETA BACEN SGS
#
#    Problema identificado: a API do BACEN retorna erro 400 quando
#    solicitamos muitos registros de uma vez (ex: 310).
#    Solução: buscar em chunks de 100 registros e concatenar.
#    Isso respeita o limite da API sem perder dados históricos.
# ─────────────────────────────────────────────────────────────────────────────

_BACEN_BASE  = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"

def _get_bacen_ultimo(serie: int) -> float | None:
    """Busca apenas o valor mais recente de uma série."""
    url = f"{_BACEN_BASE.format(serie=serie)}/ultimos/1?formato=json"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return float(r.json()[-1]["valor"].replace(",", "."))
    except Exception as e:
        log.warning(f"BACEN série {serie} (último valor): {e}")
        return None


def _get_bacen_periodo(serie: int, data_ini: str, data_fim: str) -> list[dict]:
    """
    Busca série por período (dd/MM/yyyy).
    Mais confiável que 'ultimos/N' para históricos longos —
    a API aceita períodos completos sem limite de registros.
    """
    url = (f"{_BACEN_BASE.format(serie=serie)}"
           f"?formato=json&dataInicial={data_ini}&dataFinal={data_fim}")
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        dados = r.json()
        if isinstance(dados, list) and len(dados) > 0:
            return dados
        return []
    except Exception as e:
        log.warning(f"BACEN série {serie} ({data_ini}→{data_fim}): {e}")
        return []




def _get_bacen_historico_desde(serie: int, data_ini_fixa: str) -> list[dict]:
    """
    Busca histórico desde uma data fixa até hoje em janelas de 4 anos.

    Por que data fixa:
        Evita incluir dados antes da data-base do VNA (01/07/2000).
        Se buscássemos a série 433 (IPCA) desde 1980, incluiríamos meses
        de hiperinflação (IPCA > 80% a.m.) que inflam o VNA incorretamente.

    Por que janelas de 4 anos:
        A API do BACEN aceita períodos sem limite de registros quando
        buscados por data, mas períodos muito longos podem gerar timeout.
        Janelas de 4 anos (~48 meses) são seguras e rápidas.

    data_ini_fixa: "dd/MM/yyyy" — data de início do VNA.
    """
    hoje   = date.today()
    d_ini  = datetime.strptime(data_ini_fixa, "%d/%m/%Y").date()
    todos  = []
    JANELA = 4  # anos por chunk

    ano_ini = d_ini.year
    mes_ini = d_ini.month

    while True:
        ano_fim = min(ano_ini + JANELA, hoje.year)
        mes_fim = hoje.month if ano_fim == hoje.year else 12

        d_i = f"01/{mes_ini:02d}/{ano_ini}"
        ultimo_dia = calendar.monthrange(ano_fim, mes_fim)[1]
        d_f = f"{ultimo_dia:02d}/{mes_fim:02d}/{ano_fim}"

        chunk = _get_bacen_periodo(serie, d_i, d_f)
        if chunk:
            todos.extend(chunk)

        if ano_fim >= hoje.year:
            break

        ano_ini = ano_fim + 1
        mes_ini = 1

    # Remove duplicatas por data e ordena cronologicamente
    vistos, unicos = set(), []
    for d in todos:
        if d["data"] not in vistos:
            vistos.add(d["data"])
            unicos.append(d)
    unicos.sort(key=lambda x: datetime.strptime(x["data"], "%d/%m/%Y"))
    return unicos


# ─────────────────────────────────────────────────────────────────────────────
# 6. VNA LFT — acumulação da Selic desde 01/07/2000
# ─────────────────────────────────────────────────────────────────────────────

def calcular_vna_lft() -> float:
    """
    VNA LFT de HOJE = R$1.000 × Π(1 + Selic_dia_i / 100)
    acumulando todos os dias úteis desde 01/07/2000 até hoje.
    Série 11: taxa em % ao dia (ex: 0.059400 = 0.0594% ao dia)
    Valor esperado em 03/2026: ~R$18.400–18.600
    """
    log.info("  Calculando VNA LFT hoje (série 11 — desde 01/07/2000)...")
    dados = _get_bacen_historico_desde(11, "01/07/2000")

    if len(dados) < 100:
        log.warning(f"  VNA LFT: {len(dados)} registros — fallback R$18.500")
        return 18_500.0

    log.info(f"  VNA LFT: {len(dados)} dias ({dados[0]['data']} → {dados[-1]['data']})")

    vna = 1_000.0
    for d in dados:
        try:
            vna *= (1 + float(d["valor"].replace(",", ".")) / 100)
        except (ValueError, KeyError):
            continue

    if not (10_000 < vna < 30_000):
        log.warning(f"  VNA LFT fora do esperado ({vna:.2f}) — fallback R$18.500")
        return 18_500.0

    log.info(f"  VNA LFT hoje: R${vna:,.4f}")
    return vna


def calcular_vna_lft_na_data(data_ref: date) -> float:
    """
    VNA LFT em uma data histórica específica.

    Necessário para calcular o PU Compra correto da LFT:
        PU Compra = VNA(data_compra) / (1 + spread)^(DU_na_data/252)

    Se usarmos VNA(hoje) para o PU Compra, perdemos o accrual diário
    da Selic entre a data de compra e hoje — que é exatamente o
    rendimento que a LFT deveria gerar. O P&L ficaria artificialmente
    negativo mesmo sem variação de spread.

    Funcionamento: acumula a série 11 só até a data_ref,
    excluindo os fatores posteriores.
    """
    log.info(f"  Calculando VNA LFT em {data_ref.strftime('%d/%m/%Y')}...")
    dados = _get_bacen_historico_desde(11, "01/07/2000")

    if len(dados) < 100:
        log.warning("  VNA LFT na data: fallback R$18.400")
        return 18_400.0

    data_ref_str = data_ref.strftime("%d/%m/%Y")
    vna = 1_000.0
    for d in dados:
        try:
            data_reg = datetime.strptime(d["data"], "%d/%m/%Y").date()
            if data_reg > data_ref:
                break   # para de acumular após a data de referência
            vna *= (1 + float(d["valor"].replace(",", ".")) / 100)
        except (ValueError, KeyError):
            continue

    if not (10_000 < vna < 30_000):
        log.warning(f"  VNA LFT na data fora do esperado ({vna:.2f}) — fallback R$18.400")
        return 18_400.0

    log.info(f"  VNA LFT em {data_ref_str}: R${vna:,.4f}")
    return vna


def calcular_vna_ntnb_na_data(data_ref: date) -> float:
    """
    VNA NTN-B em uma data histórica específica (convenção ANBIMA).

    Necessário para calcular o PU Compra correto da NTN-B:
        PU Compra = Σ [Cupom(VNA_compra) / (1+taxa)^(DU_i/252)]
                  + VNA_compra / (1+taxa)^(DU_n/252)

    Mesma lógica de corte de `calcular_vna_ntnb`:
      1. Acumula IPCAs completos até o último aniversário <= data_ref
      2. Aplica o IPCA do período corrente via pro-rata (DU ANBIMA)
    """
    log.info(f"  Calculando VNA NTN-B em {data_ref.strftime('%d/%m/%Y')}...")
    dados = _get_bacen_historico_desde(433, "01/07/2000")

    if len(dados) < 50:
        log.warning("  VNA NTN-B na data: fallback R$4.500")
        return 4_500.0

    # Último aniversário em ou antes de data_ref
    aniv_ant  = _ultimo_aniversario_ntnb(data_ref)
    cutoff    = date(aniv_ant.year, aniv_ant.month, 1)
    aniv_prox = _aniversario_ntnb(
        aniv_ant.year if aniv_ant.month < 12 else aniv_ant.year + 1,
        aniv_ant.month + 1 if aniv_ant.month < 12 else 1)

    vna = 1_000.0
    ipca_prorata        = None
    ipca_ultimo_acum    = None
    for d in dados:
        try:
            data_reg = datetime.strptime(d["data"], "%d/%m/%Y").date()
            if data_reg > data_ref:
                break
            ipca_val = float(d["valor"].replace(",", ".")) / 100
            if data_reg >= cutoff:
                # IPCA do período corrente → reserva para pro-rata, não acumula
                ipca_prorata = ipca_val
                break
            vna *= 1 + ipca_val
            ipca_ultimo_acum = ipca_val
        except (ValueError, KeyError):
            continue

    # Aplica pro-rata por dias úteis (ANBIMA): aniv_ant → data_ref
    ipca_per = ipca_prorata if ipca_prorata is not None else ipca_ultimo_acum
    if ipca_per is not None:
        du_dec = cal.bizdays(aniv_ant, data_ref)
        du_tot = cal.bizdays(aniv_ant, aniv_prox)
        if du_tot > 0:
            vna *= (1 + ipca_per) ** (du_dec / du_tot)

    if not (3_000 < vna < 7_000):
        log.warning(f"  VNA NTN-B na data fora do esperado ({vna:.2f}) — fallback R$4.500")
        return 4_500.0

    log.info(f"  VNA NTN-B em {data_ref.strftime('%d/%m/%Y')}: R${vna:,.4f}")
    return vna


# ─────────────────────────────────────────────────────────────────────────────
# 7. VNA NTN-B — acumulação do IPCA desde 01/07/2000
# ─────────────────────────────────────────────────────────────────────────────

def _aniversario_ntnb(ano: int, mes: int) -> date:
    """Dia 15 do mês ajustado para o próximo dia útil (aniversário NTN-B)."""
    d = date(ano, mes, 15)
    while not cal.isbizday(d):
        d += timedelta(days=1)
    return d


def _ultimo_aniversario_ntnb(data_ref: date) -> date:
    """
    Retorna o último aniversário NTN-B (dia 15 ajustado) em ou antes de data_ref.
    Necessário para determinar o corte correto do loop de acumulação de IPCA.
    """
    y, m = data_ref.year, data_ref.month
    aniv_corrente = _aniversario_ntnb(y, m)
    if aniv_corrente <= data_ref:
        return aniv_corrente
    # O 15 do mês corrente ainda não chegou → usa o do mês anterior
    return _aniversario_ntnb(y if m > 1 else y - 1, m - 1 if m > 1 else 12)


def _fator_ipca_pro_rata() -> float:
    """
    Pro-rata do IPCA para o período corrente entre aniversários NTN-B.
    Convenção ANBIMA: (1 + IPCA_período)^(DU_aniv_ant→hoje / DU_aniv_ant→aniv_prox)

    IMPORTANTE: chamado apenas APÓS o loop de `calcular_vna_ntnb`, que para
    ANTES do IPCA do período corrente. Assim o mesmo IPCA não é duplamente
    aplicado (uma vez inteiro no loop + uma vez parcial aqui).
    """
    hoje  = date.today()
    ano_ini = hoje.year if hoje.month > 3 else hoje.year - 1
    mes_ini = (hoje.month - 3) % 12 + 1
    dados   = _get_bacen_periodo(
        433,
        f"01/{mes_ini:02d}/{ano_ini}",
        hoje.strftime("%d/%m/%Y")
    )
    if not dados or len(dados) < 1:
        return 1.0
    try:
        ultimo   = dados[-1]
        data_ult = datetime.strptime(ultimo["data"], "%d/%m/%Y").date()
        ipca_ult = float(ultimo["valor"].replace(",", ".")) / 100

        # Se o IPCA do mês corrente já foi publicado, aplica completo
        if data_ult.month == hoje.month and data_ult.year == hoje.year:
            return 1 + ipca_ult

        y, m      = hoje.year, hoje.month
        aniv_ant  = _ultimo_aniversario_ntnb(hoje)
        aniv_prox = _aniversario_ntnb(
            y if m < 12 else y + 1,
            m + 1 if m < 12 else 1)

        # Pro-rata por dias úteis (convenção ANBIMA)
        du_decorridos = cal.bizdays(aniv_ant, hoje)
        du_total      = cal.bizdays(aniv_ant, aniv_prox)

        fator = (1 + ipca_ult) ** (du_decorridos / du_total) if du_total > 0 else 1.0
        log.info(f"  Fator IPCA pro-rata ({du_decorridos}/{du_total} DU): {fator:.8f}")
        return fator
    except Exception as e:
        log.warning(f"  Fator IPCA pro-rata falhou: {e}")
        return 1.0


def calcular_vna_ntnb() -> float:
    """
    VNA NTN-B = R$1.000 × Π(IPCAs completos até último aniversário) × fator_pro_rata

    O loop acumula apenas os IPCAs que já foram integralmente incorporados ao VNA
    (aplicados em aniversários passados). O IPCA do período corrente é aplicado
    somente via fator_pro_rata, evitando dupla contagem.

    Exemplo (hoje = 10/03/2026):
      - aniv_ant = 15/02/2026 → cutoff = 01/02/2026
      - Loop acumula: jul/2000 ... jan/2026  (para antes de fev/2026)
      - Pro-rata aplica fev/2026 de 15/02 a 10/03  ✓

    Início fixo em 01/07/2000 evita incluir dados de hiperinflação.
    Valor esperado em 03/2026: ~R$4.500–4.700
    """
    log.info("  Calculando VNA NTN-B (IPCA acumulado desde 01/07/2000)...")
    dados = _get_bacen_historico_desde(433, "01/07/2000")

    if len(dados) < 50:
        log.warning(f"  VNA NTN-B: {len(dados)} registros insuficientes — fallback R$4.600")
        return 4_600.0

    log.info(f"  VNA NTN-B: {len(dados)} meses ({dados[0]['data']} → {dados[-1]['data']})")

    # Corte: acumula apenas IPCAs cujo aniversário de aplicação já passou
    # O IPCA de mês M é aplicado no aniversário do mês M+1
    # Para aniv_ant = Feb 15: include até jan/2026 (data_reg < Feb 1)
    # Para aniv_ant = Mar 15: include até fev/2026 (data_reg < Mar 1)
    aniv_ant = _ultimo_aniversario_ntnb(date.today())
    cutoff   = date(aniv_ant.year, aniv_ant.month, 1)

    vna = 1_000.0
    for d in dados:
        try:
            data_reg = datetime.strptime(d["data"], "%d/%m/%Y").date()
            if data_reg >= cutoff:
                break   # IPCA do período corrente: tratado pelo fator_pro_rata
            vna *= 1 + float(d["valor"].replace(",", ".")) / 100
        except (ValueError, KeyError):
            continue

    vna *= _fator_ipca_pro_rata()

    if not (3_000 < vna < 7_000):
        log.warning(f"  VNA NTN-B fora do esperado ({vna:.2f}) — fallback R$4.600")
        return 4_600.0

    log.info(f"  VNA NTN-B final: R${vna:,.4f}")
    return vna


# ─────────────────────────────────────────────────────────────────────────────
# 8. COLETA DE TAXAS DE MERCADO
#
#    Fontes testadas e resultado (16/03/2026):
#
#    Opção 1 — JSON B3/Tesouro Direto
#        URL: tesourodireto.com.br/.../treasurybondsresults.json
#        Resultado: 403 Cloudflare (bloqueado por bot-detection) — INDISPONÍVEL
#
#    Opção 2 — CSV histórico CDN Tesouro
#        URL: cdn.tesouro.gov.br/.../historicos/LTN.csv
#        Resultado: 404 — endpoint não existe mais — INDISPONÍVEL
#
#    Opção 3 — ANBIMA página pública
#        URL: anbima.com.br/pt_br/informar/taxas-de-titulos-publicos.htm
#        Resultado: conteúdo carregado via JavaScript — requests não consegue
#        extrair tabelas, BeautifulSoup retorna apenas shell vazio — INDISPONÍVEL
#
#    Opção 4 — API Tesouro Nacional
#        URL: api.tesouronacional.gov.br/titulos
#        Resultado: DNS não resolve — domínio inexistente — INDISPONÍVEL
#
#    Fonte FUNCIONAL encontrada — SGS BACEN série 226:
#        URL: api.bcb.gov.br/dados/serie/bcdata.sgs.226/dados/ultimos/1
#        Retorna a taxa LTN % a.a. do último dia útil (ex: 16.93% em 13/03/2026)
#        Confirmado via comparação histórica: dez/2021 = 12.59% aa (correto),
#        jun/2015 = 16.67% aa (correto). Atualização diária, sem autenticação.
#        Cobre LTN independente de vencimento (taxa única de mercado, pré-fixada).
#
#    Séries SGS descartadas após teste:
#        227 → fator mensal do cupom NTN-F (1+10%)^(1/12)-1, não yield de negociação
#        228 → dados de 1994, série obsoleta para NTN-B
#        1190 → bad request 400, série indisponível
#
#    NTN-F, NTN-B e LFT: nenhuma fonte pública acessível retorna yield
#    por vencimento específico sem autenticação. Fallback taxa_compra + 2bp
#    é honesto e funcional — a variação do P&L de NTN-B e LFT é real
#    pois os VNAs (acumulação IPCA/Selic) são calculados com precisão.
# ─────────────────────────────────────────────────────────────────────────────

def _coletar_taxa_ltn_sgs226() -> float | None:
    """
    Busca a taxa LTN de mercado via SGS BACEN série 226.
    Retorna taxa em decimal a.a. (ex: 0.1693) ou None em caso de falha.
    """
    url = f"{_BACEN_BASE.format(serie=226)}/ultimos/1?formato=json"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        valor = float(r.json()[-1]["valor"].replace(",", "."))
        # Sanidade: taxa LTN deve estar entre 5% e 40% a.a.
        if 0.05 <= valor <= 0.40:
            return valor
        log.warning(f"  SGS 226 valor fora do esperado: {valor} — ignorado")
        return None
    except Exception as e:
        log.warning(f"  SGS 226 (taxa LTN): {e}")
        return None


def buscar_taxas_anbima_historico(data: date) -> dict:
    """
    Busca taxas indicativas ANBIMA (Tx. Indicativas, índice 7) para uma data.

    Tenta o arquivo da data fornecida; se 404 (fim de semana / feriado),
    recua até 5 dias consecutivos para encontrar o último pregão disponível.

    URL pattern: https://www.anbima.com.br/informacoes/merc-sec/arqs/msYYMMDD.txt
    Formato    : delimitado por '@', encoding latin-1.
    Colunas    : Titulo @ Data Ref @ Cod SELIC @ Emissao @ Vencimento @
                 Tx.Compra @ Tx.Venda @ Tx.Indicativas @ PU @ ...

    Retorna {nome_titulo: taxa_decimal}  — ex: {"LTN 01/07/2026": 0.142359}
    Retorna {} se nenhum arquivo disponível (fallback para taxa_compra na chamada).
    """
    mapa = {}
    for nome, cfg in CARTEIRA_CONFIG.items():
        venc_date = date.fromisoformat(cfg["venc"])
        mapa[(cfg["tipo"], venc_date.strftime("%Y%m%d"))] = nome

    for delta in range(6):          # tenta a data e até 5 dias anteriores
        d     = data - timedelta(days=delta)
        fname = f"ms{d.strftime('%y%m%d')}.txt"
        url   = f"https://www.anbima.com.br/informacoes/merc-sec/arqs/{fname}"
        try:
            r = requests.get(url, timeout=15)
            if r.status_code != 200:
                log.debug(f"  ANBIMA {fname}: HTTP {r.status_code}")
                continue
            content = r.content.decode("latin-1")
            taxas   = {}
            for linha in content.splitlines():
                partes = linha.split("@")
                if len(partes) < 9:
                    continue
                chave = (partes[0].strip(), partes[4].strip())
                if chave not in mapa:
                    continue
                try:
                    taxas[mapa[chave]] = float(partes[7].replace(",", ".")) / 100
                except (ValueError, IndexError):
                    continue
            if taxas:
                log.info(
                    f"  ANBIMA {fname} ({d.strftime('%d/%m/%Y')}): "
                    f"{len(taxas)}/{len(CARTEIRA_CONFIG)} títulos"
                )
                for nome_t, taxa_t in taxas.items():
                    log.info(f"    {nome_t:22s}: {taxa_t:.4%} a.a.")
                return taxas
        except Exception as e:
            log.warning(f"  ANBIMA {fname}: {e}")
            continue

    log.warning(f"  ANBIMA histórico: nenhum arquivo disponível para {data} (±5 dias)")
    return {}


def coletar_taxas_mercado() -> dict:
    """
    Coleta taxas de mercado para os títulos da carteira.

    Hierarquia:
        1. ANBIMA arquivo de HOJE (Tx. Indicativas — todos os títulos)
        2. SGS BACEN série 226 (LTN apenas — fallback se ANBIMA indisponível)
        3. None → força taxa_compra + 2bp em calcular_carteira

    Retorna dict: {nome_titulo: {"taxa": float, "fonte": str} | None}
        None força o fallback em calcular_carteira.
    """
    taxas: dict = {}

    # 1. Tenta ANBIMA hoje (cobre todos os tipos de uma vez)
    taxas_anbima = buscar_taxas_anbima_historico(date.today())
    if taxas_anbima:
        log.info(f"  Taxas de mercado: ANBIMA hoje ({len(taxas_anbima)} títulos)")
        for nome in CARTEIRA_CONFIG:
            taxas[nome] = ({"taxa": taxas_anbima[nome], "fonte": "ANBIMA"}
                           if nome in taxas_anbima else None)
        return taxas

    # 2. Fallback: SGS 226 para LTN; None para demais
    taxa_ltn = _coletar_taxa_ltn_sgs226()
    if taxa_ltn is not None:
        log.info(f"  SGS 226 (LTN): taxa de mercado = {taxa_ltn:.4%} a.a.")
    else:
        log.info("  SGS 226 (LTN): indisponível — fallback taxa_compra + 2bp")

    for nome, cfg in CARTEIRA_CONFIG.items():
        tipo = cfg.get("tipo", "")
        if tipo == "LTN" and taxa_ltn is not None:
            taxas[nome] = {"taxa": taxa_ltn, "fonte": "SGS-226"}
        else:
            taxas[nome] = None  # força fallback em calcular_carteira

    return taxas


# ─────────────────────────────────────────────────────────────────────────────
# 9. COLETA MACRO
# ─────────────────────────────────────────────────────────────────────────────

def coletar_macro() -> dict:
    """
    Coleta todos os indicadores macro via BACEN SGS.
    VNAs calculados com método de acumulação histórica em chunks.
    """
    log.info("Coletando indicadores macro — BACEN SGS...")
    selic    = _get_bacen_ultimo(432)
    ipca_12m = _get_bacen_ultimo(13522)
    vna_lft  = calcular_vna_lft()
    vna_ntnb = calcular_vna_ntnb()
    # VNAs na data de compra — necessários para PU Compra correto
    vna_lft_compra  = calcular_vna_lft_na_data(DATA_COMPRA)
    vna_ntnb_compra = calcular_vna_ntnb_na_data(DATA_COMPRA)

    macro = {
        "selic_meta"         : selic    / 100 if selic    else 0.1500,
        "ipca_12m"           : ipca_12m / 100 if ipca_12m else 0.0483,
        "vna_lft"            : vna_lft,
        "vna_lft_compra"     : vna_lft_compra,
        "vna_ntnb"           : vna_ntnb,
        "vna_ntnb_compra"    : vna_ntnb_compra,
        "ultima_atualizacao" : datetime.now().strftime("%d/%m/%Y %H:%M"),
    }
    log.info(
        f"  Selic={macro['selic_meta']:.2%} | "
        f"IPCA 12m={macro['ipca_12m']:.2%} | "
        f"VNA LFT hoje=R${macro['vna_lft']:,.2f} | "
        f"VNA LFT compra=R${macro['vna_lft_compra']:,.2f} | "
        f"VNA NTN-B hoje=R${macro['vna_ntnb']:,.2f} | "
        f"VNA NTN-B compra=R${macro['vna_ntnb_compra']:,.2f}"
    )
    return macro

# ─────────────────────────────────────────────────────────────────────────────
# 10. CÁLCULOS DE RENDA FIXA
# ─────────────────────────────────────────────────────────────────────────────

def _fluxos_semestrais(hoje: date, venc: date,
                        cupom: float, principal: float) -> list[tuple]:
    """
    Gera fluxos de caixa semestrais retroagindo do vencimento.
    Datas ajustadas para o próximo dia útil conforme convenção ANBIMA.
    """
    datas, dt = [], venc
    while dt > hoje:
        datas.append(dt)
        mes, ano = dt.month - 6, dt.year
        if mes <= 0:
            mes += 12
            ano -= 1
        try:
            dt = dt.replace(year=ano, month=mes)
        except ValueError:
            dt = dt.replace(year=ano, month=mes, day=28)
    datas.sort()
    fluxos = []
    for i, d in enumerate(datas):
        # Ajusta para próximo dia útil se cair em feriado/fim de semana
        d_adj = d
        while not cal.isbizday(d_adj):
            d_adj += timedelta(days=1)
        du = dias_uteis(hoje, d_adj)
        if du <= 0:
            continue
        fc = cupom + principal if i == len(datas) - 1 else cupom
        fluxos.append((fc, du))
    return fluxos


def _pu_ltn(taxa: float, du: int) -> float:
    return 1_000 / (1 + taxa) ** (du / 252)


def _pu_ntnf(taxa: float, hoje: date, venc: date) -> float:
    cupom  = 1_000 * ((1.10 ** 0.5) - 1)
    fluxos = _fluxos_semestrais(hoje, venc, cupom, 1_000)
    return sum(fc / (1 + taxa) ** (du / 252) for fc, du in fluxos)


def _pu_ntnb(taxa: float, hoje: date, venc: date, vna: float) -> float:
    cupom  = vna * ((1.06 ** 0.5) - 1)
    fluxos = _fluxos_semestrais(hoje, venc, cupom, vna)
    return sum(fc / (1 + taxa) ** (du / 252) for fc, du in fluxos)


def _pu_lft(spread: float, du: int, vna: float) -> float:
    return vna / (1 + spread) ** (du / 252)


def _duration(taxa: float, fluxos: list, pu: float) -> float:
    if pu <= 0: return 0.0
    return sum((du / 252) * fc / (1 + taxa) ** (du / 252)
               for fc, du in fluxos) / pu


def _dv01(pu: float, dur: float, taxa: float) -> float:
    if pu <= 0 or dur <= 0: return 0.0
    return pu * dur / (1 + taxa) / 10_000


def _calcular_titulo(tipo: str, taxa: float, du: int,
                     ref: date, venc: date,
                     vna_lft: float, vna_ntnb: float) -> tuple:
    """Retorna (PU, Duration, DV01) para um título."""
    if tipo == "LTN":
        pu  = _pu_ltn(taxa, du)
        dur = du / 252
        dv  = _dv01(pu, dur, taxa)

    elif tipo == "NTN-F":
        pu     = _pu_ntnf(taxa, ref, venc)
        fluxos = _fluxos_semestrais(ref, venc,
                                     1_000 * ((1.10**0.5) - 1), 1_000)
        dur    = _duration(taxa, fluxos, pu)
        dv     = _dv01(pu, dur, taxa)

    elif tipo == "NTN-B":
        pu     = _pu_ntnb(taxa, ref, venc, vna_ntnb)
        cupom  = vna_ntnb * ((1.06**0.5) - 1)
        fluxos = _fluxos_semestrais(ref, venc, cupom, vna_ntnb)
        dur    = _duration(taxa, fluxos, pu)
        dv     = _dv01(pu, dur, taxa)

    elif tipo == "LFT":
        pu  = _pu_lft(taxa, du, vna_lft)
        # LFT: spread DV01 — sensibilidade do PU a 1bp de variação no spread.
        # PU = VNA / (1 + spread)^(DU/252)
        # → dPU/d(spread) = -PU × (DU/252) / (1 + spread)
        # → Spread DV01 = PU × (DU/252) / (1 + spread) / 10.000
        #
        # _dv01 computa: pu × dur / (1 + taxa) / 10.000
        # Passando dur = DU/252 (Macaulay), o resultado é correto:
        #   pu × (DU/252) / (1 + spread) / 10.000  ✓
        # Usar dur = (DU/252)/(1+spread) causaria dupla divisão — ERRADO.
        dur = du / 252   # Macaulay Duration (consistente com LTN/NTN-F/NTN-B)
        dv  = _dv01(pu, dur, taxa)

    else:
        pu, dur, dv = 0.0, 0.0, 0.0

    return pu, dur, dv

# ─────────────────────────────────────────────────────────────────────────────
# 11. LER CARTEIRA DO EXCEL
# ─────────────────────────────────────────────────────────────────────────────

def ler_carteira(wb) -> list[dict]:
    ws       = wb.sheets["CARTEIRA RF"]
    carteira = []
    for row in range(3, 10):
        titulo      = ws.range(f"A{row}").value
        indexador   = ws.range(f"B{row}").value
        venc_raw    = ws.range(f"C{row}").value
        taxa_compra = ws.range(f"F{row}").value
        if not titulo: continue
        qtd = QUANTIDADES.get(titulo.strip(), int(ws.range(f"E{row}").value or 1_000))
        if isinstance(venc_raw, datetime): venc = venc_raw.date()
        else: venc = datetime.strptime(str(venc_raw), "%d/%m/%Y").date()
        carteira.append({
            "titulo"      : titulo.strip(),
            "indexador"   : indexador,
            "vencimento"  : venc,
            "qtd"         : int(qtd),
            "taxa_compra" : float(taxa_compra),
            "row"         : row,
        })
    log.info(f"Carteira lida: {len(carteira)} títulos")
    return carteira

# ─────────────────────────────────────────────────────────────────────────────
# 12. CALCULAR MÉTRICAS DA CARTEIRA
# ─────────────────────────────────────────────────────────────────────────────

def calcular_carteira(carteira: list, taxas_merc: dict,
                       macro: dict,
                       taxas_historicas: dict = None) -> list[dict]:
    """
    PU Compra LTN/NTN-F → taxa ANBIMA histórica de DATA_COMPRA (Tx. Indicativas)
                           Fallback: taxa_compra da planilha, se ANBIMA indisponível
    PU Compra LFT       → idem, usa VNA_LFT(DATA_COMPRA)
    PU Compra NTN-B     → idem, usa VNA_NTN-B(DATA_COMPRA)
    PU Mercado / Duration / DV01 → taxa_mercado + DU(hoje) + VNA(hoje)

    taxas_historicas: dict {nome_titulo: taxa_decimal} retornado por
        buscar_taxas_anbima_historico(DATA_COMPRA). Se None ou vazio,
        mantém comportamento anterior (taxa_compra da planilha).
    """
    hoje         = date.today()
    vna_lft      = macro["vna_lft"]
    vna_lft_cmp  = macro.get("vna_lft_compra",  vna_lft)
    vna_ntnb     = macro["vna_ntnb"]
    vna_ntnb_cmp = macro.get("vna_ntnb_compra", vna_ntnb)
    resultado    = []

    for t in carteira:
        cfg  = CARTEIRA_CONFIG.get(t["titulo"], {})
        tipo = cfg.get("tipo", "")

        du_hoje   = dias_uteis(hoje, t["vencimento"])
        du_compra = dias_uteis(DATA_COMPRA, t["vencimento"])

        # Determina taxa de mercado ANTES dos cálculos —
        # usada para PU Mercado, Duration e DV01 (risco corrente)
        dados_api = taxas_merc.get(t["titulo"])
        if dados_api and dados_api["taxa"] is not None:
            taxa_merc = dados_api["taxa"]
        else:
            log.warning(f"  {t['titulo']}: fallback taxa_compra + 2bp")
            taxa_merc = t["taxa_compra"] + 0.0002

        # PU Compra: taxa real ANBIMA do dia da compra → fallback taxa da planilha
        taxa_real_compra = (taxas_historicas or {}).get(t["titulo"], t["taxa_compra"])
        if taxas_historicas and t["titulo"] in taxas_historicas:
            delta_pp = (taxa_real_compra - t["taxa_compra"]) * 100
            log.info(
                f"  {t['titulo']:22s} | taxa ANBIMA {DATA_COMPRA}: "
                f"{taxa_real_compra:.4%}  (planilha: {t['taxa_compra']:.4%}, "
                f"delta: {delta_pp:+.4f} pp)"
            )
        else:
            log.info(f"  {t['titulo']:22s} | taxa ANBIMA indisponível — usando planilha: "
                     f"{t['taxa_compra']:.4%}")

        # PU Compra: usa VNA da data de compra para LFT e NTN-B
        vna_comp_lft  = vna_lft_cmp  if tipo == "LFT"   else vna_lft
        vna_comp_ntnb = vna_ntnb_cmp if tipo == "NTN-B" else vna_ntnb
        pu_comp, _, _ = _calcular_titulo(
            tipo, taxa_real_compra, du_compra,
            DATA_COMPRA, t["vencimento"],
            vna_comp_lft, vna_comp_ntnb)

        # PU Mercado, Duration e DV01: taxa de mercado + VNA de hoje
        pu_merc, dur, dv = _calcular_titulo(
            tipo, taxa_merc, du_hoje,
            hoje, t["vencimento"], vna_lft, vna_ntnb)

        resultado.append({
            **t,
            "du"           : du_hoje,
            "tipo_bacen"   : tipo,     # usado para filtrar LFT no DV01 total
            "taxa_mercado" : taxa_merc,
            "pu_compra"    : pu_comp,
            "pu_mercado"   : pu_merc,
            "duration"     : dur,
            "dv01"         : dv,
        })
        log.info(
            f"  {t['titulo']:22s} | DU={du_hoje:4d} | "
            f"PU Cmp=R${pu_comp:>12,.4f} | "
            f"PU Mkt=R${pu_merc:>12,.4f} | "
            f"Dur={dur:.4f} | DV01=R${dv:.4f}"
        )
    return resultado

# ─────────────────────────────────────────────────────────────────────────────
# 13. ESCREVER NO EXCEL
# ─────────────────────────────────────────────────────────────────────────────

def escrever_excel(wb, resultado: list, macro: dict,
                    pl_total: float, pl_diario: float):
    """
    Escreve todos os dados calculados no Excel.

    pl_total  = P&L acumulado desde DATA_COMPRA (07/03/2026)
    pl_diario = variação do Valor de Mercado em relação ao dia anterior
                (lido do historico.csv — None se for o primeiro dia)
    """
    log.info("Escrevendo dados no Excel...")
    ws1 = wb.sheets["CARTEIRA RF"]
    ws2 = wb.sheets["CONTROLE CARTEIRA"]
    ws3 = wb.sheets["PARÂMETROS"]

    # ── Aba CARTEIRA RF ──────────────────────────────────────────────────────
    for t in resultado:
        r = t["row"]
        ws1.range(f"D{r}").value = t["du"]
        ws1.range(f"E{r}").value = t["qtd"]
        ws1.range(f"G{r}").value = t["taxa_mercado"]
        ws1.range(f"H{r}").value = t["pu_compra"]
        ws1.range(f"I{r}").value = t["pu_mercado"]
        ws1.range(f"N{r}").value = t["duration"]
        ws1.range(f"O{r}").value = t["dv01"]

    # ── Aba CONTROLE CARTEIRA — cards de KPI escritos diretamente ────────────
    # Calcula métricas agregadas
    # DV01 do card exclui LFT (spread DV01 ≠ taxa DV01 — não comparáveis)
    valor_mercado  = sum(t["pu_mercado"] * t["qtd"] for t in resultado)
    dv01_total     = sum(t["dv01"] * t["qtd"] for t in resultado
                         if t.get("tipo_bacen", "") != "LFT")
    # Duration média exclui LFT (duration de spread ≠ duration de taxa)
    resultado_nao_lft = [t for t in resultado if t.get("tipo_bacen", "") != "LFT"]
    vm_nao_lft = sum(t["pu_mercado"] * t["qtd"] for t in resultado_nao_lft)
    dur_media  = (
        sum(t["duration"] * t["pu_mercado"] * t["qtd"] for t in resultado_nao_lft)
        / vm_nao_lft if vm_nao_lft > 0 else 0
    )

    # Cards — escritos diretamente pelo Python
    ws2.range("C4").value = pl_total        # P&L Total (desde 07/03)
    ws2.range("E4").value = valor_mercado   # Valor de Mercado
    ws2.range("G4").value = dv01_total      # DV01 da Carteira (ex-LFT)
    ws2.range("I4").value = dur_media       # Duration Média (ex-LFT)

    # Formata células DV01 e Duration das LFTs como informativas (itálico, cinza)
    # para deixar claro que são spread DV01, não taxa DV01
    for t in resultado:
        cfg = CARTEIRA_CONFIG.get(t["titulo"], {})
        if cfg.get("tipo") == "LFT":
            r = t["row"]
            for col in ["N", "O", "P"]:  # Duration, DV01, DV01 Total
                ws1.range(f"{col}{r}").api.Font.Italic = True
                ws1.range(f"{col}{r}").api.Font.Color  = 0x888888

    # P&L do Dia — linha 7 col C
    pl_dia_val = pl_diario if pl_diario is not None else 0.0
    ws2.range("C7").value          = pl_dia_val
    ws2.range("C7").number_format  = 'R$ #,##0.00;-R$ #,##0.00;"—"'
    ws2.range("C7").api.Font.Bold  = True
    ws2.range("C7").api.Font.Size  = 16
    cor = 0x2D6A4F if pl_dia_val >= 0 else 0xC0392B
    ws2.range("C7").api.Font.Color = cor
    if pl_diario is None:
        ws2.range("C7").api.Font.Color = 0x888888

    # ── Aba PARÂMETROS ────────────────────────────────────────────────────────
    ws3.range("B4").value = macro["selic_meta"]
    ws3.range("B5").value = macro["ipca_12m"]
    ws3.range("B6").value = macro["vna_lft"]
    ws3.range("B7").value = macro["ultima_atualizacao"]
    ws3.range("B8").value = DATA_COMPRA.strftime("%d/%m/%Y")

    for i, t in enumerate(resultado):
        row = 24 + i
        ws3.range(f"B{row}").value = t["taxa_mercado"]
        ws3.range(f"C{row}").value = t["pu_mercado"]
        ws3.range(f"D{row}").value = macro["ultima_atualizacao"]

    # ── Nota de rodapé — CARTEIRA RF ─────────────────────────────────────────
    nota = (
        "* Compra fictícia em 07/03/2026 com quantidades proporcionais ao PU "
        "(LTN/NTN-F: 1.000u · NTN-B: 200u · LFT: 50u). "
        "Altere QUANTIDADES no código para customizar."
    )
    ws1.range("A11").value = nota
    ws1.range("A11").api.Font.Italic = True
    ws1.range("A11").api.Font.Size   = 9
    ws1.range("A11").api.Font.Color  = 0x888888   # cinza discreto

    log.info(f"Excel atualizado | P&L Total={pl_total:+,.2f} | "
             f"P&L Dia={pl_dia_val:+,.2f} | Duration={dur_media:.4f}")

# ─────────────────────────────────────────────────────────────────────────────
# 14. GRÁFICOS MATPLOTLIB → colados no Excel
# ─────────────────────────────────────────────────────────────────────────────

plt.rcParams.update({
    "figure.facecolor": _BG, "axes.facecolor": _BG,
    "axes.edgecolor": "#ccccdd", "axes.labelcolor": _SUBTEXT,
    "xtick.color": _SUBTEXT, "ytick.color": _SUBTEXT,
    "text.color": _AZUL_ESC, "grid.color": "#e0e0ee",
    "grid.linestyle": "--", "grid.alpha": 0.6,
    "font.family": "serif",
    "axes.spines.top": False, "axes.spines.right": False,
})


def _graf_pl(resultado: list) -> str:
    labels = [t["titulo"].replace(" ", "\n", 1) for t in resultado]
    vals   = [(t["pu_mercado"] - t["pu_compra"]) * t["qtd"] for t in resultado]
    cores  = [_VERDE if v >= 0 else _VERMELHO for v in vals]
    fig, ax = plt.subplots(figsize=(8.5, 4), facecolor=_BG)
    fig.subplots_adjust(left=0.23, right=0.96, top=0.87, bottom=0.10)
    bars = ax.barh(labels, vals, color=cores, edgecolor="white", height=0.6)
    for bar, v in zip(bars, vals):
        off = abs(v) * 0.02
        ha  = "left" if v >= 0 else "right"
        ax.text(v + (off if v >= 0 else -off),
                bar.get_y() + bar.get_height() / 2,
                f"R$ {v:+,.0f}", va="center", ha=ha,
                fontsize=8, fontweight="bold", color=_AZUL_ESC)
    ax.axvline(0, color=_SUBTEXT, linewidth=0.8)
    ax.set_title("P&L por Título (R$)", fontsize=11,
                 fontweight="bold", color=_AZUL_ESC, pad=10)
    ax.set_xlabel("P&L (R$)", fontsize=9, color=_SUBTEXT)
    ax.grid(True, axis="x", alpha=0.5)
    fig.text(0.96, 0.02, f"Gerado: {datetime.now().strftime('%d/%m/%Y %H:%M')}",
             ha="right", fontsize=7, color=_SUBTEXT, fontstyle="italic")
    path = os.path.abspath("grafico_pl.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=_BG)
    plt.close(fig)
    return path


def _graf_dv01(resultado: list) -> str:
    # Apenas Pré e IPCA+ — LFT excluída pois seu spread DV01 não é
    # comparável ao DV01 de taxa nominal/real dos demais títulos.
    exp = {"Pré": 0.0, "IPCA+": 0.0}
    for t in resultado:
        if t["indexador"] in exp:
            exp[t["indexador"]] += t["dv01"] * t["qtd"]
    cores_idx = {"Pré": _AZUL_ESC, "IPCA+": _AZUL_CLAR}
    nomes = list(exp.keys())
    vals  = list(exp.values())
    fig, ax = plt.subplots(figsize=(6, 3.8), facecolor=_BG)
    fig.subplots_adjust(left=0.15, right=0.97, top=0.82, bottom=0.13)
    bars = ax.bar(nomes, vals, color=[cores_idx[n] for n in nomes],
                  edgecolor="white", width=0.5)
    for bar, v in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2, v * 1.02,
                f"R$ {v:,.0f}", ha="center", va="bottom",
                fontsize=9, fontweight="bold", color=_AZUL_ESC)
    ax.set_title("DV01 por Indexador (R$)", fontsize=11,
                 fontweight="bold", color=_AZUL_ESC, pad=6)
    ax.set_ylabel("DV01 Total (R$)", fontsize=9, color=_SUBTEXT)
    ax.grid(True, axis="y", alpha=0.5)
    fig.text(0.5, 0.94, "LFT excluída — spread DV01 não comparável a DV01 de taxa",
             ha="center", fontsize=7, color=_SUBTEXT, fontstyle="italic",
             transform=fig.transFigure)
    fig.text(0.97, 0.02, f"Gerado: {datetime.now().strftime('%d/%m/%Y %H:%M')}",
             ha="right", fontsize=7, color=_SUBTEXT, fontstyle="italic")
    path = os.path.abspath("grafico_dv01.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=_BG)
    plt.close(fig)
    return path


def _graf_historico() -> str | None:
    if not os.path.exists(HISTORICO_CSV): return None
    df = pd.read_csv(HISTORICO_CSV, parse_dates=["data"])
    if len(df) < 2: return None
    fig, ax = plt.subplots(figsize=(8.5, 3.5), facecolor=_BG)
    fig.subplots_adjust(left=0.12, right=0.97, top=0.87, bottom=0.20)
    ax.plot(df["data"], df["pl_total"], color=_AZUL_ESC, linewidth=1.5, zorder=3)
    ax.fill_between(df["data"], df["pl_total"], 0,
                    where=df["pl_total"] >= 0, alpha=0.15, color=_VERDE)
    ax.fill_between(df["data"], df["pl_total"], 0,
                    where=df["pl_total"] < 0,  alpha=0.15, color=_VERMELHO)
    ax.axhline(0, color=_SUBTEXT, linewidth=0.8, linestyle="--")
    ax.set_title("Evolução Histórica do P&L (R$)", fontsize=11,
                 fontweight="bold", color=_AZUL_ESC, pad=10)
    ax.set_ylabel("P&L (R$)", fontsize=9, color=_SUBTEXT)
    ax.grid(True, alpha=0.4)
    ax.xaxis.set_tick_params(rotation=30, labelsize=8)
    fig.text(0.97, 0.02, f"Gerado: {datetime.now().strftime('%d/%m/%Y %H:%M')}",
             ha="right", fontsize=7, color=_SUBTEXT, fontstyle="italic")
    path = os.path.abspath("grafico_historico.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=_BG)
    plt.close(fig)
    return path


def _inserir_imagem(ws, path: str, name: str, cell: str, w: int, h: int):
    if not path or not os.path.exists(path): return
    try:
        ws.pictures[name].update(path)
    except Exception:
        ws.pictures.add(path, name=name, update=True,
                        left=ws.range(cell).left, top=ws.range(cell).top,
                        width=w, height=h)


def colar_graficos(wb, p_pl: str, p_dv01: str, p_hist):
    ws2 = wb.sheets["CONTROLE CARTEIRA"]
    _inserir_imagem(ws2, p_pl,   "GraficoPL",   "A17", 500, 210)
    _inserir_imagem(ws2, p_dv01, "GraficoDV01", "G17", 340, 210)
    if p_hist:
        _inserir_imagem(ws2, p_hist, "GraficoHist", "A33", 660, 210)
    log.info("Gráficos colados no Excel.")

# ─────────────────────────────────────────────────────────────────────────────
# 15. HISTÓRICO CSV
# ─────────────────────────────────────────────────────────────────────────────

def salvar_historico(resultado: list, macro: dict) -> tuple:
    """
    Appenda snapshot diário no historico.csv.
    Retorna (pl_total, dv01_total, pl_diario).

    pl_total  = P&L acumulado desde DATA_COMPRA (PU Mkt - PU Cmp) × Qtd
    pl_diario = variação do Valor de Mercado em relação ao dia anterior
                no historico.csv. None se for a primeira execução.
    """
    pl_total      = sum((t["pu_mercado"] - t["pu_compra"]) * t["qtd"]
                         for t in resultado)
    valor_mercado = sum(t["pu_mercado"] * t["qtd"] for t in resultado)
    # DV01 exclui LFT (spread DV01 ≠ taxa DV01)
    dv01_total    = sum(t["dv01"] * t["qtd"] for t in resultado
                         if t.get("tipo_bacen", "") != "LFT")
    resultado_nao_lft = [t for t in resultado if t.get("tipo_bacen", "") != "LFT"]
    vm_nao_lft = sum(t["pu_mercado"] * t["qtd"] for t in resultado_nao_lft)
    dur_media  = (sum(t["duration"] * t["pu_mercado"] * t["qtd"]
                       for t in resultado_nao_lft)
                   / vm_nao_lft if vm_nao_lft > 0 else 0)

    # P&L diário = variação do Valor de Mercado vs. dia anterior
    pl_diario = None
    if os.path.exists(HISTORICO_CSV):
        try:
            df_hist = pd.read_csv(HISTORICO_CSV)
            if len(df_hist) >= 1:
                vm_anterior = float(df_hist.iloc[-1]["valor_mercado"])
                pl_diario   = valor_mercado - vm_anterior
        except Exception as e:
            log.warning(f"  Não foi possível calcular P&L diário: {e}")

    linha = {
        "data"           : date.today().isoformat(),
        "pl_total"       : round(pl_total,      2),
        "pl_diario"      : round(pl_diario, 2) if pl_diario is not None else 0.0,
        "valor_mercado"  : round(valor_mercado,  2),
        "dv01_carteira"  : round(dv01_total,     4),
        "duration_media" : round(dur_media,      4),
        "selic"          : macro["selic_meta"],
        "ipca_12m"       : macro["ipca_12m"],
    }

    existe = os.path.exists(HISTORICO_CSV)
    with open(HISTORICO_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=linha.keys())
        if not existe: w.writeheader()
        w.writerow(linha)

    pl_dia_str = f"{pl_diario:+,.2f}" if pl_diario is not None else "N/A"
    log.info(
        f"Histórico salvo | P&L Total={pl_total:+,.2f} | "
        f"P&L Dia={pl_dia_str} | "
        f"DV01={dv01_total:,.4f} | Duration={dur_media:.4f}"
    )
    return pl_total, dv01_total, pl_diario

# ─────────────────────────────────────────────────────────────────────────────
# 16. ALERTA E-MAIL
# ─────────────────────────────────────────────────────────────────────────────

def enviar_alerta(dv01_total: float, pl_total: float, pl_diario: float,
                  resultado: list):
    if dv01_total <= DV01_LIMITE: return
    log.warning(f"⚠️  ALERTA DV01: R${dv01_total:,.2f} > limite R${DV01_LIMITE:,.2f}")
    if not EMAIL_ATIVO:
        log.info("E-mail desativado (EMAIL_ATIVO=False).")
        return
    try:
        linhas = "".join(
            f"<tr><td>{t['titulo']}</td>"
            f"<td>R$ {t['dv01']:,.4f}</td>"
            f"<td>R$ {t['dv01'] * t['qtd']:,.2f}</td></tr>"
            for t in resultado)
        corpo = f"""<h2>⚠️ Alerta de Risco — Book de Tesouraria</h2>
        <p><b>Data:</b> {date.today().strftime('%d/%m/%Y')}</p>
        <p><b>DV01 Total:</b> R$ {dv01_total:,.2f} (limite: R$ {DV01_LIMITE:,.2f})</p>
        <p><b>P&L do Dia:</b> R$ {pl_diario:+,.2f}</p>
        <table border="1" cellpadding="5">{linhas}</table>
        <small><i>Gerado por treasury.py</i></small>"""
        msg             = MIMEMultipart("alternative")
        msg["Subject"]  = f"[ALERTA RISCO] DV01 acima do limite — {date.today()}"
        msg["From"]     = EMAIL_REMETENTE
        msg["To"]       = EMAIL_DESTINO
        msg.attach(MIMEText(corpo, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(EMAIL_REMETENTE, EMAIL_SENHA)
            s.sendmail(EMAIL_REMETENTE, EMAIL_DESTINO, msg.as_string())
        log.info("E-mail de alerta enviado.")
    except Exception as e:
        log.error(f"Falha ao enviar e-mail: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# 17. FUNÇÃO PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def run():
    if not cal.isbizday(date.today()):
        log.info(f"Hoje ({date.today().strftime('%d/%m/%Y')}) é feriado ANBIMA — execução cancelada.")
        return

    log.info("=" * 65)
    log.info("INICIANDO ATUALIZAÇÃO  —  treasury.py")
    log.info(f"Data/hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log.info("=" * 65)
    app = wb = None
    try:
        app = xw.App(visible=False)
        wb  = app.books.open(os.path.abspath(EXCEL_PATH))
        log.info(f"Excel aberto: {EXCEL_PATH}")

        carteira   = ler_carteira(wb)
        macro      = coletar_macro()
        taxas_mkt  = coletar_taxas_mercado()
        log.info(f"Buscando taxas ANBIMA históricas de {DATA_COMPRA} (PU Compra)...")
        taxas_hist = buscar_taxas_anbima_historico(DATA_COMPRA)
        resultado  = calcular_carteira(carteira, taxas_mkt, macro, taxas_hist)

        # Calcula P&Ls antes de escrever no Excel
        pl_total   = sum((t["pu_mercado"] - t["pu_compra"]) * t["qtd"]
                          for t in resultado)
        # P&L diário: compara valor_mercado de hoje com o do dia anterior
        pl_diario  = None
        vm_hoje = sum(t["pu_mercado"] * t["qtd"] for t in resultado)

        if os.path.exists(HISTORICO_CSV):
            try:
                df_h = pd.read_csv(HISTORICO_CSV)
                if len(df_h) >= 1:
                    vm_ant = float(df_h.iloc[-1]["valor_mercado"])
                    # Sanidade: só calcula P&L diário se a variação
                    # for menor que 20% — evita comparar carteiras
                    # com configurações diferentes (ex: qtd mudou)
                    variacao_pct = abs(vm_hoje - vm_ant) / vm_ant if vm_ant > 0 else 1
                    if variacao_pct < 0.20:
                        pl_diario = vm_hoje - vm_ant
                    else:
                        log.warning(
                            f"P&L diário ignorado: variação de {variacao_pct:.1%} "
                            f"sugere mudança de configuração da carteira. "
                            f"Delete o historico.csv para resetar."
                        )
                        pl_diario = None
            except Exception:
                pass

        escrever_excel(wb, resultado, macro, pl_total, pl_diario)

        p_pl   = _graf_pl(resultado)
        p_dv01 = _graf_dv01(resultado)
        p_hist = _graf_historico()
        colar_graficos(wb, p_pl, p_dv01, p_hist)

        wb.save()
        wb.close()
        app.quit()
        log.info("Excel salvo e fechado com sucesso.")

        pl_total, dv01_total, pl_diario = salvar_historico(resultado, macro)
        enviar_alerta(dv01_total, pl_total, pl_diario or 0.0, resultado)

        log.info("✅ Atualização concluída.")
        log.info("=" * 65)

    except Exception as e:
        log.error(f"❌ Erro durante a execução: {e}", exc_info=True)
        try:
            if wb:  wb.close()
            if app: app.quit()
        except Exception:
            pass

# ─────────────────────────────────────────────────────────────────────────────
# 18. SCHEDULER — seg a sex às 09:05
# ─────────────────────────────────────────────────────────────────────────────

def iniciar_scheduler():
    for dia in ["monday","tuesday","wednesday","thursday","friday"]:
        getattr(schedule.every(), dia).at("09:05").do(run)
    log.info("Scheduler ativo — execução diária às 09:05 (seg–sex)")
    log.info("Pressione Ctrl+C para encerrar.")
    while True:
        schedule.run_pending()
        time.sleep(30)

# ─────────────────────────────────────────────────────────────────────────────
# 19. ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--scheduler":
        iniciar_scheduler()
    else:
        run()
