# Treasury-P-L
Automated fixed income portfolio tracker with Python + Excel
# 📊 Treasury Dashboard — Monitoramento Automatizado de Carteira de Renda Fixa

> **Um projeto em Python que simula como a tesouraria de um banco acompanha sua carteira de títulos públicos em tempo real.**

🇧🇷 Português | 🇺🇸 (Soon)

Desenvolvido por **Murilo Mendes** | Estudante de Economia | https://linkedin.com/in/murilomendesz

---

## O que é este projeto?

Todo banco tem uma **mesa de tesouraria**: uma equipe responsável por gerenciar o portfólio de ativos financeiros da própria instituição. Uma das principais funções dessa mesa é monitorar uma carteira de títulos públicos (LTNs, NTN-Bs, NTN-Fs e LFTs) e responder, a qualquer momento:

- **Quanto vale minha carteira agora?**
- **Quanto ganhei ou perdi hoje?**
- **Quão sensível é meu portfólio a variações nas taxas de juros?**

Em grandes instituições, isso é feito por sistemas sofisticados e caros, como o Bloomberg. Este projeto simula esse fluxo de trabalho usando **Python + Excel**, puxando dados reais de mercado automaticamente toda manhã.

---

## O que ele faz na prática?

```
09:05 — Agendador dispara automaticamente (seg–sex)
     ↓
Busca dados ao vivo da API pública do Banco Central/ANBIMA (Alguns dados ANBIMA não puderam ser computados)
     ↓
Calcula o valor justo (Marcação a Mercado) de cada título
     ↓
Atualiza a planilha Excel com novos preços, P&L e métricas de risco
     ↓
Gera gráficos e os cola automaticamente no Excel
     ↓
Salva um snapshot diário no historico.csv
     ↓
Envia alerta por e-mail se o risco da carteira ultrapassar o limite
```

Sem trabalho manual. Sem copiar e colar. O operador abre o Excel e tudo já está atualizado.

---

## A Carteira (Simulada)

7 títulos públicos federais brasileiros, comprados em **07/03/2026**:

| Título | Tipo | Cupom | Vencimento |
|--------|------|-------|-----------|
| LTN 01/07/2026 | Prefixado | Nenhum (zero cupom) | Curto |
| LTN 01/01/2028 | Prefixado | Nenhum | Médio |
| NTN-F 01/01/2031 | Prefixado | 10% a.a. semestral | Médio-Longo |
| NTN-B 15/05/2029 | IPCA+ | 6% a.a. semestral | Médio |
| NTN-B 15/05/2035 | IPCA+ | 6% a.a. semestral | Longo |
| LFT 01/03/2027 | Pós-fixado (Selic) | Nenhum | Curto |
| LFT 01/09/2030 | Pós-fixado (Selic) | Nenhum | Longo |

---

## Conceitos-chave 

### Marcação a Mercado (M2M)
Todo título tem um *preço de compra* (o que você pagou) e um *preço de mercado* (o que vale hoje). A diferença é o seu **P&L** — lucro ou prejuízo. O processo de reprecificar a carteira diariamente se chama Marcação a Mercado, e é obrigatório para todas as instituições financeiras brasileiras segundo regulação do BACEN.

### Duration
Pense na duration como o *dial de sensibilidade* de um título. Um título que vence em 10 anos com cupons pequenos no meio do caminho vai reagir muito mais violentamente a mudanças nas taxas de juros do que um título que vence em 6 meses. A duration mede exatamente o quanto o preço se move quando as taxas variam 1% — essencial para gestão de risco.

### DV01 (Valor em Reais de 1 Basis Point)
Leva a Duration um passo adiante: *"se as taxas de juros se moverem apenas 0,01% (1 basis point), quantos reais eu perco ou ganho?"* Uma carteira com DV01 de R$500 significa que um movimento de 1bp custa R$500. Traders usam isso para dimensionar seus hedges no mercado de derivativos (DI futuro).

> ⚠️ **Nota metodológica sobre LFTs:** O título pós-fixado (LFT) não carrega risco de taxa de juros tradicional, seu preço se move diariamente com a taxa Selic através do VNA (Valor Nominal Atualizado). O que calculamos para LFTs é o *spread DV01*: sensibilidade a variações no spread *sobre* a Selic. Isso é conceitualmente diferente do DV01 de taxa dos títulos prefixados e é exibido separadamente na planilha para evitar distorções na agregação de risco da carteira.

---

## Como a matemática funciona

### Precificação (PU)

**LTN** (zero cupom, prefixado):
```
PU = 1.000 / (1 + taxa)^(dias_úteis / 252)
```

**NTN-F** (prefixado com cupons semestrais de 10% a.a.):
```
PU = Σ [cupom / (1 + taxa)^(DU_i/252)] + [1.000 / (1 + taxa)^(DU_n/252)]
```

**NTN-B** (IPCA+ com cupons semestrais de 6% a.a.):
```
PU = Σ [cupom / (1 + taxa_real)^(DU_i/252)] + [VNA / (1 + taxa_real)^(DU_n/252)]
```
Onde **VNA** = R$1.000 × IPCA acumulado desde 15/07/2000

**LFT** (pós-fixado Selic):
```
PU = VNA / (1 + spread)^(DU/252)
```
Onde **VNA** = R$1.000 × Selic acumulada desde 01/07/2000

> Todos os cálculos utilizam **dias úteis (base 252)** com o calendário oficial ANBIMA/B3, implementado via biblioteca `bizdays`.

---

## Fontes de Dados (100% gratuitas, sem autenticação)

### ANBIMA — Taxas Indicativas
As taxas de compra e de mercado são obtidas diretamente dos arquivos históricos públicos da ANBIMA:

```
https://www.anbima.com.br/informacoes/merc-sec/arqs/ms260306.txt  ← exemplo: 06/03/2026
```

O padrão do nome é `ms` + `AAMMDD`. O arquivo do dia é baixado automaticamente a cada execução. Se a data for fim de semana ou feriado, o código recua até encontrar o último pregão disponível (até 5 dias).

É utilizada a coluna **Tx. Indicativas** — média de mercado publicada diariamente pela ANBIMA — para os 7 títulos da carteira:

- **Taxa de Compra:** arquivo de 06/03/2026 (último pregão antes da compra fictícia de 07/03)
- **Taxa de Mercado:** arquivo do dia atual em que o código é executado

> O arquivo não requer autenticação e é acessível via `requests`. O acesso direto pelo navegador pode ser redirecionado pelo portal da ANBIMA.

---

### BACEN SGS — VNAs e Indicadores Macro
Os VNAs (Valor Nominal Atualizado) e indicadores macroeconômicos são calculados a partir da **API pública do Banco Central do Brasil**:

| Série | Dado |
|-------|------|
| 11 | Taxa Selic diária (% ao dia) → calcula VNA LFT |
| 433 | IPCA mensal (% a.m.) → calcula VNA NTN-B |
| 432 | Meta da taxa Selic (% a.a.) |
| 13522 | IPCA acumulado 12 meses |

---

## A Planilha Excel

A planilha (`carteira_legado.xlsx`) simula uma planilha legado de tesouraria: o tipo que existe em todo banco e nunca é substituído porque centenas de fórmulas dependem dela.

**Três abas:**

**📋 CARTEIRA RF** — O book principal
- Células amarelas = inputs do operador (quantidade, taxa de compra)
- Células azuis = atualizadas automaticamente pelo Python toda manhã
- Células cinzas = fórmulas nativas do Excel (P&L, Valor de Mercado, DV01 Total)
- Formatação condicional: P&L verde = lucro, vermelho = prejuízo

**📊 CONTROLE CARTEIRA** — Painel de risco
- P&L total desde a data de compra
- P&L do dia (vs ontem)
- DV01 da carteira (excluindo spread DV01 das LFTs)
- Duration média ponderada
- Gráficos: P&L por título, DV01 por indexador, histórico do P&L

**⚙️ PARÂMETROS** — Dados de mercado escritos pelo Python
- Taxa Selic, IPCA, valores de VNA
- Curva de juros ETTJ ANBIMA
- Taxas indicativas por título

---

## Estrutura do Projeto

```
treasury-dashboard/
├── treasury.py          ← Script principal (execute este)
├── carteira_legado.xlsx ← Template da planilha Excel
├── requirements.txt     ← Dependências Python
├── .gitignore
└── README.md
```

Gerado em tempo de execução (não versionado no git):
```
├── historico.csv        ← Histórico diário de P&L
├── treasury.log         ← Log de execução
├── grafico_pl.png       ← Gráfico de P&L (colado no Excel)
├── grafico_dv01.png     ← Gráfico de DV01
└── grafico_historico.png← Curva histórica de P&L
```

---

## Como executar

**1. Instalar dependências**
```bash
pip install requests pandas numpy xlwings matplotlib bizdays schedule
```

**2. Executar uma vez (manual)**
```bash
python treasury.py
```

**3. Executar com agendador (seg–sex às 09:05)**
```bash
python treasury.py --scheduler
```

**4. Configurar alertas por e-mail** *(opcional)*

Em `treasury.py`, defina:
```python
EMAIL_ATIVO     = True
EMAIL_REMETENTE = "seuemail@gmail.com"
EMAIL_SENHA     = "sua-senha-de-app"    # Senha de app do Google
EMAIL_DESTINO   = "seuemail@gmail.com"
DV01_LIMITE     = 10_000               # Alerta se DV01 > R$10.000
```

---

## Limitações conhecidas e transparência técnica

Este é um projeto educacional/portfólio, não um sistema de produção. Principais diferenças em relação a um sistema real de tesouraria:

| Funcionalidade | Este projeto | Sistema real |
|----------------|-------------|--------------|
| Taxas de mercado | Taxas indicativas ANBIMA (arquivo público diário) | Igual |
| Precisão do VNA | ~0,5% de diferença vs oficial | ANBIMA oficial (8 casas decimais) |
| Universo de títulos | 7 títulos, 1 data de compra | Book completo, múltiplas datas |
| Atualização | Diária (09:05) | Tempo real |
| Calendário de feriados | ANBIMA/B3 via bizdays | Igual |
| Metodologia de precificação | Fórmulas padrão ANBIMA | Igual |

---

## Tecnologias

`Python 3.11+` · `xlwings` · `pandas` · `numpy` · `matplotlib` · `bizdays` · `requests` · `schedule`

---

## Por que construí isso

Sou estudante de Economia no 5º semestre, apaixonado por trading e teses de investimento, com grande interesse em ingressar em uma função de Front Office em banco de atacado (Tesouraria ALM / Trading Desk / Sales & Trading).

Recentemente me deparei com a biblioteca xlwings, que substitui completamente a necessidade de usar VBA (Visual Basic for Applications) no Excel. Isso me deu a ideia de montar um projeto para mostrar o quão eficiente pode ser combinar Python para buscar dados via API, realizar cálculos e vetorizações, com a boa e velha planilha Excel.

Este projeto é minha tentativa de mostrar que as duas ferramentas podem coexistir e, quando trabalhadas juntas, se tornam algo muito mais poderoso do que cada uma separada.

Se você chegou até aqui, obrigado pela leitura. Críticas, sugestões e conexões são sempre bem-vindas > pode abrir uma issue ou me chamar no LinkedIn. Este é meu primeiro projeto público no GitHub, e certamente não será o último.

---

*Feedback bem-vindo — abra uma issue ou conecte-se no [LinkedIn](https://linkedin.com/in/murilomendesz)*
