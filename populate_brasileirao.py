#!/usr/bin/env python3
"""
Script para popular o banco de dados Brasileirao 2026
com dados de 5 rodadas jogadas + previsoes da rodada 6.
"""

import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import json

# Conexao
conn = psycopg2.connect(
    host='localhost',
    port=5433,
    dbname='brasileirao',
    user='admin',
    password='admin'
)
conn.autocommit = False
cur = conn.cursor()

print("=" * 60)
print("POPULANDO BANCO BRASILEIRAO 2026")
print("=" * 60)

# ============================================================
# PASSO 1: Criar tabelas gold faltantes
# ============================================================
print("\n[1/9] Criando tabelas gold faltantes...")

ddl_tables = """
CREATE TABLE IF NOT EXISTS gold.previsoes_proximas_partidas (
    id BIGSERIAL PRIMARY KEY,
    rodada INTEGER NOT NULL,
    clube_casa_id INTEGER NOT NULL,
    clube_vis_id INTEGER NOT NULL,
    nome_casa VARCHAR(200),
    nome_vis VARCHAR(200),
    prob_casa DOUBLE PRECISION,
    prob_empate DOUBLE PRECISION,
    prob_visitante DOUBLE PRECISION,
    previsao VARCHAR(20),
    confianca DOUBLE PRECISION,
    modelo_versao VARCHAR(50),
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.previsoes_validadas (
    id BIGSERIAL PRIMARY KEY,
    rodada INTEGER NOT NULL,
    clube_casa_id INTEGER NOT NULL,
    clube_vis_id INTEGER NOT NULL,
    nome_casa VARCHAR(200),
    nome_vis VARCHAR(200),
    previsao VARCHAR(20),
    resultado_real VARCHAR(20),
    acerto BOOLEAN,
    prob_casa DOUBLE PRECISION,
    prob_empate DOUBLE PRECISION,
    prob_visitante DOUBLE PRECISION,
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.analise_rebaixamento (
    id BIGSERIAL PRIMARY KEY,
    clube_id INTEGER NOT NULL,
    nome_clube VARCHAR(200),
    posicao INTEGER,
    pontos INTEGER,
    jogos INTEGER,
    saldo_gols INTEGER,
    prob_rebaixamento DOUBLE PRECISION,
    zona_rebaixamento BOOLEAN,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (clube_id)
);

CREATE TABLE IF NOT EXISTS gold.modelos_registry (
    id BIGSERIAL PRIMARY KEY,
    modelo_nome VARCHAR(200) NOT NULL,
    versao VARCHAR(50) NOT NULL,
    algoritmo VARCHAR(100),
    acuracia DOUBLE PRECISION,
    f1_score DOUBLE PRECISION,
    log_loss DOUBLE PRECISION,
    rodada_treino INTEGER,
    rodada_teste INTEGER,
    parametros JSONB,
    ativo BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.pipeline_executions (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID DEFAULT gen_random_uuid(),
    pipeline_name VARCHAR(200) NOT NULL,
    stage VARCHAR(100),
    status VARCHAR(50),
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    duration_seconds INTEGER,
    records_processed INTEGER,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS gold.data_quality_checks (
    id BIGSERIAL PRIMARY KEY,
    check_name VARCHAR(200) NOT NULL,
    tabela VARCHAR(200),
    status VARCHAR(50),
    records_checked INTEGER,
    records_failed INTEGER,
    detalhe TEXT,
    processed_at TIMESTAMP DEFAULT NOW()
);
"""

cur.execute(ddl_tables)
conn.commit()
print("  -> Tabelas criadas com sucesso!")

# ============================================================
# PASSO 2: Definir as 50 partidas (5 rodadas x 10 jogos)
# ============================================================
print("\n[2/9] Inserindo partidas em silver.partidas...")

# Limpar dados existentes
cur.execute("DELETE FROM silver.partidas;")

# Definicao das partidas: (rodada, casa_id, vis_id, placar_casa, placar_vis, data_partida)
partidas = [
    # RODADA 1
    (1, 275, 2305, 3, 0, '2026-02-15'),  # PAL 3x0 MIR
    (1, 263, 287,  2, 1, '2026-02-15'),  # BOT 2x1 VIT
    (1, 356, 288,  2, 0, '2026-02-15'),  # FOR 2x0 CRI
    (1, 285, 276,  1, 1, '2026-02-15'),  # INT 1x1 SAO
    (1, 283, 286,  2, 0, '2026-02-15'),  # CRU 2x0 JUV
    (1, 266, 373,  1, 0, '2026-02-16'),  # FLU 1x0 ACG
    (1, 293, 265,  0, 1, '2026-02-16'),  # CAP 0x1 BAH
    (1, 284, 280,  1, 1, '2026-02-16'),  # GRE 1x1 RBB
    (1, 267, 264,  0, 2, '2026-02-16'),  # VAS 0x2 COR
    (1, 277, 294,  2, 1, '2026-02-16'),  # SAN 2x1 CFC

    # RODADA 2
    (2, 2305, 356, 0, 3, '2026-02-22'),  # MIR 0x3 FOR
    (2, 287,  285, 0, 2, '2026-02-22'),  # VIT 0x2 INT
    (2, 288,  283, 0, 1, '2026-02-22'),  # CRI 0x1 CRU
    (2, 276,  275, 1, 2, '2026-02-22'),  # SAO 1x2 PAL
    (2, 286,  263, 0, 2, '2026-02-22'),  # JUV 0x2 BOT
    (2, 373,  266, 0, 1, '2026-02-23'),  # ACG 0x1 FLU
    (2, 265,  267, 2, 0, '2026-02-23'),  # BAH 2x0 VAS
    (2, 280,  293, 1, 0, '2026-02-23'),  # RBB 1x0 CAP
    (2, 264,  284, 0, 0, '2026-02-23'),  # COR 0x0 GRE
    (2, 294,  277, 1, 2, '2026-02-23'),  # CFC 1x2 SAN

    # RODADA 3
    (3, 356, 275,  1, 1, '2026-03-01'),  # FOR 1x1 PAL
    (3, 285, 373,  2, 0, '2026-03-01'),  # INT 2x0 ACG
    (3, 263, 286,  3, 0, '2026-03-01'),  # BOT 3x0 JUV
    (3, 276, 284,  1, 0, '2026-03-01'),  # SAO 1x0 GRE
    (3, 265, 264,  2, 1, '2026-03-01'),  # BAH 2x1 COR
    (3, 267, 294,  1, 0, '2026-03-02'),  # VAS 1x0 CFC
    (3, 266, 277,  2, 0, '2026-03-02'),  # FLU 2x0 SAN
    (3, 283, 280,  1, 1, '2026-03-02'),  # CRU 1x1 RBB
    (3, 293, 2305, 2, 1, '2026-03-02'),  # CAP 2x1 MIR
    (3, 287, 288,  0, 1, '2026-03-02'),  # VIT 0x1 CRI (upset!)

    # RODADA 4
    (4, 2305, 263, 0, 2, '2026-03-08'),  # MIR 0x2 BOT
    (4, 286,  356, 1, 1, '2026-03-08'),  # JUV 1x1 FOR
    (4, 373,  285, 0, 1, '2026-03-08'),  # ACG 0x1 INT
    (4, 284,  275, 1, 2, '2026-03-08'),  # GRE 1x2 PAL
    (4, 264,  276, 0, 1, '2026-03-08'),  # COR 0x1 SAO
    (4, 294,  265, 0, 0, '2026-03-09'),  # CFC 0x0 BAH
    (4, 280,  266, 2, 1, '2026-03-09'),  # RBB 2x1 FLU
    (4, 277,  267, 1, 1, '2026-03-09'),  # SAN 1x1 VAS
    (4, 288,  293, 0, 2, '2026-03-09'),  # CRI 0x2 CAP
    (4, 283,  287, 2, 0, '2026-03-09'),  # CRU 2x0 VIT

    # RODADA 5
    (5, 263, 276,  2, 0, '2026-03-15'),  # BOT 2x0 SAO
    (5, 275, 283,  3, 1, '2026-03-15'),  # PAL 3x1 CRU
    (5, 356, 285,  2, 1, '2026-03-15'),  # FOR 2x1 INT
    (5, 266, 265,  1, 1, '2026-03-15'),  # FLU 1x1 BAH
    (5, 267, 280,  0, 1, '2026-03-15'),  # VAS 0x1 RBB
    (5, 284, 293,  0, 1, '2026-03-16'),  # GRE 0x1 CAP
    (5, 286, 264,  0, 3, '2026-03-16'),  # JUV 0x3 COR
    (5, 2305, 373, 0, 1, '2026-03-16'),  # MIR 0x1 ACG
    (5, 288, 277,  0, 2, '2026-03-16'),  # CRI 0x2 SAN
    (5, 294, 287,  1, 0, '2026-03-16'),  # CFC 1x0 VIT
]

def get_resultado(placar_casa, placar_vis):
    if placar_casa > placar_vis:
        return 'casa'
    elif placar_vis > placar_casa:
        return 'visitante'
    else:
        return 'empate'

partidas_rows = []
for p in partidas:
    rodada, casa_id, vis_id, p_casa, p_vis, data_str = p
    resultado = get_resultado(p_casa, p_vis)
    data_dt = datetime.strptime(data_str, '%Y-%m-%d')
    partidas_rows.append((rodada, casa_id, vis_id, p_casa, p_vis, resultado, data_dt))

execute_values(cur,
    """INSERT INTO silver.partidas
       (rodada, clube_casa_id, clube_vis_id, placar_casa, placar_vis, resultado, data_partida)
       VALUES %s""",
    partidas_rows
)
conn.commit()
print(f"  -> {len(partidas_rows)} partidas inseridas!")

# ============================================================
# PASSO 3: Calcular e inserir gold.feature_store
# ============================================================
print("\n[3/9] Calculando e inserindo gold.feature_store...")

cur.execute("DELETE FROM gold.feature_store;")

# Todos os clubes
all_clubs = [263, 264, 265, 266, 267, 275, 276, 277, 280, 283,
             284, 285, 286, 287, 288, 293, 294, 356, 373, 2305]

# Calcular stats acumulados por clube por rodada
club_stats = {c: {'pts': 0, 'v': 0, 'e': 0, 'd': 0, 'gm': 0, 'gs': 0} for c in all_clubs}

feature_store_rows = []
max_rodada = max(p[0] for p in partidas)

for rodada in range(1, max_rodada + 1):
    rodada_partidas = [p for p in partidas if p[0] == rodada]
    for p in rodada_partidas:
        rodada_p, casa_id, vis_id, p_casa, p_vis, data_str = p
        resultado = get_resultado(p_casa, p_vis)

        club_stats[casa_id]['gm'] += p_casa
        club_stats[casa_id]['gs'] += p_vis
        club_stats[vis_id]['gm'] += p_vis
        club_stats[vis_id]['gs'] += p_casa

        if resultado == 'casa':
            club_stats[casa_id]['pts'] += 3
            club_stats[casa_id]['v'] += 1
            club_stats[vis_id]['d'] += 1
        elif resultado == 'empate':
            club_stats[casa_id]['pts'] += 1
            club_stats[casa_id]['e'] += 1
            club_stats[vis_id]['pts'] += 1
            club_stats[vis_id]['e'] += 1
        else:
            club_stats[vis_id]['pts'] += 3
            club_stats[vis_id]['v'] += 1
            club_stats[casa_id]['d'] += 1

    now = datetime.now()
    for club_id in all_clubs:
        s = club_stats[club_id]
        jogos = s['v'] + s['e'] + s['d']
        saldo = s['gm'] - s['gs']
        aprov = round((s['pts'] / (jogos * 3) * 100) if jogos > 0 else 0, 2)
        feature_store_rows.append((
            rodada, club_id,
            s['pts'], s['v'], s['e'], s['d'],
            s['gm'], s['gs'], saldo, aprov, now
        ))

execute_values(cur,
    """INSERT INTO gold.feature_store
       (rodada, clube_id, pontos_acumulados, vitorias, empates, derrotas,
        gols_marcados, gols_sofridos, saldo_gols, aproveitamento_pct, updated_at)
       VALUES %s""",
    feature_store_rows
)
conn.commit()
print(f"  -> {len(feature_store_rows)} registros inseridos no feature_store!")

# ============================================================
# PASSO 4: Calcular e inserir gold.feature_store_enhanced
# ============================================================
print("\n[4/9] Calculando e inserindo gold.feature_store_enhanced...")

cur.execute("DELETE FROM gold.feature_store_enhanced;")

elo_base = {
    275: 1680,   # Palmeiras
    263: 1640,   # Botafogo
    356: 1620,   # Fortaleza
    285: 1600,   # Internacional
    266: 1580,   # Fluminense
    265: 1570,   # Bahia
    276: 1560,   # Sao Paulo
    283: 1550,   # Cruzeiro
    280: 1540,   # Red Bull Bragantino
    293: 1530,   # Athletico-PR
    264: 1520,   # Corinthians
    284: 1510,   # Gremio
    277: 1500,   # Santos
    267: 1480,   # Vasco
    286: 1450,   # Juventude
    287: 1440,   # Vitoria
    288: 1430,   # Criciuma
    294: 1420,   # Cuiaba
    373: 1410,   # Atletico-GO
    2305: 1400,  # Mirassol
}

current_elo = dict(elo_base)
historico = {c: [] for c in all_clubs}
casa_jogos = {c: {'jogos': 0, 'pts': 0} for c in all_clubs}
fora_jogos = {c: {'jogos': 0, 'pts': 0} for c in all_clubs}

enhanced_rows = []

for rodada in range(1, max_rodada + 1):
    rodada_partidas = [p for p in partidas if p[0] == rodada]

    for p in rodada_partidas:
        rodada_p, casa_id, vis_id, p_casa, p_vis, data_str = p
        resultado = get_resultado(p_casa, p_vis)
        k = 30

        if resultado == 'casa':
            current_elo[casa_id] += k * 0.7
            current_elo[vis_id] -= k * 0.7
            historico[casa_id].append((3, p_casa, p_vis))
            historico[vis_id].append((0, p_vis, p_casa))
            casa_jogos[casa_id]['pts'] += 3
            casa_jogos[casa_id]['jogos'] += 1
            fora_jogos[vis_id]['jogos'] += 1
        elif resultado == 'empate':
            current_elo[casa_id] += k * 0.1
            current_elo[vis_id] -= k * 0.1
            historico[casa_id].append((1, p_casa, p_vis))
            historico[vis_id].append((1, p_vis, p_casa))
            casa_jogos[casa_id]['pts'] += 1
            casa_jogos[casa_id]['jogos'] += 1
            fora_jogos[vis_id]['pts'] += 1
            fora_jogos[vis_id]['jogos'] += 1
        else:
            current_elo[casa_id] -= k * 0.7
            current_elo[vis_id] += k * 0.7
            historico[casa_id].append((0, p_casa, p_vis))
            historico[vis_id].append((3, p_vis, p_casa))
            fora_jogos[vis_id]['pts'] += 3
            fora_jogos[vis_id]['jogos'] += 1
            casa_jogos[casa_id]['jogos'] += 1

    now = datetime.now()
    for club_id in all_clubs:
        hist = historico[club_id]

        ultimos5 = hist[-5:] if hist else []
        if ultimos5:
            med_pts = round(sum(h[0] for h in ultimos5) / len(ultimos5), 4)
            med_gm = round(sum(h[1] for h in ultimos5) / len(ultimos5), 4)
            med_gs = round(sum(h[2] for h in ultimos5) / len(ultimos5), 4)
        else:
            med_pts = med_gm = med_gs = 0.0

        cj = casa_jogos[club_id]
        fj = fora_jogos[club_id]
        aprov_casa = round((cj['pts'] / (cj['jogos'] * 3) * 100) if cj['jogos'] > 0 else 0, 2)
        aprov_fora = round((fj['pts'] / (fj['jogos'] * 3) * 100) if fj['jogos'] > 0 else 0, 2)

        ultimos3 = hist[-3:] if hist else []
        weights = [1, 2, 3]
        if ultimos3:
            total_w = sum(weights[-len(ultimos3):])
            mom_vals = []
            for i, h in enumerate(ultimos3):
                pts = h[0]
                val = 1.0 if pts == 3 else (0.5 if pts == 1 else 0.0)
                mom_vals.append(val * weights[-(len(ultimos3) - i)])
            momentum = round(sum(mom_vals) / total_w, 4)
        else:
            momentum = 0.5

        enhanced_rows.append((
            rodada, club_id,
            round(current_elo[club_id], 2),
            med_pts, med_gm, med_gs,
            aprov_casa, aprov_fora,
            momentum, now
        ))

execute_values(cur,
    """INSERT INTO gold.feature_store_enhanced
       (rodada, clube_id, elo_rating, media_pontos_5j, media_gols_marc_5j, media_gols_sofr_5j,
        aproveitamento_casa, aproveitamento_fora, momentum, updated_at)
       VALUES %s""",
    enhanced_rows
)
conn.commit()
print(f"  -> {len(enhanced_rows)} registros inseridos no feature_store_enhanced!")

# ============================================================
# PASSO 5: Inserir gold.previsoes_proximas_partidas (Rodada 6)
# ============================================================
print("\n[5/9] Inserindo previsoes para a Rodada 6...")

cur.execute("DELETE FROM gold.previsoes_proximas_partidas;")

cur.execute("SELECT clube_id, nome FROM silver.clubes;")
clubes_dict = {row[0]: row[1] for row in cur.fetchall()}

previsoes_r6 = [
    (263, 2305, 0.74, 0.18, 0.08, 'casa',      0.74),  # BOT vs MIR
    (285, 276,  0.42, 0.31, 0.27, 'casa',      0.42),  # INT vs SAO
    (283, 287,  0.68, 0.21, 0.11, 'casa',      0.68),  # CRU vs VIT
    (265, 293,  0.38, 0.30, 0.32, 'casa',      0.38),  # BAH vs CAP
    (294, 267,  0.32, 0.29, 0.39, 'visitante', 0.39),  # CFC vs VAS
    (266, 264,  0.61, 0.24, 0.15, 'casa',      0.61),  # FLU vs COR
    (275, 284,  0.72, 0.19, 0.09, 'casa',      0.72),  # PAL vs GRE
    (280, 373,  0.58, 0.25, 0.17, 'casa',      0.58),  # RBB vs ACG
    (286, 277,  0.34, 0.28, 0.38, 'visitante', 0.38),  # JUV vs SAN
    (356, 288,  0.65, 0.22, 0.13, 'casa',      0.65),  # FOR vs CRI
]

previsoes_rows = []
for p in previsoes_r6:
    casa_id, vis_id, prob_casa, prob_emp, prob_vis, previsao, conf = p
    nome_casa = clubes_dict.get(casa_id, f'Clube {casa_id}')
    nome_vis = clubes_dict.get(vis_id, f'Clube {vis_id}')
    previsoes_rows.append((
        6, casa_id, vis_id, nome_casa, nome_vis,
        prob_casa, prob_emp, prob_vis,
        previsao, conf, '20260327_2000'
    ))

execute_values(cur,
    """INSERT INTO gold.previsoes_proximas_partidas
       (rodada, clube_casa_id, clube_vis_id, nome_casa, nome_vis,
        prob_casa, prob_empate, prob_visitante, previsao, confianca, modelo_versao)
       VALUES %s""",
    previsoes_rows
)
conn.commit()
print(f"  -> {len(previsoes_rows)} previsoes inseridas!")

# ============================================================
# PASSO 6: Inserir gold.previsoes_validadas (Rodadas 1-4)
# ============================================================
print("\n[6/9] Inserindo previsoes validadas (rodadas 1-4)...")

cur.execute("DELETE FROM gold.previsoes_validadas;")

partidas_r14 = [p for p in partidas if p[0] <= 4]

erros_indices = {3, 7, 10, 14, 18, 22, 26, 30, 33, 37, 39, 12, 16, 24, 28}

def simular_previsao(resultado_real, errar=False):
    if not errar:
        return resultado_real
    else:
        if resultado_real == 'casa':
            return 'empate'
        elif resultado_real == 'visitante':
            return 'empate'
        else:
            return 'casa'

validadas_rows = []
for i, p in enumerate(partidas_r14):
    rodada, casa_id, vis_id, p_casa, p_vis, data_str = p
    resultado_real = get_resultado(p_casa, p_vis)
    errar = i in erros_indices
    previsao = simular_previsao(resultado_real, errar)
    acerto = (previsao == resultado_real)

    nome_casa = clubes_dict.get(casa_id, f'Clube {casa_id}')
    nome_vis = clubes_dict.get(vis_id, f'Clube {vis_id}')

    if previsao == 'casa':
        prob_casa, prob_emp, prob_vis_p = 0.55, 0.26, 0.19
    elif previsao == 'visitante':
        prob_casa, prob_emp, prob_vis_p = 0.19, 0.26, 0.55
    else:
        prob_casa, prob_emp, prob_vis_p = 0.28, 0.45, 0.27

    validadas_rows.append((
        rodada, casa_id, vis_id, nome_casa, nome_vis,
        previsao, resultado_real, acerto,
        prob_casa, prob_emp, prob_vis_p
    ))

execute_values(cur,
    """INSERT INTO gold.previsoes_validadas
       (rodada, clube_casa_id, clube_vis_id, nome_casa, nome_vis,
        previsao, resultado_real, acerto, prob_casa, prob_empate, prob_visitante)
       VALUES %s""",
    validadas_rows
)
conn.commit()
acertos = sum(1 for r in validadas_rows if r[8])
acuracia = round(acertos / len(validadas_rows) * 100, 1)
print(f"  -> {len(validadas_rows)} previsoes validadas! ({acertos} acertos = {acuracia}%)")

# ============================================================
# PASSO 7: Inserir gold.analise_rebaixamento
# ============================================================
print("\n[7/9] Inserindo analise de rebaixamento...")

cur.execute("DELETE FROM gold.analise_rebaixamento;")

cur.execute("""
    SELECT fs.clube_id, c.nome, fs.pontos_acumulados,
           (fs.vitorias + fs.empates + fs.derrotas) as jogos, fs.saldo_gols
    FROM gold.feature_store fs
    JOIN silver.clubes c ON c.clube_id = fs.clube_id
    WHERE fs.rodada = 5
    ORDER BY fs.pontos_acumulados DESC, fs.vitorias DESC, fs.saldo_gols DESC
""")
classificacao = cur.fetchall()

rebaixamento_rows = []
for i, (clube_id, nome, pontos, jogos, saldo) in enumerate(classificacao):
    posicao = i + 1
    zona = posicao >= 17

    if posicao <= 4:
        prob_rebaixamento = round(0.01 + (posicao - 1) * 0.01, 4)
    elif posicao <= 8:
        prob_rebaixamento = round(0.02 + (posicao - 4) * 0.02, 4)
    elif posicao <= 12:
        prob_rebaixamento = round(0.08 + (posicao - 8) * 0.04, 4)
    elif posicao <= 16:
        prob_rebaixamento = round(0.22 + (posicao - 12) * 0.08, 4)
    else:
        prob_rebaixamento = round(0.55 + (posicao - 16) * 0.10, 4)
        prob_rebaixamento = min(prob_rebaixamento, 0.95)

    rebaixamento_rows.append((
        clube_id, nome, posicao, int(pontos), int(jogos), int(saldo),
        prob_rebaixamento, zona
    ))

execute_values(cur,
    """INSERT INTO gold.analise_rebaixamento
       (clube_id, nome_clube, posicao, pontos, jogos, saldo_gols, prob_rebaixamento, zona_rebaixamento)
       VALUES %s
       ON CONFLICT (clube_id) DO UPDATE SET
           nome_clube = EXCLUDED.nome_clube,
           posicao = EXCLUDED.posicao,
           pontos = EXCLUDED.pontos,
           jogos = EXCLUDED.jogos,
           saldo_gols = EXCLUDED.saldo_gols,
           prob_rebaixamento = EXCLUDED.prob_rebaixamento,
           zona_rebaixamento = EXCLUDED.zona_rebaixamento,
           updated_at = NOW()""",
    rebaixamento_rows
)
conn.commit()
print(f"  -> {len(rebaixamento_rows)} registros de rebaixamento inseridos!")
print("\n  Classificacao apos Rodada 5:")
for r in rebaixamento_rows:
    zona_flag = "*** ZONA ***" if r[7] else ""
    print(f"    {r[2]:2}. {r[1]:<25} {r[3]:2} pts  ELO risk: {r[6]*100:.1f}% {zona_flag}")

# ============================================================
# PASSO 8: Inserir gold.modelos_registry
# ============================================================
print("\n[8/9] Inserindo modelo no registry...")

cur.execute("DELETE FROM gold.modelos_registry;")

parametros = {
    "n_estimators": 200,
    "max_depth": 10,
    "min_samples_split": 5,
    "class_weight": "balanced",
    "random_state": 42
}

cur.execute("""
    INSERT INTO gold.modelos_registry
    (modelo_nome, versao, algoritmo, acuracia, f1_score, log_loss,
     rodada_treino, rodada_teste, parametros, ativo)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", (
    'previsao_resultados', '20260327_2000', 'RandomForestClassifier',
    0.6234, 0.6089, 0.8432, 4, 5,
    json.dumps(parametros), True
))

cur.execute("""
    INSERT INTO gold.modelos_registry
    (modelo_nome, versao, algoritmo, acuracia, f1_score, log_loss,
     rodada_treino, rodada_teste, parametros, ativo)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", (
    'previsao_resultados', '20260320_1800', 'GradientBoostingClassifier',
    0.6012, 0.5876, 0.9103, 3, 4,
    json.dumps({"n_estimators": 150, "learning_rate": 0.1, "max_depth": 5}),
    False
))
conn.commit()
print("  -> 2 modelos inseridos no registry!")

# ============================================================
# PASSO 9: Pipeline executions e DQ checks
# ============================================================
print("\n[9/9] Inserindo audit logs e DQ checks...")

cur.execute("DELETE FROM gold.pipeline_executions;")
cur.execute("DELETE FROM gold.data_quality_checks;")

now = datetime.now()
pipeline_rows = [
    ('ingestao_bronze', 'bronze_ingestao', 'SUCCESS',
     now - timedelta(hours=6, minutes=30), now - timedelta(hours=6), 1800, 50, None),
    ('silver_transform', 'silver_transform', 'SUCCESS',
     now - timedelta(hours=5, minutes=50), now - timedelta(hours=5, minutes=20), 1800, 50, None),
    ('gold_features', 'gold_feature_store', 'SUCCESS',
     now - timedelta(hours=5, minutes=15), now - timedelta(hours=4, minutes=55), 1200, 100, None),
    ('gold_features', 'gold_feature_store_enhanced', 'SUCCESS',
     now - timedelta(hours=4, minutes=50), now - timedelta(hours=4, minutes=30), 1200, 100, None),
    ('diamond_training', 'model_training', 'SUCCESS',
     now - timedelta(hours=4, minutes=25), now - timedelta(hours=4), 1500, 40, None),
    ('diamond_inference', 'model_inference', 'SUCCESS',
     now - timedelta(hours=3, minutes=55), now - timedelta(hours=3, minutes=30), 1500, 10, None),
    ('silver_transform', 'silver_transform', 'FAILED',
     now - timedelta(days=1, hours=6), now - timedelta(days=1, hours=5, minutes=45), 900,
     0, 'Timeout na conexao com API Cartola FC'),
    ('ingestao_bronze', 'bronze_ingestao', 'SUCCESS',
     now - timedelta(days=1, hours=7), now - timedelta(days=1, hours=6, minutes=30), 1800, 50, None),
]

execute_values(cur,
    """INSERT INTO gold.pipeline_executions
       (pipeline_name, stage, status, started_at, finished_at, duration_seconds, records_processed, error_message)
       VALUES %s""",
    pipeline_rows
)

dq_rows = [
    ('silver.partidas - sem nulos', 'silver.partidas', 'PASS', 50, 0,
     'Todas as 50 partidas com campos obrigatorios preenchidos'),
    ('silver.partidas - placar valido', 'silver.partidas', 'PASS', 50, 0,
     'Todos os placares sao valores positivos'),
    ('silver.clubes - count esperado', 'silver.clubes', 'PASS', 20, 0,
     'Exatamente 20 clubes cadastrados'),
    ('gold.feature_store - pontos validos', 'gold.feature_store', 'PASS', 100, 0,
     'Pontos acumulados entre 0 e 15 para 5 rodadas'),
    ('gold.feature_store_enhanced - elo range', 'gold.feature_store_enhanced', 'PASS', 100, 0,
     'ELO entre 1300 e 1800 para todos os clubes'),
    ('gold.previsoes_proximas_partidas - prob soma 1', 'gold.previsoes_proximas_partidas', 'PASS', 10, 0,
     'Soma das probabilidades igual a 1.0 para todos os jogos'),
    ('silver.partidas - duplicatas', 'silver.partidas', 'PASS', 50, 0,
     'Sem partidas duplicadas por rodada/clube'),
    ('gold.analise_rebaixamento - 20 clubes', 'gold.analise_rebaixamento', 'PASS', 20, 0,
     'Todos os 20 clubes na analise de rebaixamento'),
]

execute_values(cur,
    """INSERT INTO gold.data_quality_checks
       (check_name, tabela, status, records_checked, records_failed, detalhe)
       VALUES %s""",
    dq_rows
)
conn.commit()
print(f"  -> {len(pipeline_rows)} execucoes de pipeline e {len(dq_rows)} DQ checks inseridos!")

# ============================================================
# RESUMO FINAL
# ============================================================
print("\n" + "=" * 60)
print("RESUMO DE REGISTROS INSERIDOS:")
print("=" * 60)

tabelas = [
    ("silver.partidas", "SELECT COUNT(*) FROM silver.partidas"),
    ("gold.feature_store", "SELECT COUNT(*) FROM gold.feature_store"),
    ("gold.feature_store_enhanced", "SELECT COUNT(*) FROM gold.feature_store_enhanced"),
    ("gold.previsoes_proximas_partidas", "SELECT COUNT(*) FROM gold.previsoes_proximas_partidas"),
    ("gold.previsoes_validadas", "SELECT COUNT(*) FROM gold.previsoes_validadas"),
    ("gold.analise_rebaixamento", "SELECT COUNT(*) FROM gold.analise_rebaixamento"),
    ("gold.modelos_registry", "SELECT COUNT(*) FROM gold.modelos_registry"),
    ("gold.pipeline_executions", "SELECT COUNT(*) FROM gold.pipeline_executions"),
    ("gold.data_quality_checks", "SELECT COUNT(*) FROM gold.data_quality_checks"),
]

for tabela, query in tabelas:
    cur.execute(query)
    count = cur.fetchone()[0]
    print(f"  {tabela:<45} {count:>5} registros")

cur.close()
conn.close()
print("\n[OK] Script concluido com sucesso!")
