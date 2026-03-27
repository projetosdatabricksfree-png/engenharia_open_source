-- ============================================================
-- Medallion Architecture: bronze > silver > gold
-- Banco: brasileirao
-- ============================================================
\connect brasileirao;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Schemas (camadas do medalhao)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- BRONZE: dados brutos da API (append-only)
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.partidas_raw (
    id             BIGSERIAL PRIMARY KEY,
    rodada         INTEGER,
    clube_casa_id  INTEGER,
    clube_vis_id   INTEGER,
    placar_casa    INTEGER,
    placar_vis     INTEGER,
    data_partida   TIMESTAMP,
    status         VARCHAR(50),
    raw_payload    JSONB,
    ingested_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.clubes_info_raw (
    id          BIGSERIAL PRIMARY KEY,
    clube_id    INTEGER,
    nome        VARCHAR(200),
    abreviacao  VARCHAR(20),
    escudo_url  TEXT,
    raw_payload JSONB,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.jogadores_status_raw (
    id          BIGSERIAL PRIMARY KEY,
    atleta_id   INTEGER,
    nome        VARCHAR(200),
    clube_id    INTEGER,
    posicao_id  INTEGER,
    status_id   INTEGER,
    raw_payload JSONB,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.pontuacoes_historico_raw (
    id          BIGSERIAL PRIMARY KEY,
    rodada      INTEGER,
    atleta_id   INTEGER,
    clube_id    INTEGER,
    pontos      DOUBLE PRECISION,
    raw_payload JSONB,
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- SILVER: dados limpos e normalizados
-- ============================================================
CREATE TABLE IF NOT EXISTS silver.partidas (
    id             BIGSERIAL PRIMARY KEY,
    rodada         INTEGER NOT NULL,
    clube_casa_id  INTEGER NOT NULL,
    clube_vis_id   INTEGER NOT NULL,
    placar_casa    INTEGER,
    placar_vis     INTEGER,
    resultado      VARCHAR(20),   -- 'casa' | 'visitante' | 'empate'
    data_partida   TIMESTAMP,
    processed_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.clubes (
    id         BIGSERIAL PRIMARY KEY,
    clube_id   INTEGER UNIQUE NOT NULL,
    nome       VARCHAR(200),
    abreviacao VARCHAR(20),
    escudo_url TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.estatisticas_jogador_partida (
    id           BIGSERIAL PRIMARY KEY,
    rodada       INTEGER,
    atleta_id    INTEGER,
    clube_id     INTEGER,
    posicao_id   INTEGER,
    pontos       DOUBLE PRECISION,
    processed_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- GOLD: feature store, ML e marts de BI
-- ============================================================

-- Feature store basico
CREATE TABLE IF NOT EXISTS gold.feature_store (
    id                  BIGSERIAL PRIMARY KEY,
    rodada              INTEGER NOT NULL,
    clube_id            INTEGER NOT NULL,
    pontos_acumulados   INTEGER DEFAULT 0,
    vitorias            INTEGER DEFAULT 0,
    empates             INTEGER DEFAULT 0,
    derrotas            INTEGER DEFAULT 0,
    gols_marcados       INTEGER DEFAULT 0,
    gols_sofridos       INTEGER DEFAULT 0,
    saldo_gols          INTEGER DEFAULT 0,
    aproveitamento_pct  DOUBLE PRECISION DEFAULT 0,
    updated_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE (rodada, clube_id)
);

-- Feature store avancado (ELO, medias moveis, momentum)
CREATE TABLE IF NOT EXISTS gold.feature_store_enhanced (
    id                       BIGSERIAL PRIMARY KEY,
    rodada                   INTEGER NOT NULL,
    clube_id                 INTEGER NOT NULL,
    elo_rating               DOUBLE PRECISION DEFAULT 1500,
    media_pontos_5j          DOUBLE PRECISION DEFAULT 0,
    media_gols_marc_5j       DOUBLE PRECISION DEFAULT 0,
    media_gols_sofr_5j       DOUBLE PRECISION DEFAULT 0,
    aproveitamento_casa      DOUBLE PRECISION DEFAULT 0,
    aproveitamento_fora      DOUBLE PRECISION DEFAULT 0,
    momentum                 DOUBLE PRECISION DEFAULT 0,
    updated_at               TIMESTAMP DEFAULT NOW(),
    UNIQUE (rodada, clube_id)
);

-- Outputs do pipeline ML (Spark → gold)
CREATE TABLE IF NOT EXISTS gold.previsoes_proximas_partidas (
    id              BIGSERIAL PRIMARY KEY,
    rodada          INTEGER NOT NULL,
    clube_casa_id   INTEGER NOT NULL,
    clube_vis_id    INTEGER NOT NULL,
    nome_casa       VARCHAR(200),
    nome_vis        VARCHAR(200),
    prob_casa       DOUBLE PRECISION,
    prob_empate     DOUBLE PRECISION,
    prob_visitante  DOUBLE PRECISION,
    previsao        VARCHAR(20),
    confianca       DOUBLE PRECISION,
    modelo_versao   VARCHAR(50),
    processed_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.previsoes_validadas (
    id              BIGSERIAL PRIMARY KEY,
    rodada          INTEGER NOT NULL,
    clube_casa_id   INTEGER NOT NULL,
    clube_vis_id    INTEGER NOT NULL,
    nome_casa       VARCHAR(200),
    nome_vis        VARCHAR(200),
    previsao        VARCHAR(20),
    resultado_real  VARCHAR(20),
    acerto          BOOLEAN,
    prob_casa       DOUBLE PRECISION,
    prob_empate     DOUBLE PRECISION,
    prob_visitante  DOUBLE PRECISION,
    processed_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.analise_rebaixamento (
    id                   BIGSERIAL PRIMARY KEY,
    clube_id             INTEGER NOT NULL,
    nome_clube           VARCHAR(200),
    posicao              INTEGER,
    pontos               INTEGER,
    jogos                INTEGER,
    saldo_gols           INTEGER,
    prob_rebaixamento    DOUBLE PRECISION,
    zona_rebaixamento    BOOLEAN,
    updated_at           TIMESTAMP DEFAULT NOW(),
    UNIQUE (clube_id)
);

CREATE TABLE IF NOT EXISTS gold.modelos_registry (
    id             BIGSERIAL PRIMARY KEY,
    modelo_nome    VARCHAR(200) NOT NULL,
    versao         VARCHAR(50)  NOT NULL,
    algoritmo      VARCHAR(100),
    acuracia       DOUBLE PRECISION,
    f1_score       DOUBLE PRECISION,
    log_loss       DOUBLE PRECISION,
    rodada_treino  INTEGER,
    rodada_teste   INTEGER,
    parametros     JSONB,
    ativo          BOOLEAN DEFAULT FALSE,
    processed_at   TIMESTAMP DEFAULT NOW()
);

-- Tabelas de auditoria e qualidade de dados
CREATE TABLE IF NOT EXISTS gold.pipeline_executions (
    id                 BIGSERIAL PRIMARY KEY,
    run_id             UUID DEFAULT uuid_generate_v4(),
    pipeline_name      VARCHAR(200) NOT NULL,
    stage              VARCHAR(100),
    status             VARCHAR(50),
    started_at         TIMESTAMP,
    finished_at        TIMESTAMP,
    duration_seconds   INTEGER,
    records_processed  INTEGER,
    error_message      TEXT
);

CREATE TABLE IF NOT EXISTS gold.data_quality_checks (
    id               BIGSERIAL PRIMARY KEY,
    check_name       VARCHAR(200) NOT NULL,
    tabela           VARCHAR(200),
    status           VARCHAR(50),
    records_checked  INTEGER,
    records_failed   INTEGER,
    detalhe          TEXT,
    processed_at     TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- GOLD: Views analíticas (recriadas no init para persistência)
-- ============================================================
CREATE OR REPLACE VIEW gold.vw_classificacao AS
WITH ultima_rodada AS (SELECT MAX(rodada) AS rodada FROM gold.feature_store)
SELECT
    ROW_NUMBER() OVER (ORDER BY fs.pontos_acumulados DESC, fs.vitorias DESC, fs.saldo_gols DESC, fs.gols_marcados DESC) AS posicao,
    fs.clube_id,
    c.nome,
    c.abreviacao,
    (fs.vitorias + fs.empates + fs.derrotas) AS jogos,
    fs.pontos_acumulados  AS pontos,
    fs.vitorias,
    fs.empates,
    fs.derrotas,
    fs.gols_marcados      AS gols_pro,
    fs.gols_sofridos      AS gols_contra,
    fs.saldo_gols,
    fs.aproveitamento_pct AS aproveitamento
FROM gold.feature_store fs
JOIN silver.clubes c ON fs.clube_id = c.clube_id
JOIN ultima_rodada ur ON fs.rodada = ur.rodada;

CREATE OR REPLACE VIEW gold.vw_elo_ranking AS
WITH ultima_rodada AS (SELECT MAX(rodada) AS rodada FROM gold.feature_store)
SELECT
    ROW_NUMBER() OVER (ORDER BY fs.pontos_acumulados DESC, fs.aproveitamento_pct DESC) AS ranking,
    fs.clube_id,
    c.nome,
    c.abreviacao,
    fs.pontos_acumulados  AS pontos,
    fs.aproveitamento_pct AS aproveitamento,
    fs.vitorias,
    fs.empates,
    fs.derrotas
FROM gold.feature_store fs
JOIN silver.clubes c ON fs.clube_id = c.clube_id
JOIN ultima_rodada ur ON fs.rodada = ur.rodada
ORDER BY pontos DESC;

CREATE OR REPLACE VIEW gold.vw_previsoes AS
SELECT
    p.rodada,
    p.nome_casa,
    p.nome_vis,
    p.prob_casa,
    p.prob_empate,
    p.prob_visitante,
    p.previsao,
    p.confianca,
    p.modelo_versao,
    p.processed_at
FROM gold.previsoes_proximas_partidas p
ORDER BY p.rodada, p.confianca DESC;

CREATE OR REPLACE VIEW gold.vw_desempenho_modelo AS
WITH validadas AS (
    SELECT
        rodada,
        COUNT(*)                                                        AS total,
        SUM(CASE WHEN previsao = resultado_real THEN 1 ELSE 0 END)     AS acertos
    FROM gold.previsoes_validadas
    GROUP BY rodada
)
SELECT
    rodada,
    total                                              AS partidas,
    acertos,
    ROUND(acertos::NUMERIC / NULLIF(total,0) * 100, 1) AS acuracia_pct
FROM validadas
ORDER BY rodada;

-- Indices
CREATE INDEX IF NOT EXISTS idx_feature_rodada       ON gold.feature_store_enhanced (rodada, clube_id);
CREATE INDEX IF NOT EXISTS idx_previsoes_rodada     ON gold.previsoes_proximas_partidas (rodada);
CREATE INDEX IF NOT EXISTS idx_validadas_rodada     ON gold.previsoes_validadas (rodada);
CREATE INDEX IF NOT EXISTS idx_rebaixamento_pos     ON gold.analise_rebaixamento (posicao);
CREATE INDEX IF NOT EXISTS idx_pipeline_status      ON gold.pipeline_executions (status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_partidas_rodada      ON silver.partidas (rodada);

SELECT 'Medallion schemas criados com sucesso.' AS status;
