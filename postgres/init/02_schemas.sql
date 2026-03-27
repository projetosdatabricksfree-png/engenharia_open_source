-- ============================================================
-- Medallion Architecture: bronze > silver > gold > diamond
-- Banco: brasileirao
-- ============================================================
\connect brasileirao;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Schemas (camadas do medalhao)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS diamond;

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
-- GOLD: feature store para machine learning
-- ============================================================
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

-- ============================================================
-- DIAMOND: previsoes, modelos e auditoria
-- ============================================================
CREATE TABLE IF NOT EXISTS diamond.previsoes_proximas_partidas (
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
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS diamond.previsoes_validadas (
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
    validated_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS diamond.analise_rebaixamento (
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

CREATE TABLE IF NOT EXISTS diamond.modelos_registry (
    id             BIGSERIAL PRIMARY KEY,
    modelo_nome    VARCHAR(200) NOT NULL,
    versao         VARCHAR(50) NOT NULL,
    algoritmo      VARCHAR(100),
    acuracia       DOUBLE PRECISION,
    f1_score       DOUBLE PRECISION,
    log_loss       DOUBLE PRECISION,
    rodada_treino  INTEGER,
    rodada_teste   INTEGER,
    parametros     JSONB,
    ativo          BOOLEAN DEFAULT FALSE,
    created_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS diamond.pipeline_executions (
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

CREATE TABLE IF NOT EXISTS diamond.data_quality_checks (
    id               BIGSERIAL PRIMARY KEY,
    check_name       VARCHAR(200) NOT NULL,
    tabela           VARCHAR(200),
    status           VARCHAR(50),
    records_checked  INTEGER,
    records_failed   INTEGER,
    detalhe          TEXT,
    checked_at       TIMESTAMP DEFAULT NOW()
);

-- Indices para performance nas consultas do BI
CREATE INDEX IF NOT EXISTS idx_previsoes_rodada   ON diamond.previsoes_proximas_partidas (rodada);
CREATE INDEX IF NOT EXISTS idx_rebaixamento_pos   ON diamond.analise_rebaixamento (posicao);
CREATE INDEX IF NOT EXISTS idx_validadas_rodada   ON diamond.previsoes_validadas (rodada);
CREATE INDEX IF NOT EXISTS idx_pipeline_status    ON diamond.pipeline_executions (status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_feature_rodada     ON gold.feature_store_enhanced (rodada, clube_id);
CREATE INDEX IF NOT EXISTS idx_partidas_rodada    ON silver.partidas (rodada);

SELECT 'Medallion schemas criados com sucesso.' AS status;
