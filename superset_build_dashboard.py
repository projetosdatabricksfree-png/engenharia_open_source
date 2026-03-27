"""
Script para criar dashboard Brasileirão 2026 no Superset 3.1
Executar dentro do container superset:
  docker cp superset_build_dashboard.py superset:/tmp/
  docker exec superset python /tmp/superset_build_dashboard.py
"""
import json
import sys

sys.path.insert(0, "/app")

from superset.app import create_app

app = create_app()

with app.app_context():
    from superset import db
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard
    from superset.connectors.sqla.models import SqlaTable, TableColumn
    from superset.models.core import Database
    import sqlalchemy as sa

    # ── 1. Banco de dados ─────────────────────────────────────────────
    db_conn = db.session.query(Database).filter_by(database_name="Brasileirao").first()

    if not db_conn:
        db_conn = Database(
            database_name="Brasileirao",
            sqlalchemy_uri="postgresql+psycopg2://admin:admin@postgres:5432/brasileirao",
            expose_in_sqllab=True,
            allow_run_async=False,  # sem Celery worker; queries sincronas
            allow_ctas=False,
            allow_cvas=False,
            allow_dml=False,
            cache_timeout=300,
        )
        db.session.add(db_conn)
        db.session.commit()
        print(f"[OK] Database criado: id={db_conn.id}")
    else:
        print(f"[OK] Database existente: id={db_conn.id}")

    # ── 2. Datasets (views gold) ───────────────────────────────────────
    def get_or_create_dataset(table_name, schema="gold", description=""):
        ds = db.session.query(SqlaTable).filter_by(
            table_name=table_name,
            schema=schema,
            database_id=db_conn.id,
        ).first()
        if not ds:
            ds = SqlaTable(
                table_name=table_name,
                schema=schema,
                database_id=db_conn.id,
                description=description,
                is_sqllab_view=False,
            )
            db.session.add(ds)
            db.session.commit()
            ds.fetch_metadata()
            db.session.commit()
            print(f"  [+] Dataset criado: {schema}.{table_name} (id={ds.id})")
        else:
            print(f"  [~] Dataset existente: {schema}.{table_name} (id={ds.id})")
        return ds

    print("\n[2] Criando datasets...")
    ds_class  = get_or_create_dataset("vw_classificacao",     description="Classificação completa do Brasileirão")
    ds_prev   = get_or_create_dataset("vw_previsoes",          description="Previsões da próxima rodada")
    ds_desemp = get_or_create_dataset("vw_desempenho_modelo",  description="Acurácia do modelo por rodada")
    ds_elo    = get_or_create_dataset("vw_elo_ranking",        description="ELO Rating dos clubes")
    ds_rebx   = get_or_create_dataset("analise_rebaixamento",  description="Risco de rebaixamento por clube")

    # ── 3. Charts ──────────────────────────────────────────────────────
    print("\n[3] Criando charts...")

    def make_slice(name, viz_type, ds, params_dict):
        sl = Slice(
            slice_name=name,
            viz_type=viz_type,
            datasource_id=ds.id,
            datasource_type="table",
            params=json.dumps(params_dict),
            query_context="",
        )
        db.session.add(sl)
        db.session.commit()
        print(f"  [+] Chart: '{name}' (id={sl.id})")
        return sl

    # Chart 1 — Tabela de Classificação
    c1 = make_slice(
        "Tabela de Classificacao",
        "table",
        ds_class,
        {
            "viz_type": "table",
            "query_mode": "raw",
            "all_columns": [
                "posicao", "clube", "jogos", "pontos",
                "v", "e", "d", "gm", "gs", "sg",
                "aproveitamento", "elo", "situacao"
            ],
            "column_config": {
                "posicao":      {"label": "Pos",    "width": 50},
                "clube":        {"label": "Clube"},
                "jogos":        {"label": "J",      "width": 45},
                "pontos":       {"label": "Pts",    "width": 50},
                "v":            {"label": "V",      "width": 40},
                "e":            {"label": "E",      "width": 40},
                "d":            {"label": "D",      "width": 40},
                "gm":           {"label": "GM",     "width": 50},
                "gs":           {"label": "GS",     "width": 50},
                "sg":           {"label": "SG",     "width": 50},
                "aproveitamento": {"label": "Aprov%", "width": 70},
                "elo":          {"label": "ELO",    "width": 60},
                "situacao":     {"label": "Situação"},
            },
            "conditional_formatting": [
                {"column": "situacao", "operator": "==", "targetValue": "Libertadores",        "colorScheme": "#1a7a1a"},
                {"column": "situacao", "operator": "==", "targetValue": "Libertadores (pré)",  "colorScheme": "#2e8b57"},
                {"column": "situacao", "operator": "==", "targetValue": "Sul-Americana",       "colorScheme": "#4682b4"},
                {"column": "situacao", "operator": "==", "targetValue": "Rebaixamento",        "colorScheme": "#cc2222"},
            ],
            "include_search": True,
            "order_by_cols": [["posicao", True]],
            "row_limit": 25,
            "adhoc_filters": [],
            "time_range": "No filter",
        }
    )

    # Chart 2 — Risco de Rebaixamento
    c2 = make_slice(
        "Risco de Rebaixamento (%)",
        "echarts_bar",
        ds_rebx,
        {
            "viz_type": "echarts_bar",
            "x_axis": "nome_clube",
            "metrics": [{
                "expressionType": "SIMPLE",
                "column": {"column_name": "prob_rebaixamento"},
                "aggregate": "MAX",
                "label": "Risco (%)",
                "optionName": "metric_rebx",
            }],
            "groupby": [],
            "adhoc_filters": [],
            "row_limit": 20,
            "order_bars": True,
            "orientation": "horizontal",
            "show_legend": False,
            "color_scheme": "bnbColors",
            "x_axis_label": "Clube",
            "y_axis_label": "Probabilidade de Rebaixamento (%)",
            "rich_tooltip": True,
            "time_range": "No filter",
        }
    )

    # Chart 3 — ELO Rating
    c3 = make_slice(
        "ELO Rating dos Clubes",
        "echarts_bar",
        ds_elo,
        {
            "viz_type": "echarts_bar",
            "x_axis": "clube",
            "metrics": [{
                "expressionType": "SIMPLE",
                "column": {"column_name": "elo_rating"},
                "aggregate": "MAX",
                "label": "ELO Rating",
                "optionName": "metric_elo",
            }],
            "groupby": [],
            "adhoc_filters": [],
            "row_limit": 20,
            "order_bars": True,
            "orientation": "horizontal",
            "show_legend": False,
            "color_scheme": "supersetColors",
            "x_axis_label": "Clube",
            "y_axis_label": "ELO Rating",
            "rich_tooltip": True,
            "time_range": "No filter",
        }
    )

    # Chart 4 — Previsões da Próxima Rodada
    c4 = make_slice(
        "Previsoes - Proxima Rodada",
        "table",
        ds_prev,
        {
            "viz_type": "table",
            "query_mode": "raw",
            "all_columns": [
                "time_casa", "prob_casa_pct",
                "prob_empate_pct", "prob_visitante_pct",
                "time_visitante", "previsao", "confianca_pct"
            ],
            "column_config": {
                "time_casa":         {"label": "Mandante"},
                "prob_casa_pct":     {"label": "% Casa",      "width": 80},
                "prob_empate_pct":   {"label": "% Empate",    "width": 80},
                "prob_visitante_pct":{"label": "% Visitante", "width": 90},
                "time_visitante":    {"label": "Visitante"},
                "previsao":         {"label": "Previsão"},
                "confianca_pct":    {"label": "Confiança %",  "width": 90},
            },
            "include_search": False,
            "row_limit": 15,
            "adhoc_filters": [],
            "time_range": "No filter",
        }
    )

    # Chart 5 — Distribuição das Previsões
    c5 = make_slice(
        "Distribuicao das Previsoes",
        "pie",
        ds_prev,
        {
            "viz_type": "pie",
            "groupby": ["previsao"],
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "rodada"},
                "aggregate": "COUNT",
                "label": "Partidas",
                "optionName": "metric_count",
            },
            "adhoc_filters": [],
            "row_limit": 10,
            "sort_by_metric": True,
            "show_legend": True,
            "show_labels": True,
            "show_total": True,
            "label_type": "key_value_percent",
            "color_scheme": "supersetColors",
            "time_range": "No filter",
        }
    )

    # Chart 6 — Acurácia do Modelo por Rodada
    c6 = make_slice(
        "Acuracia do Modelo por Rodada",
        "echarts_timeseries_line",
        ds_desemp,
        {
            "viz_type": "echarts_timeseries_line",
            "x_axis": "rodada",
            "metrics": [{
                "expressionType": "SIMPLE",
                "column": {"column_name": "acuracia_pct"},
                "aggregate": "MAX",
                "label": "Acurácia (%)",
                "optionName": "metric_acc",
            }],
            "groupby": [],
            "adhoc_filters": [],
            "row_limit": 20,
            "show_legend": True,
            "rich_tooltip": True,
            "area": False,
            "markerEnabled": True,
            "smooth": True,
            "color_scheme": "supersetColors",
            "x_axis_label": "Rodada",
            "y_axis_label": "Acurácia (%)",
            "time_range": "No filter",
            "zoomable": False,
        }
    )

    # Chart 7 — Média de Pontos (Últimas 5 Rodadas)
    c7 = make_slice(
        "Media de Pontos (Ult. 5 Rodadas)",
        "echarts_bar",
        ds_elo,
        {
            "viz_type": "echarts_bar",
            "x_axis": "clube",
            "metrics": [{
                "expressionType": "SIMPLE",
                "column": {"column_name": "media_pontos_5j"},
                "aggregate": "MAX",
                "label": "Média Pontos",
                "optionName": "metric_pts",
            }],
            "groupby": [],
            "adhoc_filters": [],
            "row_limit": 20,
            "order_bars": True,
            "orientation": "horizontal",
            "show_legend": False,
            "color_scheme": "supersetColors",
            "x_axis_label": "Clube",
            "y_axis_label": "Média de Pontos (Ult. 5 Rodadas)",
            "rich_tooltip": True,
            "time_range": "No filter",
        }
    )

    # Chart 8 — Situação na Tabela
    c8 = make_slice(
        "Distribuicao por Situacao",
        "pie",
        ds_class,
        {
            "viz_type": "pie",
            "groupby": ["situacao"],
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "pontos"},
                "aggregate": "COUNT",
                "label": "Clubes",
                "optionName": "metric_cnt",
            },
            "adhoc_filters": [],
            "row_limit": 10,
            "sort_by_metric": True,
            "show_legend": True,
            "show_labels": True,
            "show_total": True,
            "label_type": "key_value",
            "color_scheme": "supersetColors",
            "time_range": "No filter",
        }
    )

    # ── 4. Dashboard ───────────────────────────────────────────────────
    print("\n[4] Criando dashboard...")

    position_json = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"children": ["GRID_ID"], "id": "ROOT_ID", "type": "ROOT"},
        "GRID_ID": {
            "children": ["ROW-title", "ROW-1", "ROW-2", "ROW-3", "ROW-4"],
            "id": "GRID_ID", "type": "GRID"
        },
        # Header
        "ROW-title": {
            "children": ["HEADER-1"],
            "id": "ROW-title", "type": "ROW",
            "meta": {"background": "BACKGROUND_TRANSPARENT"}
        },
        "HEADER-1": {
            "id": "HEADER-1", "type": "HEADER",
            "meta": {
                "text": "⚽ Brasileirão Série A 2026 — Central de Inteligência",
                "background": "BACKGROUND_TRANSPARENT"
            },
            "children": []
        },
        # Linha 1: Tabela de classificação full width
        "ROW-1": {
            "children": ["CHART-c1"],
            "id": "ROW-1", "type": "ROW",
            "meta": {"background": "BACKGROUND_TRANSPARENT"}
        },
        "CHART-c1": {
            "children": [], "id": "CHART-c1", "type": "CHART",
            "meta": {"chartId": c1.id, "height": 380, "sliceName": c1.slice_name, "width": 24}
        },
        # Linha 2: Risco Rebaixamento + ELO
        "ROW-2": {
            "children": ["CHART-c2", "CHART-c3"],
            "id": "ROW-2", "type": "ROW",
            "meta": {"background": "BACKGROUND_TRANSPARENT"}
        },
        "CHART-c2": {
            "children": [], "id": "CHART-c2", "type": "CHART",
            "meta": {"chartId": c2.id, "height": 320, "sliceName": c2.slice_name, "width": 12}
        },
        "CHART-c3": {
            "children": [], "id": "CHART-c3", "type": "CHART",
            "meta": {"chartId": c3.id, "height": 320, "sliceName": c3.slice_name, "width": 12}
        },
        # Linha 3: Previsões + Distribuição
        "ROW-3": {
            "children": ["CHART-c4", "CHART-c5"],
            "id": "ROW-3", "type": "ROW",
            "meta": {"background": "BACKGROUND_TRANSPARENT"}
        },
        "CHART-c4": {
            "children": [], "id": "CHART-c4", "type": "CHART",
            "meta": {"chartId": c4.id, "height": 300, "sliceName": c4.slice_name, "width": 16}
        },
        "CHART-c5": {
            "children": [], "id": "CHART-c5", "type": "CHART",
            "meta": {"chartId": c5.id, "height": 300, "sliceName": c5.slice_name, "width": 8}
        },
        # Linha 4: Acurácia + Média pontos + Situação
        "ROW-4": {
            "children": ["CHART-c6", "CHART-c7", "CHART-c8"],
            "id": "ROW-4", "type": "ROW",
            "meta": {"background": "BACKGROUND_TRANSPARENT"}
        },
        "CHART-c6": {
            "children": [], "id": "CHART-c6", "type": "CHART",
            "meta": {"chartId": c6.id, "height": 280, "sliceName": c6.slice_name, "width": 10}
        },
        "CHART-c7": {
            "children": [], "id": "CHART-c7", "type": "CHART",
            "meta": {"chartId": c7.id, "height": 280, "sliceName": c7.slice_name, "width": 10}
        },
        "CHART-c8": {
            "children": [], "id": "CHART-c8", "type": "CHART",
            "meta": {"chartId": c8.id, "height": 280, "sliceName": c8.slice_name, "width": 4}
        },
    }

    dash = Dashboard(
        dashboard_title="Brasileirão 2026 — Central de Inteligência",
        slug="brasileirao-2026",
        position_json=json.dumps(position_json),
        published=True,
    )
    dash.slices = [c1, c2, c3, c4, c5, c6, c7, c8]
    db.session.add(dash)
    db.session.commit()

    print(f"  [+] Dashboard criado: id={dash.id} | slug={dash.slug}")
    print(f"\n✅ Tudo criado com sucesso!")
    print(f"   URL: http://localhost:8088/superset/dashboard/{dash.slug}/")
    print(f"   IDs charts: {[c.id for c in [c1,c2,c3,c4,c5,c6,c7,c8]]}")
