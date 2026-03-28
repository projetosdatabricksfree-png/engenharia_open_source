"""
Rebuild completo do dashboard Superset — Brasileirão 2026.
Usa as views diamond.* como fonte de dados.

Executar DENTRO do container Superset:
    python /app/superset_rebuild_dashboard.py
"""
import json
from superset.app import create_app

app = create_app()

with app.app_context():
    from superset import db
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    from superset.connectors.sqla.models import SqlaTable, TableColumn
    from superset.models.core import Database

    # ─────────────────────────────────────────────────────────────
    # 1. LIMPAR tudo existente
    # ─────────────────────────────────────────────────────────────
    print("Removendo dashboards, charts e datasets existentes...")
    for d in db.session.query(Dashboard).all():
        db.session.delete(d)
    for s in db.session.query(Slice).all():
        db.session.delete(s)
    for t in db.session.query(SqlaTable).all():
        db.session.delete(t)
    db.session.commit()
    print("  Limpo.")

    # ─────────────────────────────────────────────────────────────
    # 2. CONEXÃO com o banco de dados
    # ─────────────────────────────────────────────────────────────
    conn = db.session.query(Database).filter_by(database_name="Brasileirao").first()
    if not conn:
        raise RuntimeError("Conexão 'Brasileirao' não encontrada.")
    conn.allow_run_async = False
    db.session.commit()
    print(f"  DB id={conn.id} allow_run_async={conn.allow_run_async}")

    # ─────────────────────────────────────────────────────────────
    # 3. DATASETS (diamond views + gold views necessárias)
    # ─────────────────────────────────────────────────────────────
    def make_dataset(table_name, schema, description=""):
        t = SqlaTable(
            table_name=table_name,
            schema=schema,
            database_id=conn.id,
            description=description,
            is_sqllab_view=False,
        )
        db.session.add(t)
        db.session.flush()
        return t

    ds_previsoes   = make_dataset("previsoes_proximas_partidas", "diamond",
                                  "Previsões ML para a próxima rodada")
    ds_rebaixamento= make_dataset("analise_rebaixamento",        "diamond",
                                  "Análise de risco de rebaixamento por clube")
    ds_classificacao = make_dataset("vw_classificacao",          "gold",
                                  "Tabela de classificação completa")
    ds_desempenho  = make_dataset("vw_desempenho_modelo",        "gold",
                                  "Acurácia do modelo por rodada")
    db.session.commit()
    print(f"  Datasets criados: {ds_previsoes.id}, {ds_rebaixamento.id}, {ds_classificacao.id}, {ds_desempenho.id}")

    # ─────────────────────────────────────────────────────────────
    # Helper: query_context genérico
    # ─────────────────────────────────────────────────────────────
    def qctx(datasource_id, datasource_type, queries):
        return json.dumps({
            "datasource": {"id": datasource_id, "type": datasource_type},
            "force": False,
            "queries": queries,
            "result_format": "json",
            "result_type": "full",
        })

    def simple_query(datasource_id, columns, metrics=None, filters=None,
                     orderby=None, row_limit=50):
        return qctx(datasource_id, "table", [{
            "annotation_layers": [],
            "applied_time_extras": {},
            "columns": columns,
            "metrics": metrics or [],
            "filters": filters or [],
            "extras": {"having": "", "where": ""},
            "orderby": orderby or [],
            "row_limit": row_limit,
            "series_columns": [],
            "series_limit": 0,
            "series_limit_metric": None,
            "time_range": "No filter",
        }])

    # ─────────────────────────────────────────────────────────────
    # 4. CHARTS
    # ─────────────────────────────────────────────────────────────
    charts = []

    # ── Chart 1: Tabela de Classificação ────────────────────────
    params_classificacao = json.dumps({
        "datasource": f"{ds_classificacao.id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": ["posicao", "nome", "jogos", "pontos",
                        "vitorias", "empates", "derrotas",
                        "gols_pro", "gols_contra", "saldo_gols", "aproveitamento"],
        "order_by_cols": ['["posicao", true]'],
        "row_limit": 20,
        "table_timestamp_format": "%Y-%m-%d",
        "show_cell_bars": True,
        "color_pn": True,
        "column_config": {
            "aproveitamento": {"d3NumberFormat": ".1f"},
            "prob_rebaixamento": {"d3NumberFormat": ".1%"},
        },
    })
    c = Slice(
        slice_name="🏆 Classificação Brasileirão 2026",
        viz_type="table",
        datasource_id=ds_classificacao.id,
        datasource_type="table",
        params=params_classificacao,
        query_context=simple_query(
            ds_classificacao.id,
            ["posicao", "nome", "jogos", "pontos",
             "vitorias", "empates", "derrotas",
             "gols_pro", "gols_contra", "saldo_gols", "aproveitamento"],
            orderby=[["posicao", True]],
            row_limit=20,
        ),
    )
    db.session.add(c); db.session.flush(); charts.append(c)
    print(f"  Chart '{c.slice_name}' id={c.id}")

    # ── Chart 2: Risco de Rebaixamento (barras horizontais) ──────
    params_rebaixamento = json.dumps({
        "datasource": f"{ds_rebaixamento.id}__table",
        "viz_type": "echarts_bar",
        "query_mode": "aggregate",
        "groupby": ["nome_clube"],
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "prob_rebaixamento"},
                     "aggregate": "MAX", "label": "Prob. Rebaixamento"}],
        "order_desc": True,
        "row_limit": 20,
        "orientation": "horizontal",
        "show_value": True,
        "rich_tooltip": True,
        "color_scheme": "bnbColors",
        "x_axis_title": "Probabilidade de Rebaixamento",
        "y_axis_title": "Clube",
    })
    c = Slice(
        slice_name="📉 Risco de Rebaixamento por Clube",
        viz_type="echarts_bar",
        datasource_id=ds_rebaixamento.id,
        datasource_type="table",
        params=params_rebaixamento,
        query_context=qctx(ds_rebaixamento.id, "table", [{
            "annotation_layers": [], "applied_time_extras": {},
            "columns": ["nome_clube"],
            "metrics": [{"expressionType": "SIMPLE",
                         "column": {"column_name": "prob_rebaixamento"},
                         "aggregate": "MAX", "label": "Prob. Rebaixamento"}],
            "filters": [],
            "extras": {"having": "", "where": ""},
            "orderby": [["MAX(prob_rebaixamento)", False]],
            "row_limit": 20,
            "time_range": "No filter",
        }]),
    )
    db.session.add(c); db.session.flush(); charts.append(c)
    print(f"  Chart '{c.slice_name}' id={c.id}")

    # ── Chart 3: Previsões Próxima Rodada (tabela) ───────────────
    params_previsoes_tbl = json.dumps({
        "datasource": f"{ds_previsoes.id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": ["nome_casa", "nome_vis", "previsao",
                        "prob_casa", "prob_empate", "prob_visitante", "confianca"],
        "order_by_cols": ['["confianca", false]'],
        "row_limit": 10,
        "show_cell_bars": True,
        "color_pn": False,
        "column_config": {
            "prob_casa":       {"d3NumberFormat": ".0%"},
            "prob_empate":     {"d3NumberFormat": ".0%"},
            "prob_visitante":  {"d3NumberFormat": ".0%"},
            "confianca":       {"d3NumberFormat": ".0%"},
        },
    })
    c = Slice(
        slice_name="🔮 Previsões — Próxima Rodada",
        viz_type="table",
        datasource_id=ds_previsoes.id,
        datasource_type="table",
        params=params_previsoes_tbl,
        query_context=simple_query(
            ds_previsoes.id,
            ["nome_casa", "nome_vis", "previsao",
             "prob_casa", "prob_empate", "prob_visitante", "confianca"],
            orderby=[["confianca", False]],
            row_limit=10,
        ),
    )
    db.session.add(c); db.session.flush(); charts.append(c)
    print(f"  Chart '{c.slice_name}' id={c.id}")

    # ── Chart 4: Distribuição das Previsões (pie) ────────────────
    params_pie = json.dumps({
        "datasource": f"{ds_previsoes.id}__table",
        "viz_type": "pie",
        "groupby": ["previsao"],
        "metric": {"expressionType": "SIMPLE", "column": {"column_name": "id"},
                   "aggregate": "COUNT", "label": "Partidas"},
        "row_limit": 10,
        "sort_by_metric": True,
        "show_labels": True,
        "show_legend": True,
        "labels_outside": True,
        "label_type": "key_percent",
        "color_scheme": "bnbColors",
    })
    c = Slice(
        slice_name="📊 Distribuição das Previsões",
        viz_type="pie",
        datasource_id=ds_previsoes.id,
        datasource_type="table",
        params=params_pie,
        query_context=qctx(ds_previsoes.id, "table", [{
            "annotation_layers": [], "applied_time_extras": {},
            "columns": ["previsao"],
            "metrics": [{"expressionType": "SIMPLE",
                         "column": {"column_name": "id"},
                         "aggregate": "COUNT", "label": "Partidas"}],
            "filters": [],
            "extras": {"having": "", "where": ""},
            "orderby": [["COUNT(id)", False]],
            "row_limit": 10,
            "time_range": "No filter",
        }]),
    )
    db.session.add(c); db.session.flush(); charts.append(c)
    print(f"  Chart '{c.slice_name}' id={c.id}")

    # ── Chart 5: Pontos por Clube (ranking horizontal) ────────────
    params_pontos = json.dumps({
        "datasource": f"{ds_classificacao.id}__table",
        "viz_type": "echarts_bar",
        "query_mode": "raw",
        "groupby": ["nome"],
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "pontos"},
                     "aggregate": "MAX", "label": "Pontos"}],
        "order_desc": True,
        "row_limit": 20,
        "orientation": "horizontal",
        "show_value": True,
        "rich_tooltip": True,
        "color_scheme": "supersetColors",
    })
    c = Slice(
        slice_name="🥇 Pontuação por Clube",
        viz_type="echarts_bar",
        datasource_id=ds_classificacao.id,
        datasource_type="table",
        params=params_pontos,
        query_context=qctx(ds_classificacao.id, "table", [{
            "annotation_layers": [], "applied_time_extras": {},
            "columns": ["nome"],
            "metrics": [{"expressionType": "SIMPLE",
                         "column": {"column_name": "pontos"},
                         "aggregate": "MAX", "label": "Pontos"}],
            "filters": [],
            "extras": {"having": "", "where": ""},
            "orderby": [["MAX(pontos)", False]],
            "row_limit": 20,
            "time_range": "No filter",
        }]),
    )
    db.session.add(c); db.session.flush(); charts.append(c)
    print(f"  Chart '{c.slice_name}' id={c.id}")

    # ── Chart 6: Acurácia do Modelo por Rodada (linha) ───────────
    params_acuracia = json.dumps({
        "datasource": f"{ds_desempenho.id}__table",
        "viz_type": "echarts_timeseries_line",
        "query_mode": "raw",
        "x_axis": "rodada",
        "metrics": [{"expressionType": "SIMPLE",
                     "column": {"column_name": "acuracia_pct"},
                     "aggregate": "MAX", "label": "Acurácia (%)"}],
        "groupby": [],
        "row_limit": 50,
        "rich_tooltip": True,
        "show_value": True,
        "color_scheme": "supersetColors",
        "x_axis_title": "Rodada",
        "y_axis_title": "Acurácia (%)",
    })
    c = Slice(
        slice_name="🎯 Acurácia do Modelo por Rodada",
        viz_type="echarts_timeseries_line",
        datasource_id=ds_desempenho.id,
        datasource_type="table",
        params=params_acuracia,
        query_context=simple_query(
            ds_desempenho.id,
            ["rodada", "acuracia_pct"],
            orderby=[["rodada", True]],
            row_limit=50,
        ),
    )
    db.session.add(c); db.session.flush(); charts.append(c)
    print(f"  Chart '{c.slice_name}' id={c.id}")

    # ── Chart 7: Zona de Rebaixamento (tabela detalhada) ─────────
    params_zona = json.dumps({
        "datasource": f"{ds_rebaixamento.id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": ["posicao", "nome_clube", "pontos", "jogos",
                        "saldo_gols", "prob_rebaixamento", "zona_rebaixamento"],
        "order_by_cols": ['["posicao", true]'],
        "row_limit": 20,
        "show_cell_bars": True,
        "color_pn": True,
        "column_config": {
            "prob_rebaixamento": {"d3NumberFormat": ".1%"},
        },
    })
    c = Slice(
        slice_name="⚠️ Análise de Rebaixamento",
        viz_type="table",
        datasource_id=ds_rebaixamento.id,
        datasource_type="table",
        params=params_zona,
        query_context=simple_query(
            ds_rebaixamento.id,
            ["posicao", "nome_clube", "pontos", "jogos",
             "saldo_gols", "prob_rebaixamento", "zona_rebaixamento"],
            orderby=[["posicao", True]],
            row_limit=20,
        ),
    )
    db.session.add(c); db.session.flush(); charts.append(c)
    print(f"  Chart '{c.slice_name}' id={c.id}")

    # ── Chart 8: Confiança das Previsões (barras) ────────────────
    params_conf = json.dumps({
        "datasource": f"{ds_previsoes.id}__table",
        "viz_type": "echarts_bar",
        "query_mode": "raw",
        "groupby": ["nome_casa", "nome_vis"],
        "metrics": [{"expressionType": "SIMPLE",
                     "column": {"column_name": "confianca"},
                     "aggregate": "MAX", "label": "Confiança"}],
        "order_desc": True,
        "row_limit": 10,
        "orientation": "vertical",
        "show_value": True,
        "rich_tooltip": True,
        "color_scheme": "bnbColors",
        "x_axis_title": "Partida",
        "y_axis_title": "Confiança do Modelo",
        "xAxisLabelRotation": 30,
    })
    c = Slice(
        slice_name="💡 Confiança do Modelo por Partida",
        viz_type="echarts_bar",
        datasource_id=ds_previsoes.id,
        datasource_type="table",
        params=params_conf,
        query_context=qctx(ds_previsoes.id, "table", [{
            "annotation_layers": [], "applied_time_extras": {},
            "columns": ["nome_casa", "nome_vis"],
            "metrics": [{"expressionType": "SIMPLE",
                         "column": {"column_name": "confianca"},
                         "aggregate": "MAX", "label": "Confiança"}],
            "filters": [],
            "extras": {"having": "", "where": ""},
            "orderby": [["MAX(confianca)", False]],
            "row_limit": 10,
            "time_range": "No filter",
        }]),
    )
    db.session.add(c); db.session.flush(); charts.append(c)
    print(f"  Chart '{c.slice_name}' id={c.id}")

    db.session.commit()
    print(f"\n  Total: {len(charts)} charts criados.")

    # ─────────────────────────────────────────────────────────────
    # 5. DASHBOARD com layout organizado
    # ─────────────────────────────────────────────────────────────
    ids = [c.id for c in charts]
    # ids: [0]=Classificação [1]=Rebaixamento-bar [2]=Previsoes-tbl [3]=Pie
    #       [4]=Pontos-bar [5]=Acuracia [6]=Zona-tbl [7]=Confiança-bar

    position_json = json.dumps({
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "type": "ROOT",
            "id": "ROOT_ID",
            "children": ["GRID_ID"]
        },
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": ["ROW_header", "ROW_previsoes", "ROW_classificacao", "ROW_analise"],
            "parents": ["ROOT_ID"]
        },
        # ── Linha 1: Header ──
        "ROW_header": {
            "type": "ROW",
            "id": "ROW_header",
            "children": ["COL_titulo"],
            "parents": ["GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"}
        },
        "COL_titulo": {
            "type": "COLUMN",
            "id": "COL_titulo",
            "children": ["HEADER_1"],
            "parents": ["ROW_header"],
            "meta": {"width": 12}
        },
        "HEADER_1": {
            "type": "HEADER",
            "id": "HEADER_1",
            "children": [],
            "parents": ["COL_titulo"],
            "meta": {
                "text": "⚽ Brasileirão 2026 — Central de Inteligência",
                "headerSize": "HEADER_LARGE"
            }
        },
        # ── Linha 2: Previsões ──
        "ROW_previsoes": {
            "type": "ROW",
            "id": "ROW_previsoes",
            "children": ["COL_prev_tbl", "COL_prev_pie", "COL_prev_conf"],
            "parents": ["GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"}
        },
        "COL_prev_tbl": {
            "type": "COLUMN", "id": "COL_prev_tbl",
            "children": [f"CHART_{ids[2]}"],
            "parents": ["ROW_previsoes"],
            "meta": {"width": 6}
        },
        f"CHART_{ids[2]}": {
            "type": "CHART", "id": f"CHART_{ids[2]}",
            "children": [],
            "parents": ["COL_prev_tbl"],
            "meta": {"chartId": ids[2], "width": 6, "height": 350, "sliceName": charts[2].slice_name}
        },
        "COL_prev_pie": {
            "type": "COLUMN", "id": "COL_prev_pie",
            "children": [f"CHART_{ids[3]}"],
            "parents": ["ROW_previsoes"],
            "meta": {"width": 3}
        },
        f"CHART_{ids[3]}": {
            "type": "CHART", "id": f"CHART_{ids[3]}",
            "children": [],
            "parents": ["COL_prev_pie"],
            "meta": {"chartId": ids[3], "width": 3, "height": 350, "sliceName": charts[3].slice_name}
        },
        "COL_prev_conf": {
            "type": "COLUMN", "id": "COL_prev_conf",
            "children": [f"CHART_{ids[7]}"],
            "parents": ["ROW_previsoes"],
            "meta": {"width": 3}
        },
        f"CHART_{ids[7]}": {
            "type": "CHART", "id": f"CHART_{ids[7]}",
            "children": [],
            "parents": ["COL_prev_conf"],
            "meta": {"chartId": ids[7], "width": 3, "height": 350, "sliceName": charts[7].slice_name}
        },
        # ── Linha 3: Classificação ──
        "ROW_classificacao": {
            "type": "ROW",
            "id": "ROW_classificacao",
            "children": ["COL_class_tbl", "COL_class_pts"],
            "parents": ["GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"}
        },
        "COL_class_tbl": {
            "type": "COLUMN", "id": "COL_class_tbl",
            "children": [f"CHART_{ids[0]}"],
            "parents": ["ROW_classificacao"],
            "meta": {"width": 6}
        },
        f"CHART_{ids[0]}": {
            "type": "CHART", "id": f"CHART_{ids[0]}",
            "children": [],
            "parents": ["COL_class_tbl"],
            "meta": {"chartId": ids[0], "width": 6, "height": 500, "sliceName": charts[0].slice_name}
        },
        "COL_class_pts": {
            "type": "COLUMN", "id": "COL_class_pts",
            "children": [f"CHART_{ids[4]}"],
            "parents": ["ROW_classificacao"],
            "meta": {"width": 6}
        },
        f"CHART_{ids[4]}": {
            "type": "CHART", "id": f"CHART_{ids[4]}",
            "children": [],
            "parents": ["COL_class_pts"],
            "meta": {"chartId": ids[4], "width": 6, "height": 500, "sliceName": charts[4].slice_name}
        },
        # ── Linha 4: Análise de Rebaixamento ──
        "ROW_analise": {
            "type": "ROW",
            "id": "ROW_analise",
            "children": ["COL_rebaix_tbl", "COL_rebaix_bar", "COL_acuracia"],
            "parents": ["GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"}
        },
        "COL_rebaix_tbl": {
            "type": "COLUMN", "id": "COL_rebaix_tbl",
            "children": [f"CHART_{ids[6]}"],
            "parents": ["ROW_analise"],
            "meta": {"width": 5}
        },
        f"CHART_{ids[6]}": {
            "type": "CHART", "id": f"CHART_{ids[6]}",
            "children": [],
            "parents": ["COL_rebaix_tbl"],
            "meta": {"chartId": ids[6], "width": 5, "height": 500, "sliceName": charts[6].slice_name}
        },
        "COL_rebaix_bar": {
            "type": "COLUMN", "id": "COL_rebaix_bar",
            "children": [f"CHART_{ids[1]}"],
            "parents": ["ROW_analise"],
            "meta": {"width": 4}
        },
        f"CHART_{ids[1]}": {
            "type": "CHART", "id": f"CHART_{ids[1]}",
            "children": [],
            "parents": ["COL_rebaix_bar"],
            "meta": {"chartId": ids[1], "width": 4, "height": 500, "sliceName": charts[1].slice_name}
        },
        "COL_acuracia": {
            "type": "COLUMN", "id": "COL_acuracia",
            "children": [f"CHART_{ids[5]}"],
            "parents": ["ROW_analise"],
            "meta": {"width": 3}
        },
        f"CHART_{ids[5]}": {
            "type": "CHART", "id": f"CHART_{ids[5]}",
            "children": [],
            "parents": ["COL_acuracia"],
            "meta": {"chartId": ids[5], "width": 3, "height": 500, "sliceName": charts[5].slice_name}
        },
    })

    dash = Dashboard(
        dashboard_title="Brasileirão 2026 — Central de Inteligência",
        slug="brasileirao-2026",
        position_json=position_json,
        published=True,
        css="""
.dashboard-header { background: #1a1a2e; }
.header-title { color: #e8c547; font-weight: 900; }
""",
    )
    dash.slices = charts
    db.session.add(dash)
    db.session.commit()
    print(f"\n✅ Dashboard criado! id={dash.id} slug={dash.slug}")
    print(f"   URL: http://localhost:8088/superset/dashboard/{dash.slug}/")
