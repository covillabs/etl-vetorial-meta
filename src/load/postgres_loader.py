import os
import json
import pandas as pd
from sqlalchemy import create_engine, text


# Colunas que o banco espera — usada como filtro de segurança
REQUIRED_COLUMNS = [
    "id_anuncio",
    "data_registro",
    "account_id",
    "nome_conta",
    "campanha",
    "anuncio",
    "plataforma",
    "posicionamento",
    "valor_gasto",
    "impressoes",
    "clique_link",
    "lead_formulario",
    "lead_site",
    "lead_mensagem",
    "seguidores_instagram",
    "videoview_3s",
    "videoview_50",
    "videoview_75",
    "lead",
    "hash_id",
    "raw_data",
]


class PostgresLoader:
    """Gerencia conexão e operações de UPSERT no PostgreSQL."""

    def __init__(self):
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASS")
        self.host = os.getenv("DB_HOST", "haproxy")
        self.port = os.getenv("DB_PORT", "5432")
        self.database = os.getenv("DB_NAME")

        self.engine = create_engine(
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}",
            pool_pre_ping=True,
            connect_args={"connect_timeout": 10},
        )

    def upsert_data(self, df: pd.DataFrame, raw_json_list: list[dict]) -> None:
        """Executa UPSERT no banco usando tabela temporária + ON CONFLICT.

        O método filtra dinamicamente as colunas do DataFrame para manter
        apenas as que existem em REQUIRED_COLUMNS, evitando que colunas
        extras (como reach ou ctr) quebrem a query.

        Args:
            df: DataFrame limpo vindo do DataCleaner.transform().
            raw_json_list: Lista de dicts brutos da API (para auditoria).
        """
        if df.empty:
            return

        # ---------------------------------------------------------
        # 1. TRATAMENTO PRÉVIO DE DADOS
        # ---------------------------------------------------------
        df = df.copy()
        df["raw_data"] = [json.dumps(r) for r in raw_json_list]

        # Preenche vazios numéricos com 0
        cols_numericas = [
            "valor_gasto",
            "impressoes",
            "clique_link",
            "lead_formulario",
            "lead_site",
            "lead_mensagem",
            "seguidores_instagram",
            "videoview_3s",
            "videoview_50",
            "videoview_75",
            "lead",
        ]
        for col in cols_numericas:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        # ---------------------------------------------------------
        # 2. FILTRO DE SEGURANÇA (Trava contra colunas extras)
        # ---------------------------------------------------------
        columns_to_load = [col for col in REQUIRED_COLUMNS if col in df.columns]

        missing = set(REQUIRED_COLUMNS) - set(df.columns)
        if missing:
            print(f"⚠️ [Load] AVISO: Colunas ausentes no DataFrame: {missing}")
            print("   O pipeline continuará, mas verifique o cleaner.py.")

        extra = set(df.columns) - set(REQUIRED_COLUMNS)
        if extra:
            print(f"ℹ️ [Load] Colunas ignoradas (não existem no banco): {extra}")

        df_filtered = df[columns_to_load]

        # ---------------------------------------------------------
        # 3. CARGA PARA O BANCO
        # ---------------------------------------------------------
        with self.engine.begin() as conn:
            print(f"📡 [Load] Enviando {len(df_filtered)} registros para o Postgres...")

            df_filtered.to_sql(
                "temp_meta_insights", conn, if_exists="replace", index=False
            )

            upsert_query = text("""
                INSERT INTO insights_meta_ads (
                    id_anuncio, data_registro, account_id, nome_conta, campanha,
                    anuncio, plataforma, posicionamento, valor_gasto, impressoes,
                    clique_link, lead_formulario, lead_site, lead_mensagem,
                    seguidores_instagram, videoview_3s, videoview_50, videoview_75,
                    lead, hash_id, raw_data
                )
                SELECT
                    id_anuncio,
                    CAST(data_registro AS DATE),
                    account_id, nome_conta, campanha,
                    anuncio, plataforma, posicionamento,
                    CAST(valor_gasto AS NUMERIC),
                    impressoes, clique_link, lead_formulario, lead_site, lead_mensagem,
                    seguidores_instagram, videoview_3s, videoview_50, videoview_75,
                    lead, hash_id,
                    CAST(raw_data AS JSONB)
                FROM temp_meta_insights
                ON CONFLICT (hash_id) DO UPDATE SET
                    valor_gasto = EXCLUDED.valor_gasto,
                    impressoes = EXCLUDED.impressoes,
                    clique_link = EXCLUDED.clique_link,
                    lead_formulario = EXCLUDED.lead_formulario,
                    lead_site = EXCLUDED.lead_site,
                    lead_mensagem = EXCLUDED.lead_mensagem,
                    seguidores_instagram = EXCLUDED.seguidores_instagram,
                    videoview_3s = EXCLUDED.videoview_3s,
                    videoview_50 = EXCLUDED.videoview_50,
                    videoview_75 = EXCLUDED.videoview_75,
                    lead = EXCLUDED.lead,
                    raw_data = EXCLUDED.raw_data,
                    data_insercao = CURRENT_TIMESTAMP;
            """)

            conn.execute(upsert_query)
            conn.execute(text("DROP TABLE IF EXISTS temp_meta_insights;"))
            print("✅ [Load] Carga concluída com sucesso!")
