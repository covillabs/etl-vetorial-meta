import os
import json
import pandas as pd
from sqlalchemy import create_engine, text


class PostgresLoader:
    def __init__(self):
        # Coleta variáveis do ambiente
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASS")
        self.host = os.getenv("DB_HOST", "haproxy")
        self.port = os.getenv("DB_PORT", "5432")
        self.database = os.getenv("DB_NAME")

        # Criação do engine
        self.engine = create_engine(
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}",
            pool_pre_ping=True,
            connect_args={"connect_timeout": 10},
        )

    def upsert_data(self, df, raw_json_list):
        if df.empty:
            return

        # 1. Auditoria e Correção de Nomes (Briefing)
        df["raw_data"] = [json.dumps(r) for r in raw_json_list]
        # Garante que o nome da coluna bate com o banco da Vetorial
        df = df.rename(columns={"seguidores_ganhos": "seguidores_instagram"})

        with self.engine.begin() as conn:
            print(
                f"📡 [Load] Enviando {len(df)} registros para o Postgres (via Rede Interna)..."
            )

            # Cria tabela temporária
            df.to_sql("temp_meta_insights", conn, if_exists="replace", index=False)

            # 2. Query de UPSERT com CAST na data
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
                    CAST(data_registro AS DATE), -- <--- O SEGREDO ESTÁ AQUI (Converte Texto para Data)
                    account_id, nome_conta, campanha, 
                    anuncio, plataforma, posicionamento, valor_gasto, impressoes, 
                    clique_link, lead_formulario, lead_site, lead_mensagem, 
                    seguidores_instagram, videoview_3s, videoview_50, videoview_75, 
                    lead, hash_id, raw_data
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


if __name__ == "__main__":
    print("ℹ️ Este módulo deve ser chamado pelo script principal (main.py).")
