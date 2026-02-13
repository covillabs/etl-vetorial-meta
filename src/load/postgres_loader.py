import os
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

class PostgresLoader:
    def __init__(self):
        self.conn_params = {
            "host": os.getenv("DB_HOST", "patroni_primary"),
            "port": os.getenv("DB_PORT", "5432"),
            "database": os.getenv("DB_NAME", "relatorio_meta_ads"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASS")
        }

    def get_connection(self):
        """Cria uma conexão com o banco de dados na Hetzner"""
        return psycopg2.connect(**self.conn_params)

    def upsert_data(self, df, raw_json_list):
        """
        Realiza o UPSERT dos dados limpos e anexa o JSON bruto para auditoria.
        df: DataFrame do Pandas (saída do cleaner)
        raw_json_list: Lista original de dicionários (saída da Meta API)
        """
        # Adicionamos o JSON bruto de cada linha como uma coluna no DataFrame antes da carga
        df['raw_data'] = [json.dumps(r) for r in raw_json_list]

        # Mapeamento das colunas do DataFrame para as colunas do seu Banco
        columns = [
            'id_anuncio', 'data_registro', 'account_id', 'nome_conta', 
            'campanha', 'anuncio', 'plataforma', 'posicionamento', 
            'valor_gasto', 'impressoes', 'clique_link', 'lead_formulario', 
            'lead_site', 'lead_mensagem', 'seguidores_ganhos', 'videoview_3s', 
            'videoview_50', 'videoview_75', 'lead', 'hash_id', 'raw_data'
        ]

        # Query de UPSERT (Sintaxe PostgreSQL)
        query = f"""
            INSERT INTO insights_meta_ads ({", ".join(columns)})
            VALUES %s
            ON CONFLICT (hash_id) DO UPDATE SET
                valor_gasto = EXCLUDED.valor_gasto,
                impressoes = EXCLUDED.impressoes,
                clique_link = EXCLUDED.clique_link,
                lead_formulario = EXCLUDED.lead_formulario,
                lead_site = EXCLUDED.lead_site,
                lead_mensagem = EXCLUDED.lead_mensagem,
                seguidores_ganhos = EXCLUDED.seguidores_ganhos,
                videoview_3s = EXCLUDED.videoview_3s,
                videoview_50 = EXCLUDED.videoview_50,
                videoview_75 = EXCLUDED.videoview_75,
                lead = EXCLUDED.lead,
                raw_data = EXCLUDED.raw_data,
                data_insercao = CURRENT_TIMESTAMP;
        """

        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # Prepara os valores para o psycopg2
                data_to_insert = [tuple(x) for x in df[columns].values]
                
                print(f"📡 Enviando {len(data_to_insert)} registros para o Postgres...")
                execute_values(cur, query, data_to_insert)
                conn.commit()
                print("✅ Carga concluída com sucesso no banco relatorio_meta_ads!")
        except Exception as e:
            conn.rollback()
            print(f"❌ Erro catastrófico na carga: {e}")
            raise e
        finally:
            conn.close()

if __name__ == "__main__":
    print("ℹ️ Este módulo deve ser chamado pelo script principal (main.py).")