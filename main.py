import os
import time
import schedule
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy.dialects.postgresql import insert

# Importando Módulos
from src.ingestion.extractor import MetaExtractor
from src.ingestion.ig_profile_extractor import InstagramProfileExtractor
from src.transformation.cleaner import DataCleaner
from src.load.postgres_loader import PostgresLoader
from src.notification.discord_alert import DiscordAlert

# Configuração
load_dotenv()
ACCOUNTS = os.getenv("META_AD_ACCOUNT_IDS", "").split(",")
IG_ACCOUNT_IDS = os.getenv("META_IG_ACCOUNT_IDS", "").split(
    ","
)  # <--- Agora é uma lista!
DATE_PRESET = "last_30d"
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

# Instancia o Alerta globalmente para usar no script
alert = DiscordAlert()


def run_etl_pipeline():
    start_time = datetime.now()
    print("\n" + "=" * 60)
    print(f"🏭 COVIL LABS - ETL PIPELINE - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        # Inicializa Workers globais
        cleaner = DataCleaner()
        loader = PostgresLoader()

        total_processado = 0
        erros_lista = []

        # ==========================================
        # 1. BLOCO DE ANÚNCIOS (META ADS)
        # ==========================================
        for account_id in ACCOUNTS:
            acc_id = account_id.strip()
            if not acc_id:
                continue

            print(f"\n🚀 Conta Ads: {acc_id}")

            try:
                # Extração
                extractor = MetaExtractor(acc_id)
                raw_data = extractor.get_ad_insights(date_preset=DATE_PRESET)

                if not raw_data:
                    print("⚠️ Sem dados (pausado/sem gasto).")
                    continue

                # Transformação e Carga
                clean_df = cleaner.transform(raw_data)
                loader.upsert_data(clean_df, raw_data)

                total_processado += len(clean_df)
                print("✅ Conta finalizada.")
                time.sleep(2)

            except Exception as e:
                erro_msg = f"Falha na conta Ads {acc_id}: {e}"
                print(f"❌ {erro_msg}")
                erros_lista.append(erro_msg)

        # ==========================================
        # 2. BLOCO DE SEGUIDORES (INSTAGRAM MULTI-CONTA)
        # ==========================================
        seguidores_salvos = 0

        print("\n📱 Iniciando Extração de Seguidores do Instagram...")
        for ig_id_raw in IG_ACCOUNT_IDS:
            ig_id = ig_id_raw.strip()
            if not ig_id:
                continue

            print(f"\n   🔎 Extraindo IG ID: {ig_id}...")
            try:
                ig_extractor = InstagramProfileExtractor(
                    access_token=META_ACCESS_TOKEN, ig_account_id=ig_id
                )
                df_seguidores = ig_extractor.get_daily_followers()

                if not df_seguidores.empty:
                    # Lógica de UPSERT com Chave Primária Composta
                    with loader.engine.connect() as conn:
                        data_values = df_seguidores.to_dict(orient="records")
                        stmt = insert(
                            pd.io.sql.get_schema(df_seguidores, "instagram_crescimento")
                        ).values(data_values)

                        # Atualiza caso rode mais de uma vez no mesmo dia para a MESMA conta
                        on_conflict_stmt = stmt.on_conflict_do_update(
                            index_elements=[
                                "ig_account_id",
                                "data_registro",
                            ],  # <--- A mágica da chave composta
                            set_={"seguidores_ganhos": stmt.excluded.seguidores_ganhos},
                        )

                        conn.execute(on_conflict_stmt)
                        conn.commit()

                    seguidores_salvos += len(df_seguidores)
                    print(f"   ✅ Seguidores da conta {ig_id} atualizados com sucesso.")
                else:
                    print(f"   ⚠️ Nenhum dado retornado para a conta {ig_id} hoje.")

            except Exception as e:
                erro_msg = f"Falha na extração do Instagram {ig_id}: {e}"
                print(f"   ❌ {erro_msg}")
                erros_lista.append(erro_msg)

        if seguidores_salvos == 0 and not any(ig.strip() for ig in IG_ACCOUNT_IDS):
            print(
                "⚠️ Nenhuma conta de Instagram configurada no .env (META_IG_ACCOUNT_IDS)."
            )

        # ==========================================
        # 3. RELATÓRIO FINAL E ALERTAS
        # ==========================================
        end_time = datetime.now()
        duration = end_time - start_time

        msg_final = (
            f"**Ciclo Finalizado!**\n"
            f"⏱️ Duração: {duration}\n"
            f"📊 Anúncios Salvos: {total_processado} linhas\n"
            f"📈 IG Contas Salvas: {seguidores_salvos}"
        )
        print(f"\n🏁 {msg_final}")

        if erros_lista:
            detalhes = "\n".join(erros_lista)
            alert.send(
                f"{msg_final}\n\n**Erros Encontrados:**\n{detalhes}", level="error"
            )
        else:
            # alert.send(msg_final, level="info") # Descomente se quiser receber notificação a cada ciclo bem sucedido
            pass

    except Exception as e_critico:
        msg_crash = f"💥 O ETL PAROU COMPLETAMENTE!\nErro: {str(e_critico)}"
        print(msg_crash)
        alert.send(msg_crash, level="error")


if __name__ == "__main__":
    print("🕰️ Iniciando Scheduler (4 em 4 horas)...")
    run_etl_pipeline()
    schedule.every(4).hours.do(run_etl_pipeline)
    print("💤 Aguardando próximo ciclo...")
    while True:
        schedule.run_pending()
        time.sleep(60)
