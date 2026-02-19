import os
import time
import schedule
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy.dialects.postgresql import insert

# Importando Módulos
from src.ingestion.extractor import MetaExtractor
from src.ingestion.ig_profile_extractor import InstagramProfileExtractor  # <--- NOVO
from src.transformation.cleaner import DataCleaner
from src.load.postgres_loader import PostgresLoader
from src.notification.discord_alert import DiscordAlert

# Configuração
load_dotenv()
ACCOUNTS = os.getenv("META_AD_ACCOUNT_IDS", "").split(",")
DATE_PRESET = "last_30d"

# Variáveis do Instagram
IG_ACCOUNT_ID = os.getenv("META_IG_ACCOUNT_ID")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

# Instancia o Alerta globalmente para usar no script
alert = DiscordAlert()


def run_etl_pipeline():
    start_time = datetime.now()
    print("\n" + "=" * 60)
    print(
        f"🏭 COVIL LABS - ETL PIPELINE VETORIAL - {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print("=" * 60)

    try:
        # Inicializa Workers
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
                # 1. Extração
                extractor = MetaExtractor(acc_id)
                raw_data = extractor.get_ad_insights(date_preset=DATE_PRESET)

                if not raw_data:
                    print("⚠️ Sem dados (pausado/sem gasto).")
                    continue

                # 2. Transformação
                clean_df = cleaner.transform(raw_data)

                # 3. Carga (Mantido o seu método original)
                loader.upsert_data(clean_df, raw_data)

                total_processado += len(clean_df)
                print("✅ Conta finalizada.")
                time.sleep(2)  # Pausa leve

            except Exception as e:
                erro_msg = f"Falha na conta Ads {acc_id}: {e}"
                print(f"❌ {erro_msg}")
                erros_lista.append(erro_msg)

        # ==========================================
        # 2. BLOCO DE SEGUIDORES (INSTAGRAM)
        # ==========================================
        seguidores_salvos = 0
        if IG_ACCOUNT_ID:
            print(f"\n📱 Extraindo Seguidores do Instagram (ID: {IG_ACCOUNT_ID})...")
            try:
                ig_extractor = InstagramProfileExtractor(
                    access_token=META_ACCESS_TOKEN, ig_account_id=IG_ACCOUNT_ID
                )
                df_seguidores = ig_extractor.get_daily_followers()

                if not df_seguidores.empty:
                    # Lógica de UPSERT direta e segura para a tabela instagram_crescimento
                    with loader.engine.connect() as conn:
                        data_values = df_seguidores.to_dict(orient="records")
                        stmt = insert(
                            pd.io.sql.get_schema(df_seguidores, "instagram_crescimento")
                        ).values(data_values)

                        # Atualiza caso o script rode duas vezes no mesmo dia
                        on_conflict_stmt = stmt.on_conflict_do_update(
                            index_elements=["data_registro"],
                            set_={"seguidores_ganhos": stmt.excluded.seguidores_ganhos},
                        )

                        conn.execute(on_conflict_stmt)
                        conn.commit()

                    seguidores_salvos = len(df_seguidores)
                    print(
                        f"✅ Seguidores atualizados com sucesso ({seguidores_salvos} registro inserido/atualizado)."
                    )
                else:
                    print("⚠️ Nenhum dado de seguidores retornado pela API hoje.")

            except Exception as e:
                erro_msg = f"Falha na extração do Instagram {IG_ACCOUNT_ID}: {e}"
                print(f"❌ {erro_msg}")
                erros_lista.append(erro_msg)
        else:
            print(
                "\n⚠️ META_IG_ACCOUNT_ID não configurado. Pulando extração de seguidores."
            )

        # ==========================================
        # RELATÓRIO FINAL E ALERTAS
        # ==========================================
        end_time = datetime.now()
        duration = end_time - start_time

        msg_final = (
            f"**Ciclo Finalizado!**\n"
            f"⏱️ Duração: {duration}\n"
            f"📊 Anúncios Salvos: {total_processado} linhas\n"
            f"📈 Crescimento IG Salvo: {'Sim' if seguidores_salvos > 0 else 'Não/Vazio'}"
        )
        print(f"\n🏁 {msg_final}")

        # Lógica de Notificação
        if erros_lista:
            # Se teve erro, manda alerta VERMELHO com os detalhes
            detalhes = "\n".join(erros_lista)
            alert.send(
                f"{msg_final}\n\n**Erros Encontrados:**\n{detalhes}", level="error"
            )
        else:
            # Se foi sucesso total, manda alerta VERDE (opcional)
            alert.send(msg_final, level="info")

    except Exception as e_critico:
        # Erro que derrubou o script todo (ex: banco fora do ar)
        msg_crash = f"💥 O ETL PAROU COMPLETAMENTE!\nErro: {str(e_critico)}"
        print(msg_crash)
        alert.send(msg_crash, level="error")


if __name__ == "__main__":
    print("🕰️ Iniciando Scheduler (4 em 4 horas)...")

    # Roda a primeira vez logo de cara
    run_etl_pipeline()

    # Agenda
    schedule.every(4).hours.do(run_etl_pipeline)

    print("💤 Aguardando próximo ciclo...")

    while True:
        schedule.run_pending()
        time.sleep(60)
