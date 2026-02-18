import os
import time
import schedule
from datetime import datetime
from dotenv import load_dotenv

# Importando Módulos
from src.ingestion.extractor import MetaExtractor
from src.transformation.cleaner import DataCleaner
from src.load.postgres_loader import PostgresLoader
from src.notification.discord_alert import DiscordAlert  # <--- O NOVO MÓDULO

# Configuração
load_dotenv()
ACCOUNTS = os.getenv("META_AD_ACCOUNT_IDS", "").split(",")
DATE_PRESET = "last_90d"

# Instancia o Alerta globalmente para usar no script
alert = DiscordAlert()


def run_etl_pipeline():
    start_time = datetime.now()
    print("\n" + "=" * 60)
    print(f"🏭 COVIL LABS - ETL PIPELINE VETORIAL - {start_time}")
    print("=" * 60)

    try:
        # Inicializa Workers
        cleaner = DataCleaner()
        loader = PostgresLoader()

        total_processado = 0
        erros_lista = []

        # Loop pelas Contas
        for account_id in ACCOUNTS:
            acc_id = account_id.strip()
            if not acc_id:
                continue

            print(f"\n🚀 Conta: {acc_id}")

            try:
                # 1. Extração
                extractor = MetaExtractor(acc_id)
                raw_data = extractor.get_ad_insights(date_preset=DATE_PRESET)

                if not raw_data:
                    print("⚠️ Sem dados (pausado/sem gasto).")
                    continue

                # 2. Transformação
                clean_df = cleaner.transform(raw_data)

                # 3. Carga
                loader.upsert_data(clean_df, raw_data)

                total_processado += len(clean_df)
                print("✅ Conta finalizada.")
                time.sleep(2)  # Pausa leve

            except Exception as e:
                erro_msg = f"Falha na conta {acc_id}: {e}"
                print(f"❌ {erro_msg}")
                erros_lista.append(erro_msg)

        # Relatório Final do Ciclo
        end_time = datetime.now()
        duration = end_time - start_time

        msg_final = (
            f"**Ciclo Finalizado!**\n"
            f"⏱️ Duração: {duration}\n"
            f"📊 Total Salvo: {total_processado} linhas"
        )
        print(f"🏁 {msg_final}")

        # Lógica de Notificação
        if erros_lista:
            # Se teve erro, manda alerta VERMELHO com os detalhes
            detalhes = "\n".join(erros_lista)
            alert.send(
                f"{msg_final}\n\n**Erros Encontrados:**\n{detalhes}", level="error"
            )
        else:
            # Se foi sucesso total, manda alerta VERDE (opcional, pode comentar se quiser silêncio)
            # alert.send(msg_final, level="info")
            pass

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
