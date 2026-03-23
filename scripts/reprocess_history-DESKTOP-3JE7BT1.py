import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

# Ajusta o PYTHONPATH para garantir que ele ache o pacote src/ se executado da raiz
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ingestion.extractor import MetaExtractor
from src.transformation.cleaner import DataCleaner
from src.load.postgres_loader import PostgresLoader
from src.notification.discord_alert import DiscordAlert

load_dotenv()

# Definimos o range de novembro de 2025 em diante
ACCOUNTS = os.getenv("META_AD_ACCOUNT_IDS", "").split(",")
TIME_RANGE = {
    "since": "2025-11-01",
    "until": datetime.now().strftime("%Y-%m-%d"),
}

alert = DiscordAlert()

def run_historical_reprocess():
    start_time = datetime.now()
    print("\n" + "=" * 60)
    print(
        f"⏳ COVIL LABS - REPROCESSAMENTO HISTÓRICO - {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print("=" * 60)

    try:
        cleaner = DataCleaner()
        loader = PostgresLoader()

        total_processado = 0
        erros_lista = []

        # ==========================================
        # BLOCO DE ANÚNCIOS (META ADS)
        # ==========================================
        for account_id in ACCOUNTS:
            acc_id = account_id.strip()
            if not acc_id:
                continue

            print(f"\n🚀 Conta Ads: {acc_id} (Período: {TIME_RANGE['since']} até {TIME_RANGE['until']})")

            try:
                extractor = MetaExtractor(acc_id)
                
                # CUIDADO: se a conta for EXTREMAMENTE grande (dezena de milhares de anúncios ativos/pausados),
                # o limit da Meta pode precisar de paginação maior, mas o extractor default lida com requests.
                raw_data = extractor.get_ad_insights(time_range=TIME_RANGE)

                if not raw_data:
                    print("⚠️ Sem dados para esta conta.")
                    continue

                # 2. Transformação (agora com floats exatos, sem .round())
                clean_df = cleaner.transform(raw_data)
                
                # 3. UPSERT: Como a Primary Key é hash_id(ad + data + plataforma), ele vai atualizar
                # todos os registros das datas antigas sobrescrevendo a métrica exata!
                loader.upsert_data(clean_df, raw_data)

                total_processado += len(clean_df)
                print("✅ Histórico da conta finalizado.")
                time.sleep(2)

            except Exception as e:
                erro_msg = f"Falha no histórico da conta Ads {acc_id}: {e}"
                print(f"❌ {erro_msg}")
                erros_lista.append(erro_msg)

        # Não puxamos histórico de Instagram neste script porque a métrica follows_and_unfollows
        # não aceita "maximum" de forma retroativa infinita com perfeição sem paginação pesada/granular diária limite de 30 dias.

        end_time = datetime.now()
        duration = end_time - start_time

        msg_final = (
            f"**Reprocessamento Finalizado!**\n"
            f"⏱️ Duração da Execução: {duration}\n"
            f"📊 Histórico Atualizado: {total_processado} linhas"
        )
        print(f"\n🏁 {msg_final}")

        if erros_lista:
            alert.send(
                f"{msg_final}\n\n**Erros no Reprocessamento:**\n{chr(10).join(erros_lista)}", level="error"
            )

    except Exception as e_critico:
        msg_crash = f"💥 FALHA CRÍTICA NO REPROCESSAMENTO!\nErro: {str(e_critico)}"
        print(msg_crash)
        alert.send(msg_crash, level="error")


if __name__ == "__main__":
    print("🕰️ Iniciando script manual de Carga Histórica...")
    run_historical_reprocess()
