import os
import time
from dotenv import load_dotenv

# Importando nossos módulos operários
from src.ingestion.extractor import MetaExtractor
from src.transformation.cleaner import DataCleaner
from src.load.postgres_loader import PostgresLoader

# Configuração de Ambiente
load_dotenv()
ACCOUNTS = os.getenv("META_AD_ACCOUNT_IDS", "").split(",")

# CONFIGURAÇÃO DE JANELA DE TEMPO
# 'last_90d': Pega os últimos 3 meses.
# Ideal para rodar diariamente: Cobre os 2 meses do briefing + atribuições tardias.
# Eficiência: A API da Meta já ignora campanhas antigas sem atividade.
DATE_PRESET = "last_90d"


def main():
    print("=" * 60)
    print("🏭 COVIL LABS - ETL PIPELINE VETORIAL")
    print(f"📅 Janela de Dados: {DATE_PRESET}")
    print("=" * 60)

    # 1. Inicializa Ferramentas
    cleaner = DataCleaner()
    loader = PostgresLoader()

    total_registros_processados = 0

    # 2. Loop de Produção (Itera sobre as contas)
    for account_id in ACCOUNTS:
        acc_id = account_id.strip()
        if not acc_id:
            continue

        print(f"\n🚀 Processando Conta: {acc_id}")

        # --- FASE 1: INGESTION (Extração) ---
        extractor = MetaExtractor(acc_id)
        raw_data = extractor.get_ad_insights(date_preset=DATE_PRESET)

        if not raw_data:
            print(
                "⚠️ Nenhum dado encontrado (Campanhas pausadas ou sem gasto). Pulando..."
            )
            continue

        # --- FASE 2: TRANSFORMATION (Limpeza) ---
        print("⚙️ [Transformation] Normalizando dados...")
        try:
            clean_df = cleaner.transform(raw_data)
        except Exception as e:
            print(f"❌ Erro na transformação: {e}")
            continue

        # --- FASE 3: LOAD (Carga) ---
        print(f"💾 [Load] Enviando {len(clean_df)} linhas para o Postgres...")
        try:
            loader.upsert_data(clean_df, raw_data)
            total_registros_processados += len(clean_df)
            print("✅ Sucesso!")
        except Exception as e:
            print(f"❌ Erro crítico ao salvar no Banco: {e}")

        # Pausa de cortesia para a API (Rate Limit)
        print("☕ Pausando 5s...")
        time.sleep(5)

    print("\n" + "=" * 60)
    print(
        f"🏁 FIM DO PROCESSO. Total de registros atualizados/inseridos: {total_registros_processados}"
    )
    print("=" * 60)


if __name__ == "__main__":
    main()
