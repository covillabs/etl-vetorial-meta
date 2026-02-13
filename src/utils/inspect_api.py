import os
import time
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

load_dotenv()


def run_inspection(account_id):
    """Executa o diagnóstico com proteção de limite"""
    print(f"\n{'=' * 60}")
    print(f"🚀 PROCESSANDO: {account_id}")
    print(f"{'=' * 60}")

    account = AdAccount(account_id)

    try:
        # 1. Dados da Conta
        details = account.api_get(fields=["name", "currency"])
        print(f"✅ Conta: {details['name']} | Moeda: {details['currency']}")

        # 2. Instagram: Seguidores Totais
        pages = account.get_promote_pages(fields=["instagram_business_account"])
        for page in pages:
            ig = page.get("instagram_business_account")
            if ig:
                ig_data = ig.api_get(fields=["username", "followers_count"])
                print(
                    f"📸 IG: @{ig_data['username']} | Total Seguidores: {ig_data['followers_count']}"
                )

        # 3. Amostra de Actions (Reduzido para não estourar o limite)
        # Usamos 30 dias e apenas 5 anúncios para inspeção rápida
        params = {"level": "ad", "date_preset": "last_30d", "limit": 5}
        fields = ["ad_name", "actions"]

        insights = account.get_insights(fields=fields, params=params)

        print("\n--- 🔍 MAPEAMENTO DE ACTIONS ---")
        found_actions = set()
        for ad in insights:
            for action in ad.get("actions", []):
                found_actions.add(action["action_type"])

        if found_actions:
            for act in sorted(found_actions):
                print(f"🔹 {act}")
        else:
            print("ℹ️ Nenhuma action encontrada nesta amostra.")

    except Exception as e:
        print(f"❌ Erro na conta {account_id}: {e}")


def main():
    token = os.getenv("META_ACCESS_TOKEN")
    ids_string = os.getenv("META_AD_ACCOUNT_ID")

    if not token or not ids_string:
        print("❌ Erro: Verifique TOKEN e IDS no .env")
        return

    FacebookAdsApi.init(access_token=token)
    account_ids = [id.strip() for id in ids_string.split(",")]

    for idx, acc_id in enumerate(account_ids):
        run_inspection(acc_id)

        # Pausa de 10 segundos entre as contas (exceto na última)
        if idx < len(account_ids) - 1:
            print(f"\n☕ Pausando 10s para respeitar limites da API...")
            time.sleep(10)


if __name__ == "__main__":
    main()
