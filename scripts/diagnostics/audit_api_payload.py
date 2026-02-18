import os
import json
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

load_dotenv()


def audit_ads_data(account_id):
    # Inicializa a API
    token = os.getenv("META_ACCESS_TOKEN")
    FacebookAdsApi.init(access_token=token)
    account = AdAccount(account_id)

    print(f"\n{'=' * 60}")
    print(f"🔍 AUDITORIA DE DADOS - CONTA: {account_id}")
    print(f"{'=' * 60}")

    # Lista limpa de campos válidos para Insights v24.0
    fields = [
        "ad_id",
        "ad_name",
        "campaign_name",
        "spend",
        "impressions",
        "reach",
        "inline_link_clicks",
        "actions",
        "action_values",
        "date_start",
    ]

    params = {"level": "ad", "date_preset": "last_30d", "limit": 10}

    try:
        insights = account.get_insights(fields=fields, params=params)

        if not insights:
            print("❌ Nenhum dado retornado para esta conta nos últimos 30 dias.")
            return

        # 1. Análise do Primeiro Anúncio da Amostra
        sample_ad = insights[0]
        print("\n📦 ESTRUTURA DO TOPO DO JSON (Amostra):")
        for key, val in sample_ad.items():
            status = "✅ DADO" if val and val != "0" else "⚠️ ZERO/VAZIO"
            print(f"- {key: <20} | {status} | Exemplo: {val}")

        # 2. Varredura Profunda em 'Actions' (A "caixa preta" das conversões)
        print("\n🎯 MAPEAMENTO DETALHADO DE 'ACTIONS':")
        unique_actions = {}

        for ad in insights:
            actions = ad.get("actions", [])
            for action in actions:
                a_type = action["action_type"]
                a_val = int(action["value"])
                unique_actions[a_type] = unique_actions.get(a_type, 0) + a_val

        if unique_actions:
            # Ordenar por valor para ver o que tem mais volume
            for a_type, a_total in sorted(
                unique_actions.items(), key=lambda item: item[1], reverse=True
            ):
                print(f"🔹 {a_type: <45} | Total na Amostra: {a_total}")
        else:
            print("ℹ️ A lista de 'actions' veio vazia da API.")

    except Exception as e:
        print(f"❌ Erro na auditoria: {e}")


if __name__ == "__main__":
    # Pega o primeiro ID da sua lista no .env
    conta_teste = os.getenv("META_AD_ACCOUNT_IDS", "").split(",")[0]
    audit_ads_data(conta_teste)
