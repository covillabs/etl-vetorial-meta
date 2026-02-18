import os
import json
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

load_dotenv()


def deep_scan(account_id):
    FacebookAdsApi.init(access_token=os.getenv("META_ACCESS_TOKEN"))
    account = AdAccount(account_id)

    print(f"🔎 Iniciando Varredura Profunda na conta: {account_id}")

    # Vamos pegar uma janela maior e mais anúncios para garantir que achamos o dado
    params = {
        "level": "ad",
        "date_preset": "last_90d",
        "limit": 50,  # Aumentamos a amostra
    }

    # Campos que podem conter a "mágica" dos seguidores
    fields = ["ad_name", "actions", "action_values", "inline_post_engagement"]

    try:
        insights = account.get_insights(fields=fields, params=params)

        found_something = False
        for ad in insights:
            actions = ad.get("actions", [])
            for action in actions:
                # Se o valor for maior que 0, vamos analisar o nome da métrica
                if int(action.get("value", 0)) > 0:
                    found_something = True
                    print(f"🎯 Criativo: {ad['ad_name']}")
                    print(
                        f"   🔹 Métrica: {action['action_type']} | Valor: {action['value']}"
                    )

        if not found_something:
            print("❌ Nenhuma action com valor > 0 encontrada nos anúncios recentes.")

    except Exception as e:
        print(f"❌ Erro: {e}")


if __name__ == "__main__":
    # Teste com a conta que você sabe que tem seguidores no Gerenciador
    conta_teste = os.getenv("META_AD_ACCOUNT_IDS").split(",")[0]
    deep_scan(conta_teste)
