import os
import json
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad

load_dotenv()


def audit_metadata(account_id):
    # Inicializa API
    token = os.getenv("META_ACCESS_TOKEN")
    FacebookAdsApi.init(access_token=token)
    account = AdAccount(account_id)

    print(f"\n{'=' * 60}")
    print(f"🕵️‍♂️ AUDITORIA DE CONFIGURAÇÕES - CONTA: {account_id}")
    print(f"{'=' * 60}")

    try:
        # 1. CHECAGEM DA JANELA DE ATRIBUIÇÃO (O motivo nº 1 de divergência)
        # O Gerenciador geralmente usa "7 dias clique / 1 dia visualização".
        # Vamos ver o que a API está usando por padrão.
        account_details = account.api_get(
            fields=["name", "attribution_spec", "currency"]
        )

        print(f"\n⚙️  CONFIGURAÇÃO DA CONTA:")
        print(f"   Nome: {account_details.get('name')}")
        print(f"   Moeda: {account_details.get('currency')}")

        attr_spec = account_details.get("attribution_spec")
        if attr_spec:
            print(
                f"   ⚠️ Janela de Atribuição Definida: {json.dumps(attr_spec, indent=2)}"
            )
        else:
            print(
                "   ℹ️ Janela de Atribuição: Padrão da Conta (Geralmente 7d clique / 1d view)"
            )

        # 2. CHECAGEM DE UTMS E RASTREAMENTO (Origem dos Contatos)
        # Vamos pegar os últimos 5 anúncios para ver se eles têm UTMs configuradas
        print(f"\n🔗 RASTREAMENTO DE URL (Amostra de 5 Anúncios Recentes):")

        ads = account.get_ads(
            fields=["name", "creative{url_tags, website_url}", "tracking_specs"],
            params={
                "limit": 5,
                "effective_status": ["ACTIVE"],
            },  # Pega só ativos se possível
        )

        if not ads:
            # Se não tiver ativos, pega qualquer um
            ads = account.get_ads(
                fields=["name", "creative{url_tags, website_url}"], params={"limit": 5}
            )

        for ad in ads:
            print(f"\n   🔸 Anúncio: {ad['name']}")
            creative = ad.get("creative", {})

            # Verifica UTMs (url_tags)
            utms = creative.get("url_tags")
            if utms:
                print(f"      ✅ UTMs Encontradas: {utms}")
            else:
                print(f"      ❌ Sem UTMs configuradas (url_tags vazio)")

            # Verifica URL Final
            url = creative.get("website_url")
            if url:
                print(f"      🌐 Destino: {url[:50]}...")  # Mostra só o começo
            else:
                print(f"      ⚠️ Sem URL de site explícita")

    except Exception as e:
        print(f"❌ Erro na auditoria: {e}")


if __name__ == "__main__":
    # Pega a primeira conta da lista
    primeira_conta = os.getenv("META_AD_ACCOUNT_IDS", "").split(",")[0]
    audit_metadata(primeira_conta)
