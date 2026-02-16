import os
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from dotenv import load_dotenv

load_dotenv()


class MetaExtractor:
    def __init__(self, account_id):
        self.account_id = account_id
        self.access_token = os.getenv("META_ACCESS_TOKEN")
        FacebookAdsApi.init(access_token=self.access_token)

    def get_ad_insights(self, date_preset="last_3d"):
        account = AdAccount(self.account_id)

        # 1. CAMPOS (Métricas diretas)
        fields = [
            "ad_id",
            "ad_name",
            "campaign_name",
            "spend",
            "impressions",
            "inline_link_clicks",
            "actions",
            "date_start",
            "account_id",
            "account_name",
            "video_p50_watched_actions",
            "video_p75_watched_actions",
        ]

        # 2. BREAKDOWNS (Agrupadores - Correção exigida pelo Erro 400)
        # Usamos lista [] para evitar o Warning amarelo
        params = {
            "level": "ad",
            "date_preset": date_preset,
            "time_increment": 1,
            "limit": 500,
            "breakdowns": ["publisher_platform", "platform_position"],
        }

        print(
            f"📥 [Ingestion] Baixando dados da conta {self.account_id} ({date_preset})..."
        )

        try:
            insights = account.get_insights(fields=fields, params=params)
            data = [dict(insight) for insight in insights]
            print(f"✅ [Ingestion] Sucesso! {len(data)} linhas baixadas.")
            return data
        except Exception as e:
            print(f"❌ [Ingestion] Erro: {e}")
            if hasattr(e, "api_error_message"):
                print(f"   Detalhe API: {e.api_error_message()}")
            return []
