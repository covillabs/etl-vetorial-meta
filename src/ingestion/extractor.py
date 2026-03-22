import os
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount


class MetaExtractor:
    """Cliente da Meta Marketing API para extração de insights de anúncios."""

    def __init__(self, account_id: str):
        self.account_id = account_id
        self.access_token = os.getenv("META_ACCESS_TOKEN")
        FacebookAdsApi.init(access_token=self.access_token)

    def get_ad_insights(
        self, date_preset: str = "last_30d", time_range: dict = None
    ) -> list[dict]:
        """Extrai insights granulares por anúncio com breakdowns de plataforma.

        Args:
            date_preset: Janela de tempo da API (ex: 'last_30d', 'last_90d').
            time_range: Dict opcional {'since': 'YYYY-MM-DD', 'until': 'YYYY-MM-DD'}.
                        Se fornecido, ignora o date_preset.

        Returns:
            Lista de dicts com os dados brutos de cada anúncio/dia/plataforma.
        """
        account = AdAccount(self.account_id)

        fields = [
            "ad_id",
            "ad_name",
            "campaign_id",
            "campaign_name",
            "adset_id",
            "adset_name",
            "spend",
            "impressions",
            "inline_link_clicks",
            "outbound_clicks",
            "actions",
            "action_values",
            "date_start",
            "account_id",
            "account_name",
            "video_p50_watched_actions",
            "video_p75_watched_actions",
        ]

        params = {
            "level": "ad",
            "time_increment": 1,
            "limit": 500,
            "breakdowns": ["publisher_platform", "platform_position"],
            "action_breakdowns": ["action_type"],
        }

        if time_range:
            params["time_range"] = time_range
            label = f"range {time_range['since']} até {time_range['until']}"
        else:
            params["date_preset"] = date_preset
            label = date_preset

        print(f"📥 [Ingestion] Baixando dados da conta {self.account_id} ({label})...")

        try:
            insights = account.get_insights(fields=fields, params=params)
            data = [dict(insight) for insight in insights]
            print(f"✅ [Ingestion] {len(data)} linhas extraídas.")
            return data
        except Exception as e:
            print(f"❌ [Ingestion] Erro na conta {self.account_id}: {e}")
            if hasattr(e, "api_error_message"):
                print(f"   Detalhe API: {e.api_error_message()}")
            return []
