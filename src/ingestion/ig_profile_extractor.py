import os
import requests
import logging
import pandas as pd
from datetime import datetime, timedelta


class InstagramProfileExtractor:
    """
    Conecta na Instagram Graph API para extrair métricas de crescimento do perfil.
    Foco: Capturar os seguidores ganhos diariamente (follows_and_unfollows).
    """

    def __init__(self, access_token: str, ig_account_id: str):
        self.access_token = access_token
        self.ig_account_id = ig_account_id
        self.base_url = "https://graph.facebook.com/v25.0"

    def get_daily_followers(self) -> pd.DataFrame:
        """
        Busca a métrica 'follows_and_unfollows' do dia anterior.
        Retorna um DataFrame pronto para a tabela 'instagram_crescimento'.
        """
        if not self.ig_account_id:
            logging.warning(
                "⚠️ IG_ACCOUNT_ID não fornecido. Pulando extração de seguidores do Instagram."
            )
            return pd.DataFrame()

        # Define o período da busca (D-1 para pegar o dia completo fechado)
        ontem = datetime.now() - timedelta(days=1)
        since = int(ontem.replace(hour=0, minute=0, second=0).timestamp())
        until = int(ontem.replace(hour=23, minute=59, second=59).timestamp())

        url = f"{self.base_url}/{self.ig_account_id}/insights"

        params = {
            "metric": "follows_and_unfollows",
            "period": "day",
            "metric_type": "total_value",
            "breakdown": "follow_type",
            "since": since,
            "until": until,
            "access_token": self.access_token,
        }

        try:
            logging.info(
                f"🔎 Buscando crescimento de seguidores (IG: {self.ig_account_id}) para {ontem.strftime('%Y-%m-%d')}..."
            )
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            seguidores_ganhos = 0

            # Navegação segura pelo JSON da Graph API
            if "data" in data and len(data["data"]) > 0:
                metric_data = data["data"][0].get("total_value", {})
                breakdowns = metric_data.get("breakdowns", [])

                if breakdowns:
                    results = breakdowns[0].get("results", [])
                    for result in results:
                        # Busca especificamente a dimensão "FOLLOWER" (quem seguiu de fato)
                        dim_values = result.get("dimension_values", [])
                        if "FOLLOWER" in dim_values:
                            seguidores_ganhos += int(result.get("value", 0))
                else:
                    # Fallback de segurança se a Meta não enviar a quebra
                    seguidores_ganhos = int(metric_data.get("value", 0))

            # Monta o DataFrame exato que o banco espera
            df = pd.DataFrame(
                [
                    {
                        "data_registro": ontem.strftime("%Y-%m-%d"),
                        "seguidores_ganhos": seguidores_ganhos,
                    }
                ]
            )

            logging.info(
                f"✅ Sucesso: +{seguidores_ganhos} seguidores ganhos no dia {ontem.strftime('%d/%m/%Y')}."
            )
            return df

        except requests.exceptions.HTTPError as err:
            logging.error(f"❌ Erro HTTP na API do Instagram: {err}")
            if err.response is not None:
                logging.error(f"Detalhe do erro: {err.response.text}")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"❌ Erro inesperado na extração do Instagram: {e}")
            return pd.DataFrame()
