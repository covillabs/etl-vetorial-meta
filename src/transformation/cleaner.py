import pandas as pd


class DataCleaner:
    @staticmethod
    def extract_action_value(actions, types_list):
        if not actions or not isinstance(actions, list):
            return 0
        return sum(
            int(a.get("value", 0)) for a in actions if a["action_type"] in types_list
        )

    def transform(self, raw_data):
        df = pd.DataFrame(raw_data)
        clean_df = pd.DataFrame()

        # Strings (Texto) - Seguro usar .get
        clean_df["id_anuncio"] = df.get("ad_id", "N/A")
        clean_df["data_registro"] = df.get("date_start", "N/A")
        clean_df["account_id"] = df.get("account_id", "N/A")
        clean_df["nome_conta"] = df.get("account_name", "N/A")
        clean_df["campanha"] = df.get("campaign_name", "N/A")
        clean_df["anuncio"] = df.get("ad_name", "N/A")
        clean_df["plataforma"] = df.get("publisher_platform", "N/A")
        clean_df["posicionamento"] = df.get("platform_position", "N/A")

        # Números - Verificação de Segurança (Evita o erro do astype)
        if "spend" in df.columns:
            clean_df["valor_gasto"] = df["spend"].astype(float).round(2)
        else:
            clean_df["valor_gasto"] = 0.0

        if "impressions" in df.columns:
            clean_df["impressoes"] = df["impressions"].astype(int)
        else:
            clean_df["impressoes"] = 0

        if "inline_link_clicks" in df.columns:
            clean_df["clique_link"] = df["inline_link_clicks"].astype(int)
        else:
            clean_df["clique_link"] = 0

        # Actions (Conversões)
        if "actions" in df.columns:
            clean_df["lead_formulario"] = df["actions"].apply(
                lambda x: self.extract_action_value(
                    x, ["lead", "onsite_conversion.lead_grouped", "onsite_web_lead"]
                )
            )
            clean_df["lead_site"] = df["actions"].apply(
                lambda x: self.extract_action_value(
                    x, ["offsite_conversion.fb_pixel_lead"]
                )
            )
            clean_df["lead_mensagem"] = df["actions"].apply(
                lambda x: self.extract_action_value(
                    x, ["onsite_conversion.messaging_first_reply"]
                )
            )
            clean_df["seguidores_ganhos"] = df["actions"].apply(
                lambda x: self.extract_action_value(
                    x, ["onsite_conversion.instagram_profile_followers"]
                )
            )
            clean_df["videoview_3s"] = df["actions"].apply(
                lambda x: self.extract_action_value(x, ["video_view"])
            )
        else:
            for col in [
                "lead_formulario",
                "lead_site",
                "lead_mensagem",
                "seguidores_ganhos",
                "videoview_3s",
            ]:
                clean_df[col] = 0

        # Vídeo Retenção
        if "video_p50_watched_actions" in df.columns:
            clean_df["videoview_50"] = df["video_p50_watched_actions"].apply(
                lambda x: self.extract_action_value(x, ["video_view"])
            )
        else:
            clean_df["videoview_50"] = 0

        if "video_p75_watched_actions" in df.columns:
            clean_df["videoview_75"] = df["video_p75_watched_actions"].apply(
                lambda x: self.extract_action_value(x, ["video_view"])
            )
        else:
            clean_df["videoview_75"] = 0

        # Consolidação
        clean_df["lead"] = (
            clean_df["lead_formulario"]
            + clean_df["lead_site"]
            + clean_df["lead_mensagem"]
        )

        # Hash ID (PK)
        clean_df["hash_id"] = (
            clean_df["id_anuncio"].astype(str)
            + "_"
            + clean_df["data_registro"].astype(str)
            + "_"
            + clean_df["plataforma"].astype(str)
            + "_"
            + clean_df["posicionamento"].astype(str)
        )

        return clean_df
