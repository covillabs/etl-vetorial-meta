import pandas as pd


class DataCleaner:
    @staticmethod
    def extract_action_value(actions, types_list):
        """Extrai valores de listas de ações da Meta com segurança contra Nulos"""
        if not actions or not isinstance(actions, list):
            return 0
        # Soma os valores convertendo para float e depois int para evitar erros de tipo
        return sum(
            int(float(a.get("value", 0)))
            for a in actions
            if a["action_type"] in types_list
        )

    def transform(self, raw_data):
        df = pd.DataFrame(raw_data)
        clean_df = pd.DataFrame()

        # --- TEXTOS (Uso de .get para evitar erro de coluna ausente) ---
        clean_df["id_anuncio"] = df.get("ad_id", "N/A")
        clean_df["data_registro"] = df.get("date_start", "N/A")
        clean_df["account_id"] = df.get("account_id", "N/A")
        clean_df["nome_conta"] = df.get("account_name", "N/A")
        clean_df["campanha"] = df.get("campaign_name", "N/A")
        clean_df["anuncio"] = df.get("ad_name", "N/A")
        clean_df["plataforma"] = df.get("publisher_platform", "N/A")
        clean_df["posicionamento"] = df.get("platform_position", "N/A")

        # --- NÚMEROS (A SOLUÇÃO PARA O ERRO NaN) ---
        # fillna(0) substitui o que estiver vazio por zero absoluto

        if "spend" in df.columns:
            clean_df["valor_gasto"] = df["spend"].fillna(0).astype(float).round(2)
        else:
            clean_df["valor_gasto"] = 0.0

        if "impressions" in df.columns:
            clean_df["impressoes"] = df["impressions"].fillna(0).astype(int)
        else:
            clean_df["impressoes"] = 0

        if "inline_link_clicks" in df.columns:
            clean_df["clique_link"] = df["inline_link_clicks"].fillna(0).astype(int)
        else:
            clean_df["clique_link"] = 0

        # --- TRATAMENTO DE AÇÕES (Leads e Conversões) ---
        if "actions" in df.columns:
            # Garante que se a coluna existir mas vier NaN, ela vire uma lista vazia
            actions_clean = df["actions"].apply(
                lambda x: x if isinstance(x, list) else []
            )

            clean_df["lead_formulario"] = actions_clean.apply(
                lambda x: self.extract_action_value(
                    x, ["lead", "onsite_conversion.lead_grouped", "onsite_web_lead"]
                )
            )
            clean_df["lead_site"] = actions_clean.apply(
                lambda x: self.extract_action_value(
                    x, ["offsite_conversion.fb_pixel_lead"]
                )
            )
            clean_df["lead_mensagem"] = actions_clean.apply(
                lambda x: self.extract_action_value(
                    x, ["onsite_conversion.messaging_first_reply"]
                )
            )
            clean_df["seguidores_ganhos"] = actions_clean.apply(
                lambda x: self.extract_action_value(
                    x, ["onsite_conversion.instagram_profile_followers"]
                )
            )
            clean_df["videoview_3s"] = actions_clean.apply(
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

        # --- VÍDEOS (Segurança adicional) ---
        for col, meta_field in [
            ("videoview_50", "video_p50_watched_actions"),
            ("videoview_75", "video_p75_watched_actions"),
        ]:
            if meta_field in df.columns:
                clean_df[col] = df[meta_field].apply(
                    lambda x: (
                        self.extract_action_value(x, ["video_view"])
                        if isinstance(x, list)
                        else 0
                    )
                )
            else:
                clean_df[col] = 0

        # Consolidação de Leads Total
        clean_df["lead"] = (
            clean_df["lead_formulario"]
            + clean_df["lead_site"]
            + clean_df["lead_mensagem"]
        )

        # Criação do Hash ID (Indispensável para o Upsert não duplicar dados)
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
