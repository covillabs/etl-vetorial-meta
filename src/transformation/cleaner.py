import hashlib
import pandas as pd


class DataCleaner:
    """Transforma dados brutos da Meta Marketing API em DataFrame normalizado."""

    def extract_action_value(self, actions_list: list, action_types: list[str]) -> int:
        """Soma valores de actions filtrados por tipo.

        Args:
            actions_list: Lista de dicts [{'action_type': str, 'value': str}].
            action_types: Tipos de action a serem somados.

        Returns:
            Soma inteira dos valores encontrados em actions_list que batem com action_types.
        """
        if not isinstance(actions_list, list):
            return 0
        return sum(
            int(float(a.get("value", 0)))
            for a in actions_list
            if a.get("action_type") in action_types
        )

    def transform(self, raw_data: list[dict]) -> pd.DataFrame:
        """Recebe JSON bruto da API, retorna DataFrame com colunas normalizadas.

        Fluxo:
            1. Extrai campos de texto (IDs, nomes, breakdowns)
            2. Converte métricas numéricas (spend, impressions)
            3. Processa lista de actions para leads, cliques, seguidores e vídeos
            4. Gera hash_id único para operação de UPSERT

        Args:
            raw_data: Lista de dicts retornada por MetaExtractor.get_ad_insights().

        Returns:
            DataFrame pronto para envio ao PostgresLoader.
        """
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        clean_df = pd.DataFrame()

        # -----------------------------------------------------------------
        # 1. CAMPOS DE TEXTO (IDs, Nomes e Breakdowns)
        # -----------------------------------------------------------------
        clean_df["id_anuncio"] = df["ad_id"]
        clean_df["data_registro"] = df["date_start"]
        clean_df["account_id"] = df["account_id"]
        clean_df["nome_conta"] = df["account_name"]
        clean_df["campanha"] = df["campaign_name"]
        clean_df["anuncio"] = df["ad_name"]
        clean_df["plataforma"] = df.get(
            "publisher_platform", pd.Series("unknown", index=df.index)
        ).fillna("unknown")
        clean_df["posicionamento"] = df.get(
            "platform_position", pd.Series("unknown", index=df.index)
        ).fillna("unknown")

        # -----------------------------------------------------------------
        # 2. MÉTRICAS NUMÉRICAS DIRETAS
        # -----------------------------------------------------------------
        clean_df["valor_gasto"] = (
            pd.to_numeric(df.get("spend"), errors="coerce").fillna(0).round(2)
        )
        clean_df["impressoes"] = (
            pd.to_numeric(df.get("impressions"), errors="coerce").fillna(0).astype(int)
        )

        # -----------------------------------------------------------------
        # 3. TRATAMENTO SEGURO DE ACTIONS
        # -----------------------------------------------------------------
        # Garante que cada célula seja uma lista, nunca NaN ou string
        actions_safe = df.get("actions", pd.Series(dtype=object)).apply(
            lambda x: x if isinstance(x, list) else []
        )

        # --- CLIQUES (inline raiz + link_click de actions) ---
        cliques_inline = (
            pd.to_numeric(df.get("inline_link_clicks"), errors="coerce")
            .fillna(0)
            .astype(int)
        )
        cliques_actions = actions_safe.apply(
            lambda x: self.extract_action_value(x, ["link_click"])
        )
        clean_df["clique_link"] = cliques_inline + cliques_actions

        # --- LEADS (3 origens unificadas) ---
        # Formulário: leads gerados dentro do Facebook/Instagram
        clean_df["lead_formulario"] = actions_safe.apply(
            lambda x: self.extract_action_value(
                x, ["lead", "onsite_conversion.lead_grouped", "onsite_conversion.lead"]
            )
        )
        # Site/Pixel: leads capturados via pixel no site externo
        clean_df["lead_site"] = actions_safe.apply(
            lambda x: self.extract_action_value(
                x, ["onsite_web_lead", "offsite_conversion.fb_pixel_lead"]
            )
        )
        # Mensagem: leads via WhatsApp/Direct/Messenger
        clean_df["lead_mensagem"] = actions_safe.apply(
            lambda x: self.extract_action_value(
                x,
                [
                    "onsite_conversion.messaging_first_reply",
                    "onsite_conversion.total_messaging_connection",
                ],
            )
        )
        # Total consolidado
        clean_df["lead"] = (
            clean_df["lead_formulario"]
            + clean_df["lead_site"]
            + clean_df["lead_mensagem"]
        )

        # --- SEGUIDORES (Instagram + Facebook) ---
        clean_df["seguidores_instagram"] = actions_safe.apply(
            lambda x: self.extract_action_value(
                x,
                [
                    "onsite_conversion.post_save_follow",
                    "instagram_follower_count_total",
                    "page_like",
                ],
            )
        )

        # --- VIDEO VIEWS ---
        # 3s: vem como 'video_view' dentro da lista de actions
        clean_df["videoview_3s"] = actions_safe.apply(
            lambda x: self.extract_action_value(x, ["video_view"])
        )
        # 50% e 75%: vêm como campos raiz do DataFrame (são listas de actions)
        for col_name, meta_field in [
            ("videoview_50", "video_p50_watched_actions"),
            ("videoview_75", "video_p75_watched_actions"),
        ]:
            if meta_field in df.columns:
                clean_df[col_name] = df[meta_field].apply(
                    lambda x: (
                        self.extract_action_value(x, ["video_view"])
                        if isinstance(x, list)
                        else 0
                    )
                )
            else:
                clean_df[col_name] = 0

        # -----------------------------------------------------------------
        # 4. HASH ID ÚNICO (Chave do UPSERT)
        # -----------------------------------------------------------------
        def generate_hash(row: pd.Series) -> str:
            base = f"{row['id_anuncio']}_{row['data_registro']}_{row['plataforma']}_{row['posicionamento']}"
            return hashlib.md5(base.encode()).hexdigest()

        clean_df["hash_id"] = clean_df.apply(generate_hash, axis=1)

        return clean_df
