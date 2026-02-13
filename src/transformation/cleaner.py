import pandas as pd
import os

class DataCleaner:
    @staticmethod
    def extract_action_value(actions, types_list):
        """Soma os valores de uma lista de tipos de actions específicos"""
        if not actions or not isinstance(actions, list):
            return 0
        return sum(int(a.get('value', 0)) for a in actions if a['action_type'] in types_list)

    def transform(self, raw_data):
        """Converte os dados brutos da Meta para o formato do nosso Banco"""
        df = pd.DataFrame(raw_data)
        clean_df = pd.DataFrame()

        # 1. Dados Básicos e IDs
        clean_df['id_anuncio'] = df.get('ad_id', 'N/A')
        clean_df['data_registro'] = df.get('date_start', 'N/A')
        clean_df['account_id'] = df.get('account_id', 'N/A')
        clean_df['nome_conta'] = df.get('account_name', 'N/A')
        clean_df['campanha'] = df.get('campaign_name', 'N/A')
        clean_df['anuncio'] = df.get('ad_name', 'N/A')
        clean_df['plataforma'] = df.get('publisher_platform', 'N/A')
        clean_df['posicionamento'] = df.get('platform_position', 'N/A')

        # 2. Métricas Financeiras e Alcance
        clean_df['valor_gasto'] = df.get('spend', 0).astype(float).round(2)
        clean_df['impressoes'] = df.get('impressions', 0).astype(int)
        clean_df['clique_link'] = df.get('inline_link_clicks', 0).astype(int)

        # 3. Processamento de Actions (Leads, Mensagens, Seguidores, Vídeo 3s)
        if 'actions' in df.columns:
            clean_df['lead_formulario'] = df['actions'].apply(
                lambda x: self.extract_action_value(x, ['lead', 'onsite_conversion.lead_grouped', 'onsite_web_lead'])
            )
            clean_df['lead_site'] = df['actions'].apply(
                lambda x: self.extract_action_value(x, ['offsite_conversion.fb_pixel_lead'])
            )
            clean_df['lead_mensagem'] = df['actions'].apply(
                lambda x: self.extract_action_value(x, ['onsite_conversion.messaging_first_reply'])
            )
            clean_df['seguidores_ganhos'] = df['actions'].apply(
                lambda x: self.extract_action_value(x, ['onsite_conversion.instagram_profile_followers'])
            )
            clean_df['videoview_3s'] = df['actions'].apply(
                lambda x: self.extract_action_value(x, ['video_view'])
            )
        else:
            for col in ['lead_formulario', 'lead_site', 'lead_mensagem', 'seguidores_ganhos', 'videoview_3s']:
                clean_df[col] = 0

        # 4. Métricas de Vídeo Avançadas (Corrigindo o erro de AttributeError)
        if 'video_p50_watched_actions' in df.columns:
            clean_df['videoview_50'] = df['video_p50_watched_actions'].apply(
                lambda x: self.extract_action_value(x, ['video_view'])
            )
        else:
            clean_df['videoview_50'] = 0

        if 'video_p75_watched_actions' in df.columns:
            clean_df['videoview_75'] = df['video_p75_watched_actions'].apply(
                lambda x: self.extract_action_value(x, ['video_view'])
            )
        else:
            clean_df['videoview_75'] = 0

        # 5. Consolidação de Leads Totais
        clean_df['lead'] = clean_df['lead_formulario'] + clean_df['lead_site'] + clean_df['lead_mensagem']

        # 6. Criar Hash ID Único
        clean_df['hash_id'] = (
            clean_df['id_anuncio'].astype(str) + "_" + 
            clean_df['data_registro'].astype(str) + "_" + 
            clean_df['plataforma'].astype(str) + "_" + 
            clean_df['posicionamento'].astype(str)
        )

        return clean_df