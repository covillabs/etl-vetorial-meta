 --- BLOCO DE TESTE ---
if __name__ == "__main__":
    mock_data = [{
        'ad_id': '12345',
        'account_id': '123456',
        'date_start': '2026-02-13',
        'spend': '45.20',
        'impressions': '1200',
        'inline_link_clicks': '45',
        'actions': [{'action_type': 'onsite_conversion.messaging_first_reply', 'value': '3'}],
        'publisher_platform': 'instagram',
        'platform_position': 'reels',
        'campaign_name': 'Lancamento_Sarah_Fev',
        'ad_name': 'Criativo_01_Video'
        # Note que não enviamos video_p50_watched_actions aqui para testar a resiliência
    }]
    
    cleaner = DataCleaner()
    resultado = cleaner.transform(mock_data)
    
    os.makedirs('data/processed', exist_ok=True)
    resultado.to_csv('data/processed/teste_limpeza.csv', index=False)
    
    print("\n✅ Transformação concluída com sucesso!")
    print(resultado[['anuncio', 'videoview_50', 'hash_id']].to_string(index=False))