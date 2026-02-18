"""Teste offline do DataCleaner com dados mockados.

Roda sem acessar API ou banco. Valida que o cleaner:
  - Não crasheia com dados incompletos
  - Gera todas as colunas esperadas pelo postgres_loader
  - Nomeia seguidores como 'seguidores_instagram'
"""

import sys
import os

# Permite importar módulos do projeto a partir de scripts/diagnostics
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.transformation.cleaner import DataCleaner

if __name__ == "__main__":
    mock_data = [
        {
            "ad_id": "12345",
            "account_id": "123456",
            "account_name": "Conta Teste",
            "date_start": "2026-02-13",
            "spend": "45.20",
            "impressions": "1200",
            "inline_link_clicks": "0",
            "actions": [
                {
                    "action_type": "onsite_conversion.messaging_first_reply",
                    "value": "3",
                },
                {"action_type": "link_click", "value": "12"},
                {"action_type": "video_view", "value": "500"},
                {"action_type": "lead", "value": "2"},
                {"action_type": "onsite_web_lead", "value": "1"},
                {"action_type": "onsite_conversion.post_save_follow", "value": "5"},
            ],
            "publisher_platform": "instagram",
            "platform_position": "reels",
            "campaign_name": "Lancamento_Sarah_Fev",
            "ad_name": "Criativo_01_Video",
            "video_p50_watched_actions": [
                {"action_type": "video_view", "value": "200"}
            ],
            "video_p75_watched_actions": [
                {"action_type": "video_view", "value": "100"}
            ],
        }
    ]

    cleaner = DataCleaner()
    resultado = cleaner.transform(mock_data)

    print("\n" + "=" * 60)
    print("📋 RESULTADO DA TRANSFORMAÇÃO")
    print("=" * 60)

    print(f"\n📊 Colunas geradas ({len(resultado.columns)}):")
    for col in resultado.columns:
        print(f"   • {col}: {resultado[col].iloc[0]}")

    # Validação de schema
    from src.load.postgres_loader import REQUIRED_COLUMNS

    colunas_cleaner = set(resultado.columns)
    colunas_banco = set(REQUIRED_COLUMNS) - {
        "raw_data"
    }  # raw_data é adicionada no loader

    missing = colunas_banco - colunas_cleaner
    extra = colunas_cleaner - colunas_banco

    print(f"\n🔍 VALIDAÇÃO DE SCHEMA:")
    if missing:
        print(f"   ❌ Colunas faltando no cleaner: {missing}")
    if extra:
        print(f"   ⚠️ Colunas extras (ignoradas pelo loader): {extra}")
    if not missing and not extra:
        print("   ✅ Schema perfeito! Cleaner → Loader compatíveis.")

    # Validações específicas
    assert "seguidores_instagram" in resultado.columns, (
        "FALHA: coluna deveria ser 'seguidores_instagram'"
    )
    assert "reach" not in resultado.columns, "FALHA: 'reach' não deveria existir"
    assert "ctr" not in resultado.columns, "FALHA: 'ctr' não deveria existir"
    assert resultado["clique_link"].iloc[0] == 12, (
        "FALHA: clique_link deveria ser 12 (link_click de actions + 0 inline)"
    )
    assert resultado["seguidores_instagram"].iloc[0] == 5, (
        "FALHA: seguidores deveria ser 5"
    )
    assert resultado["videoview_3s"].iloc[0] == 500, (
        "FALHA: videoview_3s deveria ser 500"
    )
    assert resultado["videoview_50"].iloc[0] == 200, (
        "FALHA: videoview_50 deveria ser 200"
    )
    assert resultado["lead"].iloc[0] == 6, "FALHA: lead total deveria ser 6 (2+1+3)"

    print("\n✅ TODOS OS TESTES PASSARAM!")
