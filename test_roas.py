import os
import json
from dotenv import load_dotenv
from src.ingestion.extractor import MetaExtractor

def test_purchases():
    load_dotenv()
    accounts = os.getenv("META_AD_ACCOUNT_IDS", "").split(",")
    valid_accounts = [acc.strip() for acc in accounts if acc.strip()]
    
    if not valid_accounts:
        print("Nenhuma conta configurada.")
        return

    print("Testando contas em busca de COMPRAS (purchases)...")
    
    for acc in valid_accounts:
        print(f"\n--- Verificando Conta: {acc} ---")
        try:
            extractor = MetaExtractor(acc)
            # Pegando os últimos 30 dias
            data = extractor.get_ad_insights(date_preset="last_30d")
            
            purchases_found = 0
            total_purchase_value = 0.0
            
            for row in data:
                # Procura nas actions
                actions = row.get("actions", [])
                action_values = row.get("action_values", [])
                
                # Check for purchase in actions
                if isinstance(actions, list):
                    for action in actions:
                        if action.get("action_type") in ["purchase", "offsite_conversion.fb_pixel_purchase"]:
                            purchases_found += int(float(action.get("value", 0)))
                            
                # Check for purchase in action_values (ROAS/Valor)
                if isinstance(action_values, list):
                    for val in action_values:
                        if val.get("action_type") in ["purchase", "offsite_conversion.fb_pixel_purchase"]:
                            total_purchase_value += float(val.get("value", 0))
            
            print(f"Total de eventos de Compra Encontrados na API: {purchases_found}")
            print(f"Soma do Valor de Compras (Receita) na API: R$ {total_purchase_value:.2f}")
            
        except Exception as e:
            print(f"Erro ao verificar conta {acc}: {e}")

if __name__ == "__main__":
    test_purchases()
