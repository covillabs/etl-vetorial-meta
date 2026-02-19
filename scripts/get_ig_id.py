import os
import requests
from dotenv import load_dotenv

load_dotenv()


def find_instagram_id():
    token = os.getenv("META_ACCESS_TOKEN")
    if not token:
        print("❌ Token não encontrado no .env")
        return

    url = "https://graph.facebook.com/v25.0/me/accounts"
    params = {"access_token": token, "fields": "name,instagram_business_account"}

    print("🔍 Verificando permissões do Token e buscando ID do Instagram...")

    try:
        response = requests.get(url, params=params)
        data = response.json()

        if "error" in data:
            print(
                f"❌ Erro na API (Seu token não tem permissão para ler Páginas): {data['error']['message']}"
            )
            return

        pages = data.get("data", [])
        if not pages:
            print(
                "⚠️ Seu token é válido, mas não tem nenhuma Página do Facebook vinculada a ele no Gerenciador de Negócios."
            )
            return

        for page in pages:
            page_name = page.get("name")
            ig_account = page.get("instagram_business_account")

            print(f"\n📄 Página do FB: {page_name}")
            if ig_account:
                print(f"   ✅ ID DO INSTAGRAM ENCONTRADO: {ig_account['id']}")
                print(
                    f"   👉 Copie esse número e coloque no seu .env como META_IG_ACCOUNT_ID={ig_account['id']}"
                )
            else:
                print("   ❌ Nenhum Instagram Comercial vinculado a esta página.")

    except Exception as e:
        print(f"Erro no script: {e}")


if __name__ == "__main__":
    find_instagram_id()
