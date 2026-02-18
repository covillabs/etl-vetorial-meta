import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


def test_connection():
    host = os.getenv("DB_HOST", "haproxy")
    port = os.getenv("DB_PORT", "5433")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    database = os.getenv("DB_NAME")

    print(f"🔍 Testando conexão em: {host}:{port}...")

    try:
        # Tenta conectar
        engine = create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{database}"
        )

        with engine.connect() as conn:
            # Executa um comando simples
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()
            print("=" * 50)
            print("✅ SUCESSO! O ETL consegue falar com o Banco.")
            print(f"🐘 Versão do Postgres: {version[0]}")
            print("=" * 50)

    except Exception as e:
        print("=" * 50)
        print("❌ FALHA NA CONEXÃO")
        error_msg = str(e)

        if "could not translate host name" in error_msg:
            print(
                "Causa provável: O nome 'haproxy' não é reconhecido. Verifique se o container está na mesma rede."
            )
        elif "Connection refused" in error_msg:
            print(
                f"Causa provável: A porta {port} está fechada ou o HAProxy não está ouvindo nela."
            )
        elif "password authentication failed" in error_msg:
            print("Causa provável: Usuário ou Senha do Banco estão incorretos.")
        else:
            print(f"Erro detalhado: {error_msg}")
        print("=" * 50)


if __name__ == "__main__":
    test_connection()
