import os

# Nome do arquivo de saída
OUTPUT_FILE = "CODIGO_FONTE_VETORIAL_ETL.txt"
ROOT_DIR = "."

# O que incluir
EXTENSIONS = {".py", ".yml", ".md", ".txt", ".env.example"}
EXACT_FILES = {"Dockerfile", "requirements.txt"}

# O que ignorar (pastas)
IGNORE_DIRS = {
    ".venv",
    ".git",
    ".agent",
    "__pycache__",
    ".github",
    ".ruff_cache",
    "data",
}


def export_project_code():
    print(f"📦 Exportando código fonte para: {OUTPUT_FILE}...")

    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as outfile:
            outfile.write(f"PROJETO EXPORTADO - VETORIAL ETL\n{'=' * 40}\n\n")

            for root, dirs, files in os.walk(ROOT_DIR):
                # Modifica a lista 'dirs' in-place para não descer nessas pastas
                dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]

                for file in files:
                    # Pula o próprio arquivo de saída
                    if file == OUTPUT_FILE:
                        continue

                    _, ext = os.path.splitext(file)
                    if ext in EXTENSIONS or file in EXACT_FILES:
                        file_path = os.path.join(root, file)

                        outfile.write(
                            f"\n{'=' * 60}\nARQUIVO: {file_path}\n{'=' * 60}\n"
                        )
                        try:
                            with open(
                                file_path, "r", encoding="utf-8", errors="ignore"
                            ) as infile:
                                outfile.write(infile.read())
                        except Exception as e:
                            outfile.write(f"\n[ERRO AO LER ARQUIVO: {e}]\n")
                        outfile.write("\n")

        print(f"✅ Sucesso! Código exportado em: {os.path.abspath(OUTPUT_FILE)}")

    except Exception as e:
        print(f"❌ Erro fatal ao exportar: {e}")


if __name__ == "__main__":
    export_project_code()
