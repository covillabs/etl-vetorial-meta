# Vetorial ETL - Facebook Ads Integration

Este projeto é um pipeline ETL (Extract, Transform, Load) robusto, focado na extração e processamento de dados da API do Facebook Ads (Meta Marketing API). O pipeline está totalmente operacional e pronto para produção.

## 🚀 Status do Projeto

Atualmente, o projeto está na fase **Operacional**.
O ciclo completo de ETL está implementado:

- **Extração (E):** Baixa insights granulares por anúncio, plataforma e posicionamento.
- **Transformação (T):** Limpa, padroniza e agrega métricas de conversão e vídeo.
- **Carga (L):** Persiste os dados no PostgreSQL usando estratégia de UPSERT (idempotência).

## 📂 Estrutura do Projeto

```
vetorial-etl/
├── main.py             # Ponto de entrada (Executa o fluxo completo)
├── Dockerfile          # Configuração para containerização
├── requirements.txt    # Dependências do projeto
├── .env                # Variáveis de ambiente (Tokens, IDs, Banco)
├── src/
│   ├── ingestion/      # Scripts de extração
│   │   └── extractor.py # Cliente da API da Meta
│   ├── transformation/ # Scripts de transformação de dados
│   │   └── cleaner.py  # Padronização e limpeza de dados
│   ├── load/           # Scripts de carga
│   │   └── postgres_loader.py # Carga no PostgreSQL (Upsert)
│   └── utils/
│       ├── inspect_api.py   # Script de diagnóstico da API
│       └── test_pipeline.py # Script de teste de integridade
├── data/               # Diretório para dados temporários ou locais
└── note.txt            # Logs de inspeção e exemplos de retorno
```

## 🛠️ Instalação e Configuração

1.  **Requisitos:**
    - Python 3.10+
    - Docker (Opcional, para rodar em container)
    - Banco de Dados PostgreSQL
    - Conta de Desenvolvedor Meta com App criado e Token de Acesso.

2.  **Instalação Local:**

    ```bash
    pip install -r requirements.txt
    ```

3.  **Configuração:**
    Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

    ```env
    # Credenciais Meta
    META_ACCESS_TOKEN=seu_token_v4
    META_AD_ACCOUNT_IDS=act_xxxxxxxx,act_yyyyyyyy

    # Credenciais Banco de Dados (Postgres)
    DB_HOST=localhost
    DB_NAME=seu_banco
    DB_USER=seu_usuario
    DB_PASS=sua_senha
    DB_PORT=5432
    ```

## ⚡ Como Executar

### Execução Direta (Local)

Para rodar o pipeline completo e atualizar o banco de dados:

```bash
python main.py
```

### via Docker

O projeto está pronto para ser rodado como um container:

1. **Build da imagem:**

   ```bash
   docker build -t vetorial-etl .
   ```

2. **Rodar o container:**
   ```bash
   docker run --env-file .env vetorial-etl
   ```

## 🔍 Scripts e Módulos

### `main.py`

O orquestrador central. Ele itera sobre todas as contas listadas no `.env`, chama o extrator, passa os dados para o limpador e envia o resultado final para o banco de dados.

### `src/ingestion/extractor.py`

Interface com a `facebook_business` SDK. Solicita métricas de entrega, gasto e conversões nos níveis de plataforma e posicionamento.

### `src/transformation/cleaner.py`

Responsável pela inteligência de negócio. Converte o JSON bruto da Meta em um DataFrame estruturado, calculando leads consolidados e métricas de retenção de vídeo.

### `src/load/postgres_loader.py`

Gerencia o banco de dados. Utiliza o `hash_id` para garantir que os dados sejam atualizados no banco sem duplicidade, mesmo que o script seja rodado múltiplas vezes no mesmo dia.

---

## 📏 Regras de Negócio (Business Rules)

Esta seção serve como guia oficial para a padronização das métricas vindas de diferentes origens em nomes únicos no banco de dados.

### Mapeamento de Métricas

| Métrica no Banco        | Nomes Técnicos na API (Meta)                                    | Origem             | Descrição                                             |
| :---------------------- | :-------------------------------------------------------------- | :----------------- | :---------------------------------------------------- |
| **`lead_formulario`**   | `lead`<br>`onsite_conversion.lead_grouped`<br>`onsite_web_lead` | Formulário Nativo  | Leads gerados nos formulários do Facebook/Instagram.  |
| **`lead_site`**         | `offsite_conversion.fb_pixel_lead`                              | Pixel no Site      | Conversões de Lead rastreadas pelo Pixel no website.  |
| **`lead_mensagem`**     | `onsite_conversion.messaging_first_reply`                       | Início de Conversa | Inícios de conversa por mensagem (WhatsApp/Insta DM). |
| **`seguidores_ganhos`** | `onsite_conversion.instagram_profile_followers`                 | Instagram          | Novos seguidores atribuídos a anúncios.               |
| **`videoview_3s`**      | `video_view`                                                    | Vídeo              | Visualizações de pelo menos 3 segundos de vídeo.      |

> **Nota:** O `hash_id` é composto pela combinação de: `ad_id` + `date_start` + `publisher_platform` + `platform_position`.
