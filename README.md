# Vetorial ETL - Facebook Ads Integration

Este projeto é um pipeline ETL (Extract, Transform, Load) de alta performance, projetado para extrair, processar e consolidar dados da **Meta Marketing API** (Facebook/Instagram Ads). Desenvolvido com foco em escalabilidade e auditabilidade, o sistema está pronto para produção em ambientes containerizados (Docker/Portainer).

---

## 🚀 Status do Projeto: OPERACIONAL

O ciclo completo de dados está implementado e validado:

- **[E] Extraction:** Captura de insights granulares (ad-level) com segmentação por plataforma e posicionamento.
- **[T] Transformation:** Motor de limpeza, normalização de métricas e deduplicação inteligente.
- **[L] Load:** Persistência em PostgreSQL com suporte a operações de `UPSERT` e histórico bruto.

---

## 📂 Visão Geral da Arquitetura

```plaintext
vetorial-etl/
├── main.py             # Orquestrador (Itera contas e gerencia janelas de tempo)
├── Dockerfile          # Receita da Imagem Docker (Python 3.10-slim)
├── requirements.txt    # Dependências (pandas, facebook_business, psycopg2)
├── .env                # Variáveis de ambiente (Segredos não versionados)
├── src/
│   ├── ingestion/      # Scripts de extração
│   │   └── extractor.py # Cliente da API (Lida com Breakdowns e Paginação)
│   ├── transformation/ # Scripts de transformação
│   │   └── cleaner.py  # Regras de limpeza, soma de leads e tratamento de nulos
│   ├── load/           # Scripts de carga
│   │   └── postgres_loader.py # Gerencia conexão e UPSERT no Banco
│   └── utils/          # Ferramentas auxiliares de debug
└── note.txt            # Logs e anotações
```

## 🛠️ Instalação e Configuração

1.  **Requisitos:**
    - Python 3.10+
    - Docker
    - Acesso ao PostgreSQL (Local ou Hetzner)
    - `.env` configurado com Token e IDs das Contas.

2.  **Instalação Local:**

    ```bash
    pip install -r requirements.txt
    ```

3.  **Variáveis de Ambiente (.env):**

    ```env
    # Credenciais Meta
    META_ACCESS_TOKEN=seu_token_aqui
    META_AD_ACCOUNT_IDS=act_12345,act_67890

    # Credenciais Banco
    DB_HOST=seu_ip_ou_localhost
    DB_NAME=postgres
    DB_USER=seu_usuario
    DB_PASS=sua_senha
    ```

## ⚡ Como Executar

**Via Docker (Produção):**

```bash
# 1. Construir a imagem!
docker build -t "nome-imagem" .

# 2. Rodar o container
docker run --env-file .env "nome-imagem"
```

**Via Terminal (Desenvolvimento):**

```bash
python main.py
```

## 📏 Regras de Negócio (Business Rules)

Esta seção documenta a lógica aplicada aos dados durante o processamento.

### 1. Estratégia de Extração (Janela de Tempo)

O pipeline utiliza o parâmetro `date_preset='last_90d'` por padrão.

- **Motivo:** A Meta pode atribuir conversões (leads/vendas) dias após o clique.
- **Comportamento:** A cada execução, o script reprocessa os últimos 3 meses. Dados antigos são atualizados no banco (Update), e novos são inseridos (Insert). Campanhas pausadas há mais de 90 dias sem atividade são ignoradas automaticamente pela API.

### 2. Granularidade e Chave Única (hash_id)

Os dados não são salvos apenas por ID do anúncio. Eles são quebrados por onde o anúncio apareceu.
A chave única (Primary Key) é um hash gerado a partir de:
`ad_id + date_start + publisher_platform (IG/FB) + platform_position (Feed/Stories/Reels)`
Isso permite saber exatamente quanto se gastou no "Instagram Stories" vs "Facebook Feed" para o mesmo anúncio.

### 3. Tratamento de Dados Nulos

A API da Meta omite colunas se a métrica for zero no dia (ex: se ninguém clicou, a chave `clicks` não vem).

- **Regra:** O ETL verifica a existência da coluna; se não existir, força o valor 0 (inteiro) ou 0.0 (float) para evitar erros de cálculo.

### 4. Mapeamento e Cálculos de Métricas

O sistema normaliza nomes técnicos da API para nomes de negócio no Banco de Dados:

| Métrica no Banco (Destino) | Origem (Meta API / Breakdown)      | Lógica / Fórmulas                          |
| :------------------------- | :--------------------------------- | :----------------------------------------- |
| **valor_gasto**            | `spend`                            | Arredondado para 2 casas decimais.         |
| **impressoes**             | `impressions`                      | Inteiro. Se nulo, 0.                       |
| **lead_formulario**        | `lead`, `onsite_web_lead`...       | Conversões via Formulário Nativo.          |
| **lead_site**              | `offsite_conversion.fb_pixel_lead` | Conversões via Pixel (Website).            |
| **lead_mensagem**          | `onsite_conversion.messaging...`   | Conversões iniciadas no WhatsApp/Direct.   |
| **seguidores_ganhos**      | `instagram_profile_followers`      | Novos seguidores atribuídos ao anúncio.    |
| **videoview_3s**           | `video_view`                       | Visualizações > 3 segundos.                |
| **videoview_50**           | `video_p50_watched_actions`        | Retenção: Usuários que viram 50% do vídeo. |
| **videoview_75**           | `video_p75_watched_actions`        | Retenção: Usuários que viram 75% do vídeo. |

### 5. Campos Calculados (Totais)

Além dos dados brutos, o ETL gera colunas consolidadas para facilitar dashboards:

- **lead (Total):** Soma de `lead_formulario` + `lead_site` + `lead_mensagem`.
- **Nota:** O `hash_id` é composto pela combinação de: `ad_id` + `date_start` + `publisher_platform` + `platform_position`.
