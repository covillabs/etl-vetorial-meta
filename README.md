# Vetorial ETL - Facebook Ads Integration

Este projeto é um pipeline ETL (Extract, Transform, Load) de alta performance, projetado para extrair, processar e consolidar dados da **Meta Marketing API** (Facebook/Instagram Ads). Desenvolvido com foco em escalabilidade e auditabilidade, o sistema está pronto para produção em ambientes containerizados (Docker/Portainer).

---

## 🚀 Status do Projeto: OPERACIONAL

O ciclo completo de dados está implementado e validado:

- **[E] Extraction:** Captura de insights granulares (ad-level) com segmentação por plataforma e posicionamento.
- **[T] Transformation:** Motor de limpeza, normalização de métricas e deduplicação inteligente.
- **[L] Load:** Persistência em PostgreSQL com suporte a operações de `UPSERT` e histórico bruto.
- **[S] Scheduler:** Execução automática a cada 4 horas (built-in).
- **[N] Notification:** Alertas de Erro/Status via Discord Webhook.

---

## 📂 Visão Geral da Arquitetura

```plaintext
vetorial-etl/
├── main.py                 # Orquestrador + Scheduler (4h loop)
├── Dockerfile              # Receita da Imagem Docker (Python 3.10-slim)
├── docker-compose.yml      # Deploy (Portainer/Swarm)
├── requirements.txt        # Dependências
├── .env                    # Variáveis de ambiente (não versionado)
├── src/
│   ├── ingestion/
│   │   └── extractor.py    # Cliente da API (Breakdowns + action_breakdowns)
│   ├── transformation/
│   │   └── cleaner.py      # Normalização, leads, seguidores, hash_id
│   ├── load/
│   │   └── postgres_loader.py  # UPSERT + Filtro de segurança (REQUIRED_COLUMNS)
│   ├── notification/
│   │   └── discord_alert.py    # Alertas via Discord Webhook
│   └── utils/              # (vazio — scripts movidos para scripts/)
└── scripts/
    └── diagnostics/        # Ferramentas de diagnóstico e debug
        ├── audit_api_payload.py    # Varredura de campos da API
        ├── audit_metadata.py       # Checagem de atribuição e UTMs
        ├── deep_scan_followers.py  # Scan profundo de seguidores
        ├── inspect_api.py          # Mapeamento de actions por conta
        ├── test_db.py              # Teste de conexão com PostgreSQL
        └── test_pipeline.py        # Teste offline do cleaner (mock data)
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

    # Notificações
    DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
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

**Rodar Testes Offline:**

```bash
python scripts/diagnostics/test_pipeline.py
```

## 📏 Regras de Negócio (Business Rules)

Esta seção documenta a lógica aplicada aos dados durante o processamento.

### 1. Estratégia de Extração (Janela de Tempo)

O pipeline utiliza o parâmetro `date_preset='last_30d'` por padrão.

- **Motivo:** A Meta pode atribuir conversões (leads/vendas) dias após o clique.
- **Comportamento:** A cada execução, o script reprocessa os últimos 30 dias. Dados antigos são atualizados no banco (Update), e novos são inseridos (Insert). Campanhas pausadas há mais de 30 dias sem atividade são ignoradas automaticamente pela API.

### 2. Granularidade e Chave Única (hash_id)

Os dados não são salvos apenas por ID do anúncio. Eles são quebrados por onde o anúncio apareceu.
A chave única (Primary Key) é um hash MD5 gerado a partir de:
`ad_id + date_start + publisher_platform (IG/FB) + platform_position (Feed/Stories/Reels)`
Isso permite saber exatamente quanto se gastou no "Instagram Stories" vs "Facebook Feed" para o mesmo anúncio.

### 3. Tratamento de Dados Nulos

A API da Meta omite colunas se a métrica for zero no dia (ex: se ninguém clicou, a chave `clicks` não vem).

- **Regra:** O ETL verifica a existência da coluna; se não existir, força o valor 0 (inteiro) ou 0.0 (float) para evitar erros de cálculo.

### 4. Mapeamento e Cálculos de Métricas

O sistema normaliza nomes técnicos da API para nomes de negócio no Banco de Dados:

| Métrica no Banco (Destino)  | Origem (Meta API / Breakdown)                                                       | Lógica / Fórmulas                                 |
| :-------------------------- | :---------------------------------------------------------------------------------- | :------------------------------------------------ |
| **valor_gasto**             | `spend`                                                                             | Arredondado para 2 casas decimais.                |
| **impressoes**              | `impressions`                                                                       | Inteiro. Se nulo, 0.                              |
| **clique_link**             | `inline_link_clicks` + `link_click` (actions)                                       | Soma dos dois campos (inline costuma vir zerado). |
| **lead_formulario**         | `lead`, `onsite_conversion.lead_grouped`, `onsite_conversion.lead`                  | Conversões via Formulário Nativo.                 |
| **lead_site**               | `onsite_web_lead`, `offsite_conversion.fb_pixel_lead`                               | Conversões via Pixel (Website).                   |
| **lead_mensagem**           | `onsite_conversion.messaging_first_reply`, `total_messaging_connection`             | WhatsApp/Direct.                                  |
| **seguidores_instagram**    | `onsite_conversion.post_save_follow`, `instagram_follower_count_total`, `page_like` | Novos seguidores.                                 |
| **videoview_3s**            | `video_view` (de actions)                                                           | Visualizações > 3 segundos.                       |
| **videoview_50**            | `video_p50_watched_actions`                                                         | Retenção: Usuários que viram 50% do vídeo.        |
| **videoview_75**            | `video_p75_watched_actions`                                                         | Retenção: Usuários que viram 75% do vídeo.        |
| **(instagram_crescimento)** | `follows_and_unfollows` (Graph API)                                                 | Saldo líquido de seguidores no dia anterior.      |

### 5. Extração de Crescimento do Perfil (Instagram)

Além dos anúncios, o pipeline extrai métricas orgânicas/perfil do Instagram:

- **Fonte:** Instagram Graph API (`/insights`).
- **Métrica:** `follows_and_unfollows` (Total de seguidores novos - Unfollows).
- **Frequência:** Diária (busca sempre o dia anterior fechado `D-1`).
- **Tabela:** `instagram_crescimento` (Upsert por `data_registro`).
- **Requisito:** Variável `META_IG_ACCOUNT_ID` configurada.

### 5. Campos Calculados (Totais)

Além dos dados brutos, o ETL gera colunas consolidadas para facilitar dashboards:

- **lead (Total):** Soma de `lead_formulario` + `lead_site` + `lead_mensagem`.
- **Nota:** O `hash_id` é composto pela combinação de: `ad_id` + `date_start` + `publisher_platform` + `platform_position`.

### 6. Filtro de Segurança (REQUIRED_COLUMNS)

O `postgres_loader.py` contém uma lista `REQUIRED_COLUMNS` que atua como trava de segurança:

- Apenas colunas dessa lista são enviadas ao banco
- Se o cleaner gerar colunas extras (ex: `reach`, `ctr`), elas são **ignoradas** silenciosamente
- Se alguma coluna esperada estiver faltando, um **WARNING** é logado (mas o pipeline não trava)
