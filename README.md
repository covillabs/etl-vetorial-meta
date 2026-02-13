# Vetorial ETL - Facebook Ads Integration

Este projeto é um pipeline ETL (Extract, Transform, Load) focado na extração e processamento de dados da API do Facebook Ads (Meta Marketing API).

## 🚀 Status do Projeto

Atualmente, o projeto avançou para a fase de **Transformação e Padronização**.
Já implementamos scripts de inspeção e o módulo de limpeza (`cleaner.py`) que normaliza os dados brutos da API para o formato do DW.

## 📂 Estrutura do Projeto

```
vetorial-etl/
├── .env                # Variáveis de ambiente (Tokens, IDs)
├── requirements.txt    # Dependências do projeto
├── src/
│   ├── ingestion/      # Scripts de extração (Em breve)
│   ├── transformation/ # Scripts de transformação de dados
│   │   └── cleaner.py  # Padronização e limpeza de dados
│   ├── load/           # Scripts de carga (Em breve)
│   └── utils/
│       └── inspect_api.py  # Script de diagnóstico e inspeção da API
├── data/               # Diretório para dados temporários ou locais
└── note.txt            # Logs de inspeção e exemplos de retorno da API
```

## 🛠️ Instalação e Configuração

1.  **Requisitos:**
    *   Python 3.8+
    *   Conta de Desenvolvedor Meta com App criado e Token de Acesso.

2.  **Instalação das dependências:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configuração:**
    Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:
    ```env
    META_ACCESS_TOKEN=seu_token_v4
    META_AD_ACCOUNT_ID=
    ```

## 🔍 Scripts Disponíveis

### `src/utils/inspect_api.py`
Este script realiza um diagnóstico inicial nas contas de anúncio configuradas.
*   **Função:** Verifica o acesso à conta, lista seguidores do Instagram associado e faz uma amostragem dos anúncios (últimos 30 dias) para listar todas as `actions` (eventos) disponíveis.
*   **Execução:**
    ```bash
    python src/utils/inspect_api.py
    ```

### `src/transformation/cleaner.py`
Este módulo contém a classe `DataCleaner`, responsável por receber os dados brutos (JSON) da API e convertê-los em um DataFrame pandas estruturado.
*   **Funcionalidades:**
    *   Extração de métricas específicas de `actions` (leads, mensagens, etc).
    *   Cálculo de métricas de vídeo (3s, 50%, 75%).
    *   Criação de chaves únicas (`hash_id`) para deduplicação.
    *   Padronização de tipos de dados (float, int).
*   **Teste Isolado:**
    O arquivo possui um bloco `main` para teste rápido.
    ```bash
    python src/transformation/cleaner.py
    ```

---

## 📏 Regras de Negócio (Business Rules)

Esta seção serve como guia oficial para a transformação de dados e manutenção futura do ETL. O objetivo é padronizar as métricas vindas de diferentes origens (Pixel, API de Conversões, Formulários) em nomes únicos no banco de dados.

### Mapeamento de Métricas

A tabela abaixo define como os eventos técnicos da API da Meta devem ser processados e renomeados para o banco de dados analítico.

| Métrica no Banco | Nomes Técnicos na API (Meta) | Origem | Descrição |
| :--- | :--- | :--- | :--- |
| **`lead_formulario`** | `lead`<br>`onsite_conversion.lead_grouped`<br>`onsite_web_lead` | Formulário Nativo | Leads gerados diretamente nos formulários do Facebook/Instagram (Instant Forms). |
| **`lead_site`** | `offsite_conversion.fb_pixel_lead` | Pixel no Site | Conversões de Lead rastreadas pelo Pixel no website externo. |
| **`lead_mensagem`** | `onsite_conversion.messaging_first_reply` | Início de Conversa | Usuários que iniciaram uma conversa por mensagem (WhatsApp, Direct, Messenger) após clique no anúncio. |
| **`lp_view`** | `landing_page_view`<br>`omni_landing_page_view` | Visualização de Página | Visualizações da página de destino (Landig Page) após o clique. |
| **`compras`** | `purchase`<br>`onsite_web_purchase`<br>`offsite_conversion.fb_pixel_purchase` | Vendas Diretas | Eventos de compra confirmada, seja via Pixel ou API de Conversões. |

> **Nota para Desenvolvedores:** Ao criar a lógica de transformação (`src/transformation`), utilize um dicionário de mapeamento ou estrutura `CASE WHEN` baseada nesta tabela para agregar os valores corretamente. Eventos não listados aqui devem ser ignorados ou categorizados como `outros` dependo da necessidade.
