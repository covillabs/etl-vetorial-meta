# Documentação Técnica - Vetorial ETL (Refatoração v2.0)

Este documento descreve em detalhes a arquitetura, o fluxo de dados e as regras de negócio implementadas no Pipeline de ETL da Vetorial para Meta Ads.

---

## 🏗️ 1. Arquitetura Modular

O projeto segue o padrão ETL clássico (Extract, Transform, Load), onde cada responsabilidade é isolada em um módulo independente dentro da pasta `src/`.

### Fluxo de Dados

1.  **Scheduled Trigger:** O `main.py` roda a cada 4 horas.
2.  **Ingestion (`src/ingestion`):** Conecta na API da Meta e baixa JSON bruto.
3.  **Transformation (`src/transformation`):** Limpa, tipa e normaliza métricas.
4.  **Load (`src/load`):** Envia para o Postgres com lógica de UPSERT.
5.  **Notification (`src/notification`):** Avisa no Discord em caso de falha.

---

## 🧩 2. Detalhes dos Módulos

### 2.1. Ingestion: `MetaExtractor`

Responsável exclusivamente por falar com a API da Meta.

**Decisão de Design Crítica:**
Para obter detalhes sobre _qual botão o usuário clicou_ (ex: "Enviar Mensagem" vs "Cadastro no Site"), é obrigatório adicionar o parâmetro `action_breakdowns=['action_type']`. Sem isso, a API retorna apenas totais genéricos.

```python
# src/ingestion/extractor.py
params = {
    "level": "ad",
    "date_preset": "last_30d",     # Janela deslizante de 30 dias
    "time_increment": 1,           # Dados dia a dia
    "limit": 500,
    "breakdowns": ["publisher_platform", "platform_position"], # Separa FB/IG e Feed/Stories
    "action_breakdowns": ["action_type"], # O SEGREDO para ver tipos de conversão
}
```

### 2.2. Transformation: `DataCleaner`

Aqui residem as Regras de Negócio da Vetorial. O objetivo é traduzir o "dialeto técnico" da Meta para métricas de negócio.

**Lógica de Unificação de Cliques:**
A API retorna `inline_link_clicks` (cliques no botão/link) e `link_click` (cliques gerais em links dentro do anúncio).

- **Decisão:** Somamos os dois, pois frequentemente um deles vem zerado dependendo do objetivo da campanha.
- **Correção v2.0:** Removemos `post_engagement` dessa soma porque ele incluía likes e comentários, inflando artificialmente o CTR.

**Lógica de Leads (Funil Completo):**
Um Lead na Vetorial pode vir de 3 origens. O ETL captura todas:

1.  **Formulário Nativo:** `lead` + `onsite_conversion.lead_grouped`
2.  **Site (Pixel):** `offsite_conversion.fb_pixel_lead`
3.  **WhatsApp/Direct:** `messaging_first_reply`

**Lógica de Seguidores (IG vs FB):**
A API mistura seguidores do Instagram com curtidas na página do Facebook.

- **Regra:** Priorizamos `instagram_follower_count_total` e `onsite_conversion.post_save_follow`.
- **Coluna no Banco:** `seguidores_instagram`.

### 2.4. Ingestion: `InstagramProfileExtractor`

Módulo dedicado a métricas de crescimento do perfil (não de anúncios).

**Atualização Multi-conta (v2.1):**
Agora o script itera sobre uma lista de IDs configurados (`META_IG_ACCOUNT_IDS`).

- O UPSERT utiliza uma **Chave Primária Composta**: `ig_account_id` + `data_registro`.
- Isso permite monitorar múltiplas contas de Instagram na mesma tabela sem conflito.

**Métrica Monitorada:** `follows_and_unfollows`.

- A Graph API não entrega "novos seguidores" diretamente. Ela entrega o saldo líquido.
- **Estratégia:** Buscamos sempre o dia anterior (`D-1`) completo (00:00 - 23:59).
- **Endpoint:** `/{ig_account_id}/insights`

```python
# src/ingestion/ig_profile_extractor.py
params = {
    "metric": "follows_and_unfollows",
    "period": "day",
    "since": timestamp_ontem_inicio,
    "until": timestamp_ontem_fim,
}
```

### 2.5. Load: `PostgresLoader`

Gerencia a persistência segura dos dados.

**Trava de Segurança (`REQUIRED_COLUMNS`):**
Para evitar que uma mudança no Cleaner quebre o Loader (ex: adicionar uma coluna que não existe no banco), o Loader possui uma lista estática de colunas permitidas.

- Se o Cleaner enviar colunas extras (ex: `ctr`, `cpc`), o Loader **ignora silenciosamente**.
- Se faltarem colunas essenciais, o Loader emite um **WARNING**, mas tenta continuar.

**Estratégia de UPSERT (Idempotência):**
Permite rodar o ETL múltiplas vezes no mesmo dia sem duplicar dados.

- **Chave Única (`hash_id`):** `md5(ad_id + data + plataforma + posicionamento)`
- **Comportamento:** Se o `hash_id` já existe, **ATUALIZA** os valores (ex: gasto aumentou ao longo do dia). Se não existe, **INSERE**.

```sql
INSERT INTO insights_meta_ads (...)
VALUES (...)
ON CONFLICT (hash_id) DO UPDATE SET
    valor_gasto = EXCLUDED.valor_gasto,
    impressoes = EXCLUDED.impressoes,
    ...
```

---

## 🕵️ 3. Ferramentas de Diagnóstico

Localizadas em `scripts/diagnostics/`, estes scripts salvam a vida quando a API muda ou dados parecem estranhos.

1.  **`audit_api_payload.py`:** Faz uma chamada crua para a API e imprime o JSON. Útil para ver se um campo novo apareceu ou mudou de nome.
2.  **`audit_metadata.py`:** Verifica configurações da conta, como Janela de Atribuição e Moeda.
3.  **`test_pipeline.py`:** Um teste unitário offline. Cria um JSON fake e passa pelo `DataCleaner` para ver se a transformação está correta, sem precisar conectar na API.

---

## 🔐 4. Segurança e Infraestrutura

- **Credenciais:** Nunca hardcoded. Sempre via variáveis de ambiente (`.env` local, `stack.env` no Portainer).
- **Logs:** O Container roda com `PYTHONUNBUFFERED=1` para garantir que logs de erro apareçam instantaneamente no Portainer.
- **Crash Loop:** O Docker tem `restart_policy: on-failure`. Se o script cair (erro de rede, API fora), ele tenta voltar sozinho.

---

## 📅 5. Agendamento (Scheduler)

Ao invés de usar CRON do sistema (difícil de monitorar em Docker), o agendamento é **interno**.

- Lib usada: `schedule`
- Intervalo: `every(4).hours`
- Comportamento: O script fica dormindo (`time.sleep(60)`) e acorda pra verificar se deu a hora.
- **Vantagem:** O container fica sempre "Running", facilitando monitoramento de uptime.

---

## 📢 6. Notificações

Se o ETL falhar, ninguém quer ter que abrir o terminal pra saber.

- **Canal:** Discord Webhook.
- **Trigger:** Qualquer Exception não tratada dentro do loop de processamento de contas.
- **Payload:** Mensagem formatada com Embed (Vermelho para erro, Verde para sucesso - opcional).

---

_Gerado automaticamente pela Equipe de Engenharia de Dados - Vetorial_
