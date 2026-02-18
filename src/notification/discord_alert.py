import os
import requests
import json
from datetime import datetime


class DiscordAlert:
    def __init__(self):
        # Carrega a URL do ambiente. Se não existir, avisa no log.
        self.webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        if not self.webhook_url:
            print("⚠️ [Discord] Webhook não configurado. Alertas serão ignorados.")

    def send(self, message, level="info"):
        """
        Envia mensagem para o Discord.
        Args:
            message (str): O texto do erro ou sucesso.
            level (str): 'info', 'warning' ou 'error'. Muda a cor/emoji.
        """
        if not self.webhook_url:
            return

        # Configurações visuais (Emojis e Títulos)
        config = {
            "info": {"emoji": "✅", "color": 3066993},  # Verde
            "warning": {"emoji": "⚠️", "color": 16776960},  # Amarelo
            "error": {"emoji": "🚨", "color": 15158332},  # Vermelho
        }

        cfg = config.get(level, config["info"])
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Payload formatado (Embed fica mais bonito no Discord)
        payload = {
            "username": "ETL Bot - Vetorial",
            "embeds": [
                {
                    "title": f"{cfg['emoji']} ETL Notification - {level.upper()}",
                    "description": message,
                    "color": cfg["color"],
                    "footer": {"text": f"Horário: {timestamp}"},
                }
            ],
        }

        try:
            response = requests.post(self.webhook_url, json=payload)
            response.raise_for_status()
        except Exception as e:
            print(f"❌ [Discord] Falha ao enviar alerta: {e}")
