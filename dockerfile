# 1. Imagem Base: Usamos um Python leve e moderno
FROM python:3.10-slim

# 2. Define o fuso horário para o Brasil (Importante para os logs e agendamentos)
ENV TZ=America/Sao_Paulo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 3. Cria a pasta de trabalho dentro do container
WORKDIR /app

# 4. Copia a lista de dependências e instala
# (Fazemos isso antes de copiar o código para aproveitar o cache do Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copia todo o resto do seu código para dentro do container
COPY . .

# 6. Comando padrão ao iniciar: Rodar o script principal
CMD ["python", "main.py"]