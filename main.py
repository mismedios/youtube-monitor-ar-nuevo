name: Monitor YouTube Auto-Scrape (30 Minutos)
on:
  schedule:
    - cron: '*/30 * * * *' # Ejecuta cada 30 minutos
  workflow_dispatch:

jobs:
  run-monitor:
    runs-on: ubuntu-latest
    
    steps:
      - name: Descargar código
        uses: actions/checkout@v4

      - name: Configurar Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'

      - name: Instalar dependencias
        run: pip install google-api-python-client requests gspread google-auth

      - name: Ejecutar Script de Extracción
        env:
          YT_API_KEY: ${{ secrets.YT_API_KEY }}
          TELEGRAM_TOKEN: ${{ secrets.TELEGRAM_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
          GOOGLE_CREDENTIALS: ${{ secrets.GOOGLE_CREDENTIALS }}
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
        run: python main.py
