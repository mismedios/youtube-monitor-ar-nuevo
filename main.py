import os
import sqlite3
import pandas as pd
from datetime import datetime, timezone, timedelta
from googleapiclient.discovery import build
import gspread
from google.oauth2.service_account import Credentials
import json
import requests

# --- CONFIGURACIÓN ---
API_KEY = str(os.getenv('YT_API_KEY', '')).strip()
TELEGRAM_TOKEN = str(os.getenv('TELEGRAM_TOKEN', '')).strip()
TELEGRAM_CHAT_ID = str(os.getenv('TELEGRAM_CHAT_ID', '')).strip()
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS', '').strip()
SPREADSHEET_ID = str(os.getenv('SPREADSHEET_ID', '')).strip()

CANALES = {
    "@TNTSportsAR": "UCI5RY8G0ar-hLIaUJvx58Lw",
    "@ESPNFans": "UCFmMw7yTuLTCuMhpZD5dVsg",
    "@LigaProfesional": "UCJmCVoUfCBQb9lcfXIS8nXQ",
}

DB_NAME = "youtube_unificada.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    # Tabla de datos de videos
    conn.execute('''CREATE TABLE IF NOT EXISTS videos (
        id_video TEXT PRIMARY KEY, titulo TEXT, descripcion TEXT, fecha_publicacion TEXT, 
        duracion TEXT, vistas INTEGER, me_gusta INTEGER, no_me_gusta TEXT, comentarios INTEGER, 
        shares TEXT, url TEXT, suscriptores INTEGER, fecha_scrapeo TEXT, hora_scrapeo TEXT, canal TEXT)''')
    # Tabla de logs para el reporte diario
    conn.execute('''CREATE TABLE IF NOT EXISTS logs (
        fecha TEXT, hora TEXT, estado TEXT, mensaje TEXT)''')
    # Tabla de control de reportes
    conn.execute('''CREATE TABLE IF NOT EXISTS control_reportes (fecha TEXT PRIMARY KEY)''')
    conn.commit()
    return conn

def log_ejecucion(conn, estado, mensaje):
    ahora_arg = datetime.now(timezone.utc) - timedelta(hours=3)
    conn.execute("INSERT INTO logs VALUES (?, ?, ?, ?)", 
                 (ahora_arg.strftime('%Y-%m-%d'), ahora_arg.strftime('%H:%M:%S'), estado, mensaje))
    conn.commit()

def obtener_datos_youtube(channel_id, nombre_canal):
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    
    # 1. Obtener Suscriptores del canal
    ch_res = youtube.channels().list(id=channel_id, part='statistics,contentDetails').execute()
    suscriptores = int(ch_res['items'][0]['statistics'].get('subscriberCount', 0))
    uploads_id = ch_res['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    # 2. Obtener IDs de videos (últimos 50 para hacerlo rápido cada 5 min)
    res = youtube.playlistItems().list(playlistId=uploads_id, part='contentDetails', maxResults=50).execute()
    video_ids = [item['contentDetails']['videoId'] for item in res['items']]

    # 3. Obtener métricas de videos
    datos_videos = []
    ahora_arg = datetime.now(timezone.utc) - timedelta(hours=3)
    fecha_scrapeo = ahora_arg.strftime('%Y-%m-%d')
    hora_scrapeo = ahora_arg.strftime('%H:%M:%S')

    if video_ids:
        v_res = youtube.videos().list(id=','.join(video_ids), part='statistics,snippet,contentDetails').execute()
        for item in v_res['items']:
            datos_videos.append({
                'ID del video': item['id'],
                'Título del video': item['snippet']['title'],
                'Descripcion del video': item['snippet']['description'][:500], # Limitamos a 500 chars para no saturar
                'Fecha Publicación el video': item['snippet'].get('publishedAt', '')[:10],
                'Duración del video': item['contentDetails'].get('duration', ''),
                'Vistas del video': int(item['statistics'].get('viewCount', 0)),
                'Me Gusta del video': int(item['statistics'].get('likeCount', 0)),
                'No Me Gusta del video': 'N/A', # API no lo provee públicamente
                'Comentarios del video': int(item['statistics'].get('commentCount', 0)),
                'Shares del video': 'N/A', # API no lo provee
                'URL del video': f"https://youtu.be/{item['id']}",
                'Suscriptores del canal': suscriptores,
                'Fecha de scrapeo de datos': fecha_scrapeo,
                'Hora de scrapeo de datos': hora_scrapeo,
                'Canal': nombre_canal
            })
    return datos_videos

def subir_a_google_sheets(datos):
    if not GOOGLE_CREDENTIALS_JSON or not SPREADSHEET_ID:
        raise Exception("Faltan credenciales o ID de Google Sheets.")
    
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    
    sheet = gc.open_by_key(SPREADSHEET_ID).sheet1
    
    # Preparamos las filas. Aseguramos el orden pedido.
    filas = []
    for d in datos:
        filas.append([
            d['ID del video'], d['Título del video'], d['Descripcion del video'],
            d['Fecha Publicación el video'], d['Duración del video'], d['Vistas del video'],
            d['Me Gusta del video'], d['No Me Gusta del video'], d['Comentarios del video'],
            d['Shares del video'], d['URL del video'], d['Suscriptores del canal'],
            d['Fecha de scrapeo de datos'], d['Hora de scrapeo de datos']
        ])
    
    if filas:
        sheet.append_rows(filas, value_input_option='USER_ENTERED')

def enviar_telegram(mensaje):
    base_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(base_url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': mensaje, 'parse_mode': 'Markdown'})

def evaluar_reporte_diario(conn):
    ahora_arg = datetime.now(timezone.utc) - timedelta(hours=3)
    fecha_hoy = ahora_arg.strftime('%Y-%m-%d')
    hora_actual = ahora_arg.hour

    # Verificar si son pasadas las 10 AM y no se envió hoy
    if hora_actual >= 10:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM control_reportes WHERE fecha=?", (fecha_hoy,))
        if not cursor.fetchone():
            # Obtener métricas del día
            cursor.execute("SELECT COUNT(*) FROM logs WHERE fecha=? AND estado='EXITO'", (fecha_hoy,))
            ejecuciones_ok = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM logs WHERE fecha=? AND estado='ERROR'", (fecha_hoy,))
            ejecuciones_error = cursor.fetchone()[0]
            
            msg = (
                f"📊 *REPORTE DIARIO DE SCRAPING - 10:00 HS*\n\n"
                f"✅ Ejecuciones exitosas hoy: {ejecuciones_ok}\n"
                f"❌ Ejecuciones con errores: {ejecuciones_error}\n\n"
                f"⚙️ *Estado:* {'🟢 Todo operando normal' if ejecuciones_error == 0 else '🔴 Hubo problemas, revisar logs.'}"
            )
            enviar_telegram(msg)
            
            # Registrar que ya se envió hoy
            conn.execute("INSERT INTO control_reportes VALUES (?)", (fecha_hoy,))
            conn.commit()

def main():
    conn = init_db()
    todos_los_datos = []
    errores = []

    try:
        for nombre_canal, channel_id in CANALES.items():
            datos_canal = obtener_datos_youtube(channel_id, nombre_canal)
            todos_los_datos.extend(datos_canal)
        
        if todos_los_datos:
            subir_a_google_sheets(todos_los_datos)
            log_ejecucion(conn, 'EXITO', f'Se scrapearon y enviaron {len(todos_los_datos)} videos a GSheets.')
    except Exception as e:
        errores.append(str(e))
        log_ejecucion(conn, 'ERROR', str(e))

    evaluar_reporte_diario(conn)
    conn.close()

if __name__ == "__main__":
    main()
