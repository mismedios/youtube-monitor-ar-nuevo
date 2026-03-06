import os
import sqlite3
import subprocess
import pandas as pd
import requests
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from googleapiclient.discovery import build

# --- CONFIGURACIÓN ---
# Usamos .strip() para limpiar espacios invisibles de los Secrets de GitHub
API_KEY = os.getenv('YT_API_KEY', '').strip()
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '').strip()
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '').strip()

# ID de tu canal
CHANNEL_ID = 'UC1Tw7oE-tWiA3qXEPpq9SWQ' 

DB_NAME = "stats_history.db"
UMBRAL_VIRAL = 10000 

def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute('''CREATE TABLE IF NOT EXISTS canal_history 
                    (fecha TEXT, vistas_totales INTEGER)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS video_history 
                    (video_id TEXT, titulo TEXT, vistas INTEGER, fecha TEXT)''')
    conn.commit()
    conn.close()

def enviar_telegram(mensaje, foto_path=None, archivo_path=None):
    # URL corregida con /bot al principio
    base_url = f"https://api.telegram.org{TELEGRAM_TOKEN}"
    
    try:
        # 1. Enviar Foto con el resumen como epígrafe
        if foto_path:
            with open(foto_path, 'rb') as f:
                r = requests.post(f"{base_url}/sendPhoto", 
                              data={'chat_id': TELEGRAM_CHAT_ID, 'caption': mensaje, 'parse_mode': 'Markdown'}, 
                              files={'photo': f})
                print(f"Respuesta Telegram Foto: {r.status_code}")
        else:
            r = requests.post(f"{base_url}/sendMessage", 
                          data={'chat_id': TELEGRAM_CHAT_ID, 'text': mensaje, 'parse_mode': 'Markdown'})
            print(f"Respuesta Telegram Texto: {r.status_code}")
        
        # 2. Enviar Excel si se generó
        if archivo_path:
            with open(archivo_path, 'rb') as f:
                requests.post(f"{base_url}/sendDocument", data={'chat_id': TELEGRAM_CHAT_ID}, files={'document': f})
                
    except Exception as e:
        print(f"❌ Error al enviar a Telegram: {e}")

def git_push_db():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "YouTube Bot AR"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "bot@github.com"], check=True)
        subprocess.run(["git", "add", DB_NAME], check=True)
        subprocess.run(["git", "commit", "-m", f"🔄 Historial actualizado: {datetime.now().date()}"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("📦 Base de datos sincronizada con GitHub.")
    except Exception as e:
        print("ℹ️ No hay cambios en la base de datos para subir.")

def obtener_datos_youtube():
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    
    # Obtener Playlist de Uploads
    ch_res = youtube.channels().list(id=CHANNEL_ID, part='contentDetails').execute()
    uploads_id = ch_res['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    # Listar IDs de videos
    video_ids = []
    next_page = None
    for _ in range(4): # Últimos 200 videos
        res = youtube.playlistItems().list(playlistId=uploads_id, part='contentDetails', maxResults=50, pageToken=next_page).execute()
        video_ids.extend([item['contentDetails']['videoId'] for item in res['items']])
        next_page = res.get('nextPageToken')
        if not next_page: break

    # Obtener métricas en lotes
    datos_videos = []
    for i in range(0, len(video_ids), 50):
        v_res = youtube.videos().list(id=','.join(video_ids[i:i+50]), part='statistics,snippet').execute()
        for item in v_res['items']:
            datos_videos.append({
                'ID': item['id'],
                'Título': item['snippet']['title'],
                'Vistas': int(item['statistics'].get('viewCount', 0)),
                'Likes': int(item['statistics'].get('likeCount', 0)),
                'URL': f"https://youtu.be{item['id']}"
            })
    return pd.DataFrame(datos_videos)

def procesar_metricas(df_hoy):
    conn = sqlite3.connect(DB_NAME)
    fecha_hoy = datetime.now().date().isoformat()
    cursor = conn.cursor()
    
    # --- 1. Canal ---
    vistas_totales_hoy = df_hoy['Vistas'].sum()
    cursor.execute("SELECT vistas_totales FROM canal_history ORDER BY fecha DESC LIMIT 1")
    row = cursor.fetchone()
    vistas_ayer = row[0] if row else vistas_totales_hoy
    crecimiento_total = vistas_totales_hoy - vistas_ayer
    conn.execute("INSERT INTO canal_history VALUES (?, ?)", (fecha_hoy, vistas_totales_hoy))

    # --- 2. Videos ---
    df_hoy['Crecimiento'] = 0
    alertas_virales = []
    
    for idx, row in df_hoy.iterrows():
        cursor.execute("SELECT vistas FROM video_history WHERE video_id=? ORDER BY fecha DESC LIMIT 1", (row['ID'],))
        res_v = cursor.fetchone()
        
        diff = 0
        if res_v:
            diff = row['Vistas'] - res_v[0]
            df_hoy.at[idx, 'Crecimiento'] = diff
            if diff >= UMBRAL_VIRAL:
                alertas_virales.append(f"🚀 *VIRAL:* {row['Título']} (+{diff:,} vistas)")
        
        conn.execute("INSERT INTO video_history VALUES (?, ?, ?, ?)", 
                     (row['ID'], row['Título'], row['Vistas'], fecha_hoy))
    
    conn.commit()
    conn.close()
    return crecimiento_total, alertas_virales

def generar_grafico(df):
    top_5 = df[df['Crecimiento'] >= 0].sort_values(by='Crecimiento', ascending=False).head(5)
    
    # Si es la primera vez que corre, graficar por vistas totales
    if top_5['Crecimiento'].sum() == 0:
        top_5 = df.sort_values(by='Vistas', ascending=False).head(5)
        col_x = 'Vistas'
        tit = "Videos más vistos (Primer Reporte)"
    else:
        col_x = 'Crecimiento'
        tit = f"Crecimiento Diario - {datetime.now().strftime('%d/%m')}"

    plt.figure(figsize=(10, 6))
    sns.barplot(x=col_x, y=top_5['Título'].str[:35], data=top_5, palette='magma')
    plt.title(tit)
    path = "grafico_hoy.png"
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    return path

def main():
    try:
        init_db()
        df_hoy = obtener_datos_youtube()
        crecimiento_total, virales = procesar_metricas(df_hoy)
        
        # Generar archivos
        excel_path = f"reporte_yt_{datetime.now().strftime('%Y%m%d')}.xlsx"
        df_hoy.to_excel(excel_path, index=False)
        grafico_path = generar_grafico(df_hoy)

        # Mensaje de Telegram
        txt_virales = "\n".join(virales) if virales else "Sin picos de viralidad hoy."
        resumen = (f"🇦🇷 *REPORTE ARGENTINA*\n"
                   f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
                   f"📊 *Vistas Totales:* {df_hoy['Vistas'].sum():,}\n"
                   f"📈 *Crecimiento 24h:* +{crecimiento_total:,}\n\n"
                   f"*Alertas Virales:*\n{txt_virales}")
        
        enviar_telegram(resumen, foto_path=grafico_path, archivo_path=excel_path)
        git_push_db()
        
    except Exception as e:
        print(f"Ocurrió un error: {e}")

if __name__ == "__main__":
    main()
