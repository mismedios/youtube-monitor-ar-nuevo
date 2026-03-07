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
# Usamos strip() para limpiar cualquier espacio invisible de los Secrets
API_KEY = str(os.getenv('YT_API_KEY', '')).strip()
TELEGRAM_TOKEN = str(os.getenv('TELEGRAM_TOKEN', '')).strip()
TELEGRAM_CHAT_ID = str(os.getenv('TELEGRAM_CHAT_ID', '')).strip()

CHANNEL_ID = 'UC1Tw7oE-tWiA3qXEPpq9SWQ' 
DB_NAME = "stats_history.db"
UMBRAL_VIRAL = 10000 

def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute('''CREATE TABLE IF NOT EXISTS canal_history (fecha TEXT, vistas_totales INTEGER)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS video_history (video_id TEXT, titulo TEXT, vistas INTEGER, fecha TEXT)''')
    conn.commit()
    conn.close()

def enviar_telegram(mensaje, foto_path=None, archivo_path=None):
    # La palabra "bot" debe ir pegada a la URL y ANTES del token
    base_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
    
    try:
        # 1. Enviar Foto
        if foto_path and os.path.exists(foto_path):
            with open(foto_path, 'rb') as f:
                # Aquí la URL debe terminar en /sendPhoto
                r = requests.post(f"{base_url}/sendPhoto", 
                                  data={'chat_id': TELEGRAM_CHAT_ID, 'caption': mensaje, 'parse_mode': 'Markdown'}, 
                                  files={'photo': f})
        else:
            # Enviar solo texto si no hay foto
            r = requests.post(f"{base_url}/sendMessage", 
                              data={'chat_id': TELEGRAM_CHAT_ID, 'text': mensaje, 'parse_mode': 'Markdown'})
        
        # 2. Enviar archivo Excel
        if archivo_path and os.path.exists(archivo_path):
            with open(archivo_path, 'rb') as f:
                requests.post(f"{base_url}/sendDocument", data={'chat_id': TELEGRAM_CHAT_ID}, files={'document': f})
        
        print(f"Telegram status: {r.status_code}")
    except Exception as e:
        print(f"❌ Error al enviar a Telegram: {e}")

def git_push_db():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "YouTube Bot AR"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "bot@github.com"], check=True)
        subprocess.run(["git", "add", DB_NAME], check=True)
        subprocess.run(["git", "commit", "-m", f"🔄 Update DB: {datetime.now().date()}"], check=True)
        subprocess.run(["git", "push"], check=True)
    except:
        pass

def obtener_datos_youtube():
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    # CORRECCIÓN: Se agrega [0] para entrar al primer canal de la lista
    ch_res = youtube.channels().list(id=CHANNEL_ID, part='contentDetails').execute()
    uploads_id = ch_res['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    video_ids = []
    next_page = None
    for _ in range(4):
        res = youtube.playlistItems().list(playlistId=uploads_id, part='contentDetails', maxResults=50, pageToken=next_page).execute()
        video_ids.extend([item['contentDetails']['videoId'] for item in res['items']])
        next_page = res.get('nextPageToken')
        if not next_page: break

    datos_videos = []
    for i in range(0, len(video_ids), 50):
        v_res = youtube.videos().list(id=','.join(video_ids[i:i+50]), part='statistics,snippet').execute()
        for item in v_res['items']:
            datos_videos.append({
                'ID': item['id'],
                'Título': item['snippet']['title'],
                'Vistas': int(item['statistics'].get('viewCount', 0)),
                'URL': f"https://youtu.be{item['id']}"
            })
    return pd.DataFrame(datos_videos)


def procesar_metricas(df_hoy, reintentos=1):
    try:
        conn = sqlite3.connect(DB_NAME)
        fecha_hoy = datetime.now().date().isoformat()
        cursor = conn.cursor()
        
        vistas_totales_hoy = int(df_hoy['Vistas'].sum())
        
        # --- 1. Crecimiento del Canal ---
        cursor.execute("SELECT vistas_totales FROM canal_history ORDER BY fecha DESC LIMIT 1")
        row = cursor.fetchone()
        
        # Leemos el dato; si es basura binaria, aquí saltará el ValueError
        vistas_ayer = int(row[0]) if row else vistas_totales_hoy
        crecimiento_total = vistas_totales_hoy - vistas_ayer
        
        conn.execute("INSERT INTO canal_history VALUES (?, ?)", (fecha_hoy, vistas_totales_hoy))

        # --- 2. Crecimiento por Video ---
        df_hoy['Crecimiento'] = 0
        alertas = []
        
        for idx, row_v in df_hoy.iterrows():
            cursor.execute("SELECT vistas FROM video_history WHERE video_id=? ORDER BY fecha DESC LIMIT 1", (row_v['ID'],))
            res_v = cursor.fetchone()
            
            if res_v:
                # Si el registro del video está corrupto, también saltará aquí
                vistas_ayer_v = int(res_v[0])
                diff = int(row_v['Vistas']) - vistas_ayer_v
                df_hoy.at[idx, 'Crecimiento'] = diff
                
                if diff >= UMBRAL_VIRAL:
                    alertas.append(f"🚀 *VIRAL:* {row_v['Título']} (+{diff:,} vistas)")
            
            conn.execute("INSERT INTO video_history VALUES (?, ?, ?, ?)", 
                         (row_v['ID'], row_v['Título'], int(row_v['Vistas']), fecha_hoy))
        
        conn.commit()
        conn.close()
        return crecimiento_total, alertas

    except (ValueError, sqlite3.DatabaseError) as e:
        # Si ocurre un error por corrupción, cerramos la conexión para liberar el archivo
        conn.close()
        
        if reintentos > 0:
            print(f"⚠️ BD corrupta detectada ({e}). Eliminando y reseteando...")
            
            # Eliminamos el archivo corrupto
            if os.path.exists(DB_NAME):
                os.remove(DB_NAME)
            
            # Volvemos a crear las tablas desde cero
            init_db()
            
            # Reintentamos el proceso (pasando reintentos=0 para no hacer un bucle infinito)
            return procesar_metricas(df_hoy, reintentos=0)
        else:
            # Si falla incluso después de limpiarlo, lanzamos la excepción
            raise Exception(f"Fallo crítico al procesar métricas tras resetear la BD: {e}")


def generar_grafico(df):
    top_5 = df.sort_values(by='Crecimiento', ascending=False).head(5)
    if top_5['Crecimiento'].sum() == 0:
        top_5 = df.sort_values(by='Vistas', ascending=False).head(5)
        col_x = 'Vistas'
    else:
        col_x = 'Crecimiento'
    
    plt.figure(figsize=(10, 6))
    sns.barplot(x=col_x, y=top_5['Título'].str[:30], data=top_5, palette='viridis')
    plt.title(f"Reporte YouTube AR - {datetime.now().strftime('%d/%m')}")
    path = "grafico.png"
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    return path

def main():
    try:
        init_db()
        df = obtener_datos_youtube()
        crec, virales = procesar_metricas(df)
        img = generar_grafico(df)
        excel = f"reporte_{datetime.now().strftime('%Y%m%d')}.xlsx"
        df.to_excel(excel, index=False)
        
        msg = f"🇦🇷 *REPORTE DIARIO*\n📈 Crecimiento: +{crec:,} vistas\n\n" + "\n".join(virales)
        enviar_telegram(msg, foto_path=img, archivo_path=excel)
        git_push_db()
    except Exception as e:
        print(f"Error en el Script: {e}")

if __name__ == "__main__":
    main()
