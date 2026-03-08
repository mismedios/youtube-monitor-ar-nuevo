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
API_KEY = str(os.getenv('YT_API_KEY', '')).strip()
TELEGRAM_TOKEN = str(os.getenv('TELEGRAM_TOKEN', '')).strip()
TELEGRAM_CHAT_ID = str(os.getenv('TELEGRAM_CHAT_ID', '')).strip()

# --- DICCIONARIO DE CANALES ---
# Reemplazá los "UC_ID..." por los IDs reales de los canales que querés monitorear.
CANALES = {
    "@TNTSportsAR": "UCI5RY8G0ar-hLIaUJvx58Lw", # Tu canal original
    "@ESPNFans": "UCFmMw7yTuLTCuMhpZD5dVsg", # Reemplazar por el ID real que necesites
    "@LigaProfesional": "UC_f0pZidCOPlpAMoOsX6S6Q",
}

UMBRAL_VIRAL = 10000 
UMBRAL_LIKES = 500
UMBRAL_COMENTARIOS = 100

def init_db(db_name):
    conn = sqlite3.connect(db_name)
    conn.execute('''CREATE TABLE IF NOT EXISTS canal_history (fecha TEXT, vistas_totales INTEGER)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS video_history (video_id TEXT, titulo TEXT, vistas INTEGER, likes INTEGER, comentarios INTEGER, fecha TEXT)''')
    conn.commit()
    conn.close()

def enviar_telegram(mensaje, foto_path=None, archivo_path=None):
    base_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
    try:
        if foto_path and os.path.exists(foto_path):
            with open(foto_path, 'rb') as f:
                r = requests.post(f"{base_url}/sendPhoto", 
                                  data={'chat_id': TELEGRAM_CHAT_ID, 'caption': mensaje, 'parse_mode': 'Markdown'}, 
                                  files={'photo': f})
        else:
            r = requests.post(f"{base_url}/sendMessage", 
                              data={'chat_id': TELEGRAM_CHAT_ID, 'text': mensaje, 'parse_mode': 'Markdown'})
        
        if archivo_path and os.path.exists(archivo_path):
            with open(archivo_path, 'rb') as f:
                requests.post(f"{base_url}/sendDocument", data={'chat_id': TELEGRAM_CHAT_ID}, files={'document': f})
        
        print(f"Telegram status: {r.status_code}")
    except Exception as e:
        print(f"❌ Error al enviar a Telegram: {e}")

def git_push_db(lista_dbs):
    try:
        subprocess.run(["git", "config", "--global", "user.name", "YouTube Bot AR"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "bot@github.com"], check=True)
        
        # Agregamos dinámicamente cada base de datos generada
        for db in lista_dbs:
            if os.path.exists(db):
                subprocess.run(["git", "add", db], check=True)
                
        subprocess.run(["git", "commit", "-m", f"🔄 Update DBs: {datetime.now().date()}"], check=True)
        subprocess.run(["git", "push"], check=True)
    except Exception as e:
        print(f"No hay cambios para commitear o hubo un error en Git Push: {e}")

def obtener_datos_youtube(channel_id):
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    ch_res = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    uploads_id = ch_res['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    video_ids = []
    next_page = None
    # 4 iteraciones de 50 = últimos 200 videos por canal
    for _ in range(4):
        res = youtube.playlistItems().list(playlistId=uploads_id, part='contentDetails', maxResults=50, pageToken=next_page).execute()
        video_ids.extend([item['contentDetails']['videoId'] for item in res['items']])
        next_page = res.get('nextPageToken')
        if not next_page: break

    datos_videos = []
    for i in range(0, len(video_ids), 50):
        v_res = youtube.videos().list(id=','.join(video_ids[i:i+50]), part='statistics,snippet,contentDetails').execute()
        for item in v_res['items']:
            datos_videos.append({
                'ID': item['id'],
                'Título': item['snippet']['title'],
                'Fecha Publicación': item['snippet'].get('publishedAt', '')[:10],
                'Duración': item['contentDetails'].get('duration', ''),
                'Vistas': int(item['statistics'].get('viewCount', 0)),
                'Me Gusta': int(item['statistics'].get('likeCount', 0)),
                'Comentarios': int(item['statistics'].get('commentCount', 0)),
                'URL': f"https://youtu.be/{item['id']}"
            })
    return pd.DataFrame(datos_videos)

def procesar_metricas(df_hoy, db_name, reintentos=1):
    try:
        conn = sqlite3.connect(db_name)
        fecha_hoy = datetime.now().date().isoformat()
        cursor = conn.cursor()
        
        vistas_totales_hoy = int(df_hoy['Vistas'].sum())
        
        cursor.execute("SELECT vistas_totales FROM canal_history ORDER BY fecha DESC LIMIT 1")
        row = cursor.fetchone()
        
        vistas_ayer = int(row[0]) if row else vistas_totales_hoy
        crecimiento_total = vistas_totales_hoy - vistas_ayer
        
        conn.execute("INSERT INTO canal_history VALUES (?, ?)", (fecha_hoy, vistas_totales_hoy))

        df_hoy['Crecimiento'] = 0
        df_hoy['Crec_Likes'] = 0
        df_hoy['Crec_Comentarios'] = 0
        alertas = []
        
        for idx, row_v in df_hoy.iterrows():
            cursor.execute("SELECT vistas, likes, comentarios FROM video_history WHERE video_id=? ORDER BY fecha DESC LIMIT 1", (row_v['ID'],))
            res_v = cursor.fetchone()
            
            if res_v:
                vistas_ayer_v = int(res_v[0])
                likes_ayer_v = int(res_v[1]) if res_v[1] is not None else int(row_v['Me Gusta'])
                comentarios_ayer_v = int(res_v[2]) if res_v[2] is not None else int(row_v['Comentarios'])
                
                diff_vistas = int(row_v['Vistas']) - vistas_ayer_v
                diff_likes = int(row_v['Me Gusta']) - likes_ayer_v
                diff_comentarios = int(row_v['Comentarios']) - comentarios_ayer_v
                
                df_hoy.at[idx, 'Crecimiento'] = diff_vistas
                df_hoy.at[idx, 'Crec_Likes'] = diff_likes
                df_hoy.at[idx, 'Crec_Comentarios'] = diff_comentarios
                
                if diff_vistas >= UMBRAL_VIRAL:
                    alertas.append(f"🚀 *VIRAL:* {row_v['Título']} (+{diff_vistas:,} vistas)")
                if diff_likes >= UMBRAL_LIKES:
                    alertas.append(f"❤️ *LLUVIA DE LIKES:* {row_v['Título']} (+{diff_likes:,} likes)")
                if diff_comentarios >= UMBRAL_COMENTARIOS:
                    alertas.append(f"💬 *DEBATE INTENSO:* {row_v['Título']} (+{diff_comentarios:,} comentarios)")
            
            conn.execute("INSERT INTO video_history VALUES (?, ?, ?, ?, ?, ?)", 
                         (row_v['ID'], row_v['Título'], int(row_v['Vistas']), int(row_v['Me Gusta']), int(row_v['Comentarios']), fecha_hoy))
        
        conn.commit()
        conn.close()
        return crecimiento_total, alertas

    except (ValueError, sqlite3.DatabaseError, sqlite3.OperationalError) as e:
        conn.close()
        if reintentos > 0:
            print(f"⚠️ BD corrupta o desactualizada detectada en {db_name} ({e}). Eliminando y reseteando...")
            if os.path.exists(db_name):
                os.remove(db_name)
            init_db(db_name)
            return procesar_metricas(df_hoy, db_name, reintentos=0)
        else:
            raise Exception(f"Fallo crítico al procesar métricas en {db_name}: {e}")

def generar_grafico(df, nombre_canal):
    top_5 = df.sort_values(by='Crecimiento', ascending=False).head(5)
    if top_5['Crecimiento'].sum() == 0:
        top_5 = df.sort_values(by='Vistas', ascending=False).head(5)
        col_x = 'Vistas'
    else:
        col_x = 'Crecimiento'
    
    plt.figure(figsize=(10, 6))
    sns.barplot(x=col_x, y=top_5['Título'].str[:30], data=top_5, palette='viridis')
    plt.title(f"Reporte {nombre_canal} - {datetime.now().strftime('%d/%m')}")
    path = f"grafico_{nombre_canal}.png"
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    return path

def main():
    dbs_procesadas = []
    
    for nombre_canal, channel_id in CANALES.items():
        print(f"\n🔄 Procesando canal: {nombre_canal}...")
        
        try:
            db_name = f"stats_{nombre_canal}.db"
            excel_name = f"reporte_{nombre_canal}_{datetime.now().strftime('%Y%m%d')}.xlsx"
            
            # 1. Iniciar/Verificar BD
            init_db(db_name)
            
            # 2. Descargar datos de este canal específico
            df = obtener_datos_youtube(channel_id)
            
            # 3. Procesar métricas cruzando con la BD de este canal
            crec, virales = procesar_metricas(df, db_name)
            
            # 4. Generar gráfico y guardar Excel
            img = generar_grafico(df, nombre_canal)
            df.to_excel(excel_name, index=False)
            
            # 5. Enviar a Telegram
            msg = f"📺 *REPORTE DIARIO - {nombre_canal}*\n📈 Crecimiento: +{crec:,} vistas\n\n" + "\n".join(virales)
            enviar_telegram(msg, foto_path=img, archivo_path=excel_name)
            
            # Registrar la BD para el push de Git
            dbs_procesadas.append(db_name)
            
        except Exception as e:
            print(f"❌ Error procesando {nombre_canal}: {e}")
            enviar_telegram(f"⚠️ Error en el monitor del canal {nombre_canal}: {e}")
            continue # Si un canal falla, el bot sigue con el siguiente
            
    # Al terminar todos los canales, subimos las bases de datos a GitHub
    git_push_db(dbs_procesadas)

if __name__ == "__main__":
    main()
