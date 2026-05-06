import os
import json
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Alcances (Scopes) para Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']

def get_gdrive_service(client_secret_input, token_input):
    """
    Obtiene el servicio de Google Drive usando OAuth2 (token.json o string JSON).
    """
    creds = None
    is_token_json = token_input.strip().startswith('{')
    
    try:
        if is_token_json:
            # Si el token es un string JSON directo (desde secrets.toml)
            info = json.loads(token_input)
            creds = Credentials.from_authorized_user_info(info, SCOPES)
        elif os.path.exists(token_input):
            # Si el token es una ruta de archivo
            creds = Credentials.from_authorized_user_file(token_input, SCOPES)
    except Exception as e:
        return None, f"Error al cargar credenciales: {e}"
    
    # Si no hay credenciales válidas, intentar refrescar o devolver error
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                # Solo guardar el token refrescado si token_input era una ruta de archivo existente
                if not is_token_json and os.path.exists(token_input):
                    with open(token_input, 'w') as token:
                        token.write(creds.to_json())
            except Exception as e:
                return None, f"Error al refrescar token: {e}"
        else:
            return None, "Token no encontrado o no válido. Asegúrese de que el token JSON sea correcto."
        
    return build('drive', 'v3', credentials=creds), None

def upload_to_gdrive(file_path, folder_id, client_secret_file, token_file):
    """
    Sube un archivo a una carpeta específica de Google Drive usando OAuth2.
    """
    if not os.path.exists(file_path):
        return None, f"Archivo no encontrado: {file_path}"
    
    service, err = get_gdrive_service(client_secret_file, token_file)
    if err:
        return None, err
    
    try:
        # Metadatos del archivo
        file_name = os.path.basename(file_path)
        
        # 1. Buscar si el archivo ya existe en esa carpeta
        query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
        response = service.files().list(q=query, spaces='drive', fields='files(id)').execute()
        files = response.get('files', [])
        
        # Definir el tipo de medio
        media = MediaFileUpload(file_path, resumable=False)
        
        if files:
            # 2. Si existe, actualizarlo (sobrescribir)
            file_id = files[0]['id']
            file = service.files().update(
                fileId=file_id,
                media_body=media,
                fields='id, webViewLink'
            ).execute()
        else:
            # 3. Si no existe, crear uno nuevo
            file_metadata = {
                'name': file_name,
                'parents': [folder_id]
            }
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink'
            ).execute()
        
        return file.get('webViewLink'), None
        
    except Exception as e:
        return None, str(e)
