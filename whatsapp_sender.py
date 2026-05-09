import asyncio
import streamlit as st
from wasenderapi import create_async_wasender
from wasenderapi.errors import WasenderAPIError
from wasenderapi.models import RetryConfig 

# Configuración de reintentos
retry_config = RetryConfig(enabled=True, max_retries=3)

# Función asíncrona para enviar mensajes
async def send_whatsapp_message_async(recipient_phone_number: str, text_body: str, image_url: str = None, document_url: str = None, filenameUrl: str = None, api_key: str = None):
    """
    Envía un mensaje de texto y/o imagen a un número de WhatsApp usando Wasender API.
    """
    if not api_key:
        api_key = st.secrets["wasender"]["API_KEY"]
    
    # Es crucial usar el cliente asíncrono como un context manager para asegurar que se cierre correctamente.
    async with create_async_wasender(api_key=api_key, retry_options=retry_config) as client:
        try:
            if image_url:
                # Enviar imagen con caption de texto
                response = await client.send_image(
                    to=recipient_phone_number,
                    url=image_url,
                    caption=text_body # El texto como caption de la imagen
                )
            elif document_url:
                # Enviar documento (ejem.:pdf) con caption de texto
                response = await client.send_document(
                    to=recipient_phone_number,
                    url=document_url,
                    filename=filenameUrl,
                    caption=text_body # El texto como caption del documento
                )

            else:
                # Enviar solo texto
                response = await client.send_text(
                    to=recipient_phone_number,
                    text_body=text_body
                )
            
            # Manejo de la respuesta
            if response.response and response.response.data:
                st.success(f"Mensaje enviado a {recipient_phone_number}. ID: {response.response.data.message_id}")
            else:
                st.warning(f"Mensaje enviado a {recipient_phone_number}, pero sin ID de mensaje. Status: {response.response.message}")
            
            if response.rate_limit:
                st.info(f"Límite de tasa restante: {response.rate_limit.remaining}/{response.rate_limit.limit}")
            
            return True
        
        except WasenderAPIError as e:
            st.error(f"Error de la API al enviar mensaje a {recipient_phone_number}: {e.message}")
            if e.status_code == 429:
                st.warning(f"Límite de tasa alcanzado. Reintentando en {e.retry_after} segundos (si el reintento está habilitado).")
            elif e.error_details:
                st.error(f"Detalles del error: Código {e.error_details.code}, Mensaje: {e.error_details.message}")
            return False
        except Exception as e:
            st.error(f"Error inesperado al enviar mensaje a {recipient_phone_number}: {e}")
            return False

# Función para enviar múltiples mensajes en paralelo
async def send_multiple_messages_parallel(users_data: list, image_url: str = None, document_url: str = None, filenameUrl: str = None):
    """
    Envía mensajes a una lista de usuarios en paralelo.
    users_data: Lista de diccionarios, cada uno con 'phone' y 'message'.
    """
    tasks = []
    for user in users_data:
        phone = user['phone']
        message = user['message']
        tasks.append(send_whatsapp_message_async(phone, message, image_url, document_url, filenameUrl))
    
    # Ejecuta todas las tareas concurrentemente
    results = await asyncio.gather(*tasks)
    return results