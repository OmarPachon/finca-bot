# -*- coding: utf-8 -*-
"""
app.py - Webhook para WhatsApp + Twilio + Ruta temporal para reiniciar BD
"""

import os
import sys
import traceback
import datetime
import psycopg2
import io
from flask import Flask, request, send_file
from twilio.twiml.messaging_response import MessagingResponse
import pandas as pd

print("🚀 Iniciando app.py...")

sys.path.append(os.path.dirname(__file__))

# Intentar importar bot
try:
    import bot
    print("✅ bot.py importado correctamente")
except Exception as e:
    print(f"❌ FALLO AL IMPORTAR BOT: {type(e).__name__}: {e}")
    print(f"📋 Traceback:\n{traceback.format_exc()}")
    bot = None

app = Flask(__name__)

# === RUTA PRINCIPAL ===
@app.route("/")
def home():
    return "🌱 Finca Digital Bot - Multi-Finca Activo", 200

# === WEBHOOK PARA TWILIO ===
@app.route("/webhook", methods=["POST"])
def webhook():
    print("🔍 [WEBHOOK] Entrando al endpoint /webhook")
    incoming_msg = request.form.get("Body", "").strip()
    sender = request.form.get("From", "")
    print(f"📩 MENSAJE RECIBIDO: '{incoming_msg}' desde {sender}")

    if not incoming_msg:
        r = MessagingResponse()
        r.message("❌ Mensaje vacío.")
        return str(r)

    if bot is None:
        r = MessagingResponse()
        r.message("❌ Error interno: módulo 'bot' no disponible")
        return str(r)

    try:
        respuesta = bot.procesar_mensaje_whatsapp(incoming_msg, remitente=sender)
        print(f"✅ RESPUESTA GENERADA: {respuesta}")
    except Exception as e:
        print(f"❌ ERROR EN FUNCION: {type(e).__name__}: {e}")
        print(f"📋 Traceback:\n{traceback.format_exc()}")
        respuesta = "❌ Hubo un error al procesar tu mensaje. Intenta más tarde."

    r = MessagingResponse()
    r.message(respuesta)
    print("📤 [WEBHOOK] Enviando respuesta a Twilio")
    return str(r)

# === RUTA TEMPORAL: REINICIAR BASE DE DATOS (solo para Render Free) ===
@app.route("/reiniciar-bd")
def reiniciar_bd():
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada en Render.", 500

        import psycopg2
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                # Eliminar todas las tablas en orden inverso (CASCADE maneja dependencias)
                cur.execute("""
                    DROP TABLE IF EXISTS 
                        registros, 
                        salud_animal, 
                        animales, 
                        usuarios, 
                        fincas 
                    CASCADE;
                """)
                conn.commit()
        
        # Reutilizar tu función de inicialización
        if bot and hasattr(bot, 'inicializar_bd'):
            if bot.inicializar_bd():
                return "✅ Base de datos reiniciada completamente.\n¡Ahora puedes registrar tu finca desde WhatsApp!", 200
            else:
                return "❌ Falló la inicialización automática de las tablas.", 500
        else:
            return "⚠️ El módulo 'bot' no está disponible para reinicializar.", 500

    except Exception as e:
        import traceback
        error_msg = f"❌ Error al reiniciar BD:\n{type(e).__name__}: {e}\n\n{traceback.format_exc()}"
        print(error_msg)
        return error_msg, 500

# === RUTA DE REPORTE (DESACTIVADA POR SEGURIDAD) ===
@app.route("/reporte")
def descargar_reporte():
    return "🔒 Acceso restringido. Usa 'exportar reporte' desde WhatsApp.", 403

# === INICIO DEL SERVIDOR ===
if __name__ == "__main__":
    port = int(oselsius.environ.get("PORT", 10000))
    print(f"🌍 Servidor iniciando en http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)