# -*- coding: utf-8 -*-
import os
import sys
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

print("🚀 Iniciando app.py...")
sys.path.append(os.path.dirname(__file__))

# Intentar importar módulos clave
try:
    import sqlite3
    print("✅ sqlite3 importado correctamente")
except Exception as e:
    print(f"❌ ERROR AL IMPORTAR sqlite3: {e}")

try:
    import re
    print("✅ re importado correctamente")
except Exception as e:
    print(f"❌ ERROR AL IMPORTAR re: {e}")

try:
    import datetime
    print("✅ datetime importado correctamente")
except Exception as e:
    print(f"❌ ERROR AL IMPORTAR datetime: {e}")

# Intentar importar bot
try:
    import bot
    print("✅ bot.py importado correctamente")
except Exception as e:
    print(f"❌ FALLO AL IMPORTAR BOT: {type(e).__name__}: {e}")
    print(f"📋 Traceback:\n{traceback.format_exc()}")
    bot = None

app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def webhook():
    print("🔍 [WEBHOOK] Entrando al endpoint /webhook")
    
    incoming_msg = request.form.get("Body", "").strip()
    sender = request.form.get("From", "")
    
    print(f"📩 MENSAJE RECIBIDO: '{incoming_msg}' desde {sender}")

    if not incoming_msg:
        print("⚠️ Mensaje vacío o no Body")
    
    if bot is None:
        print("❌ bot es None, no se puede procesar")
        r = MessagingResponse()
        r.message("❌ Error: módulo 'bot' no disponible")
        return str(r)

    try:
        respuesta = bot.procesar_mensaje_whatsapp(incoming_msg)
        print(f"✅ RESPUESTA GENERADA: {respuesta}")
    except Exception as e:
        import traceback
        print(f"❌ ERROR EN FUNCION: {type(e).__name__}: {e}")
        print(f"📋 Traceback:\n{traceback.format_exc()}")
        respuesta = "Hubo un error al procesar tu mensaje. Intenta de nuevo."

    r = MessagingResponse()
    r.message(respuesta)
    print("📤 [WEBHOOK] Enviando respuesta a Twilio")
    return str(r)
# Inicio del servidor
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"🌍 Servidor iniciando en http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)