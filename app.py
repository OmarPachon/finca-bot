# -*- coding: utf-8 -*-
"""
app.py - Webhook para WhatsApp + Twilio + Activación manual de fincas
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

# === RUTA: ACTIVAR FINCA MANUALMENTE (uso administrador) ===
@app.route("/activar-finca")
def activar_finca():
    finca_id = request.args.get("id")
    if not finca_id:
        return "❌ Usa: /activar-finca?id=123", 400
    try:
        finca_id = int(finca_id)
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada", 500

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE fincas 
                    SET suscripcion_activa = TRUE, 
                        vencimiento_suscripcion = CURRENT_DATE + INTERVAL '30 days'
                    WHERE id = %s
                """, (finca_id,))
                if cur.rowcount == 0:
                    return f"❌ No se encontró la finca con ID {finca_id}", 404
                conn.commit()
        return f"✅ Finca ID {finca_id} activada hasta {datetime.date.today() + datetime.timedelta(days=30)}", 200
    except Exception as e:
        return f"❌ Error al activar finca: {e}", 500

# === RUTA: CONSULTAR MI FINCA_ID (para dueños) ===
@app.route("/mi-finca-id")
def mi_finca_id():
    telefono = request.args.get("telefono")
    if not telefono:
        return "❌ Usa: /mi-finca-id?telefono=whatsapp:+573143539351", 400
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada", 500

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT f.id, f.nombre 
                    FROM fincas f
                    JOIN usuarios u ON f.id = u.finca_id
                    WHERE u.telefono_whatsapp = %s
                """, (telefono,))
                row = cur.fetchone()
                if row:
                    return f"📱 Tu finca: **{row[1]}**\n🆔 ID para activación: **{row[0]}**\n\nEnvía este ID junto con tu comprobante de pago.", 200
                else:
                    return "❌ No estás registrado en ninguna finca.", 404
    except Exception as e:
        return f"❌ Error al consultar finca: {e}", 500

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
    port = int(os.environ.get("PORT", 10000))
    print(f"🌍 Servidor iniciando en http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)