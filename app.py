# -*- coding: utf-8 -*-
"""
app.py - Webhook para WhatsApp + Twilio + Ruta de exportación a Excel
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
    return "🌱 Finca Digital Bot está activo y funcionando", 200

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

# === RUTA PARA EXPORTAR REPORTE A EXCEL ===
@app.route("/reporte")
def descargar_reporte():
    if not bot.BD_OK:
        return "❌ Base de datos no disponible", 500

    tipo = request.args.get("tipo", "semanal")
    formato = request.args.get("formato", "excel")

    dias_map = {"diario": 1, "semanal": 7, "quincenal": 15, "mensual": 30}
    dias = dias_map.get(tipo, 7)
    inicio = (datetime.date.today() - datetime.timedelta(days=dias)).isoformat()

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return "❌ DATABASE_URL no configurada", 500

    registros = []
    try:
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT fecha, tipo_actividad, detalle, lugar, cantidad, unidad, valor, jornales, observacion
                    FROM registros WHERE fecha >= %s ORDER BY fecha
                """, (inicio,))
                for row in cursor.fetchall():
                    registros.append({
                        "Fecha": row[0],
                        "Actividad": row[1],
                        "Detalle": row[2],
                        "Lugar": row[3],
                        "Cantidad": row[4],
                        "Unidad": row[5],
                        "Valor_COP": row[6],
                        "Jornales": row[7],
                        "Observación": row[8]
                    })
    except Exception as e:
        return f"❌ Error al leer BD: {e}", 500

    if not registros:
        return "⚠️ No hay datos en el período solicitado.", 404

    df = pd.DataFrame(registros)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name="Reporte")
    output.seek(0)

    filename = f"reporte_{tipo}_{datetime.date.today().isoformat()}.xlsx"
    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename
    )

# === INICIO DEL SERVIDOR ===
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🌍 Servidor iniciando en http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)