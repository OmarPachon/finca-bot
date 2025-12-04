# -*- coding: utf-8 -*-
"""
app.py - Webhook para WhatsApp + Twilio + Gestión completa de fincas y empleados
"""

import os
import sys
import traceback
import datetime
import psycopg2
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse

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

# === RUTA: ACTIVAR FINCA CON EMPLEADOS (uso administrador) ===
@app.route("/activar-finca-con-empleados")
def activar_finca_con_empleados():
    nombre_finca = request.args.get("nombre")
    telefono_dueno = request.args.get("telefono_dueno")
    empleados = request.args.get("empleados", "").strip()
    
    if not nombre_finca or not telefono_dueno:
        return (
            "❌ Faltan datos.\n"
            "Usa: ?nombre=MiFinca&telefono_dueno=whatsapp:+57314...&empleados=whatsapp:+57310...,whatsapp:+57311..."
        ), 400

    # Procesar lista de empleados
    lista_empleados = []
    if empleados:
        lista_empleados = [tel.strip() for tel in empleados.split(",") if tel.strip()]
    
    if len(lista_empleados) > 3:
        return "❌ Máximo 3 empleados permitidos por finca.", 400

    # Validar formato de números
    all_numbers = [telefono_dueno] + lista_empleados
    for num in all_numbers:
        if not num.startswith("whatsapp:+57") or len(num) < 15:
            return f"❌ Número inválido: {num}. Usa formato: whatsapp:+573143539351", 400

    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada", 500

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                # Crear finca
                cur.execute("""
                    INSERT INTO fincas (nombre, telefono_dueño, suscripcion_activa, vencimiento_suscripcion)
                    VALUES (%s, %s, %s, CURRENT_DATE + INTERVAL '30 days')
                    ON CONFLICT (nombre) DO UPDATE 
                    SET telefono_dueño = EXCLUDED.telefono_dueño,
                        suscripcion_activa = EXCLUDED.suscripcion_activa,
                        vencimiento_suscripcion = EXCLUDED.vencimiento_suscripcion
                    RETURNING id
                """, (nombre_finca, telefono_dueno, True))
                finca_id = cur.fetchone()[0]

                # Registrar dueño
                cur.execute("""
                    INSERT INTO usuarios (telefono_whatsapp, nombre, rol, finca_id)
                    VALUES (%s, %s, 'dueño', %s)
                    ON CONFLICT (telefono_whatsapp) DO UPDATE 
                    SET finca_id = EXCLUDED.finca_id
                """, (telefono_dueno, "Dueño", finca_id))

                # Registrar empleados
                for emp in lista_empleados:
                    cur.execute("""
                        INSERT INTO usuarios (telefono_whatsapp, nombre, rol, finca_id)
                        VALUES (%s, %s, 'trabajador', %s)
                        ON CONFLICT (telefono_whatsapp) DO UPDATE 
                        SET finca_id = EXCLUDED.finca_id
                    """, (emp, "Empleado", finca_id))

                conn.commit()
        
        empleados_txt = ", ".join(lista_empleados) if lista_empleados else "ninguno"
        return (
            f"✅ Finca '{nombre_finca}' activada con éxito.\n"
            f"• Dueño: {telefono_dueno}\n"
            f"• Empleados ({len(lista_empleados)}): {empleados_txt}\n"
            f"• Válida hasta: {datetime.date.today() + datetime.timedelta(days=30)}"
        ), 200

    except Exception as e:
        return f"❌ Error al activar finca con empleados: {e}", 500

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
                    return (
                        f"📱 Tu finca: **{row[1]}**\n"
                        f"🆔 ID: **{row[0]}**\n\n"
                        "Envía este ID junto con tu comprobante de pago."
                    ), 200
                else:
                    return "❌ No estás registrado en ninguna finca.", 404
    except Exception as e:
        return f"❌ Error al consultar finca: {e}", 500

# === RUTA TEMPORAL: REINICIAR BASE DE DATOS ===
@app.route("/reiniciar-bd")
def reiniciar_bd():
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada en Render.", 500

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
        error_msg = f"❌ Error al reiniciar BD:\n{type(e).__name__}: {e}\n\n{traceback.format_exc()}"
        print(error_msg)
        return error_msg, 500

# === RUTA DE REPORTE (DESACTIVADA) ===
@app.route("/reporte")
def descargar_reporte():
    return "🔒 Acceso restringido. Usa 'exportar reporte' desde WhatsApp.", 403

# === INICIO DEL SERVIDOR ===
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🌍 Servidor iniciando en http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)