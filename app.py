# -*- coding: utf-8 -*-
"""
app.py - Webhook para WhatsApp + Twilio + Gestión completa de fincas y empleados
"""

import os
import sys
import traceback
import datetime
import psycopg2
import re
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

# === RUTA: FORMULARIO AMIGABLE PARA ACTIVAR FINCA (uso administrador) ===
@app.route("/activar")
def formulario_activacion():
    return '''
    <html>
    <head><title>Activar Finca - Finca Digital</title></head>
    <body style="font-family: Arial; max-width: 600px; margin: 40px auto; padding: 20px; border: 1px solid #ccc; border-radius: 8px; background: #f9f9f9;">
        <h2 style="color: #28a745;">✅ Activar Finca con Empleados</h2>
        <form action="/activar-finca-con-empleados" method="GET">
            <p>
                <label><strong>Nombre de la finca:</strong></label><br>
                <input type="text" name="nombre" placeholder="Ej: Hacienda El Frayle" style="width: 100%; padding: 10px; margin: 8px 0; border: 1px solid #ccc; border-radius: 4px;" required>
            </p>
            <p>
                <label><strong>Número del dueño (10 dígitos):</strong></label><br>
                <input type="text" name="telefono_dueno" placeholder="3143539351" style="width: 100%; padding: 10px; margin: 8px 0; border: 1px solid #ccc; border-radius: 4px;" required>
                <br><small style="color: #666;">Ej: 3143539351 (sin espacios ni +57)</small>
            </p>
            <p>
                <label><strong>Números de empleados (máx. 3, separados por comas):</strong></label><br>
                <input type="text" name="empleados" placeholder="3101234567,3119876543" style="width: 100%; padding: 10px; margin: 8px 0; border: 1px solid #ccc; border-radius: 4px;">
                <br><small style="color: #666;">Ej: 3101234567,3119876543</small>
            </p>
            <button type="submit" style="background: #28a745; color: white; padding: 12px 24px; border: none; border-radius: 6px; font-size: 16px; cursor: pointer; margin-top: 10px;">
                ✅ Activar Finca
            </button>
        </form>
        <br>
        <small style="color: #888;">🔒 Solo para uso del administrador. Los números se convertirán automáticamente a formato WhatsApp.</small>
    </body>
    </html>
    '''

# === RUTA: ACTIVAR FINCA CON EMPLEADOS (versión mejorada) ===
@app.route("/activar-finca-con-empleados")
def activar_finca_con_empleados():
    nombre_finca = request.args.get("nombre", "").strip()
    telefono_dueno = request.args.get("telefono_dueno", "").strip()
    empleados_raw = request.args.get("empleados", "").strip()
    
    if not nombre_finca or not telefono_dueno:
        return "❌ Faltan datos: nombre y número del dueño son obligatorios.", 400

    # Función para limpiar y validar un número colombiano
    def formatear_numero(num):
        # Eliminar todo excepto dígitos
        solo_digitos = re.sub(r'\D', '', num)
        if len(solo_digitos) == 10 and solo_digitos.startswith('3'):
            return f"whatsapp:+57{solo_digitos}"
        return None

    # Procesar dueño
    dueno_formateado = formatear_numero(telefono_dueno)
    if not dueno_formateado:
        return f"❌ Número de dueño inválido: {telefono_dueno}. Debe ser 10 dígitos y empezar con 3.", 400

    # Procesar empleados
    lista_empleados = []
    if empleados_raw:
        for num in empleados_raw.split(","):
            num = num.strip()
            if num:
                emp_formateado = formatear_numero(num)
                if emp_formateado:
                    lista_empleados.append(emp_formateado)
                else:
                    return f"❌ Número de empleado inválido: {num}. Usa 10 dígitos (ej: 3101234567).", 400
    
    if len(lista_empleados) > 3:
        return "❌ Máximo 3 empleados permitidos.", 400

    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada", 500

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                # Crear o actualizar finca
                cur.execute("""
                    INSERT INTO fincas (nombre, telefono_dueño, suscripcion_activa, vencimiento_suscripcion)
                    VALUES (%s, %s, %s, CURRENT_DATE + INTERVAL '30 days')
                    ON CONFLICT (nombre) DO UPDATE 
                    SET telefono_dueño = EXCLUDED.telefono_dueño,
                        suscripcion_activa = EXCLUDED.suscripcion_activa
                    RETURNING id
                """, (nombre_finca, dueno_formateado, True))
                finca_id = cur.fetchone()[0]

                # Registrar dueño
                cur.execute("""
                    INSERT INTO usuarios (telefono_whatsapp, nombre, rol, finca_id)
                    VALUES (%s, %s, 'dueño', %s)
                    ON CONFLICT (telefono_whatsapp) DO UPDATE 
                    SET finca_id = EXCLUDED.finca_id
                """, (dueno_formateado, "Dueño", finca_id))

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
            f"✅ Finca '{nombre_finca}' activada con éxito.<br>"
            f"• Dueño: {dueno_formateado}<br>"
            f"• Empleados ({len(lista_empleados)}): {empleados_txt}<br>"
            f"• Válida hasta: {datetime.date.today() + datetime.timedelta(days=30)}"
        ), 200

    except Exception as e:
        return f"❌ Error al activar: {e}", 500

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
                        f"📱 Tu finca: <strong>{row[1]}</strong><br>"
                        f"🆔 ID: <strong>{row[0]}</strong><br><br>"
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