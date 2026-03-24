# -*- coding: utf-8 -*-
"""
app.py - Webhook para WhatsApp + Twilio + Gestión completa de fincas y empleados
Versión: Dashboard con filtros, gráficos 2D, exportación a Excel y procesamiento completo
"""
import os
import sys
import traceback
import datetime
import psycopg2
import re
import secrets
from flask import Flask, request, send_file
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

# === INICIALIZAR BASE DE DATOS AL INICIAR LA APP ===
if bot and hasattr(bot, 'inicializar_bd'):
    try:
        if bot.inicializar_bd():
            print("✅ Base de datos inicializada al arrancar la app.")
        else:
            print("⚠️ La inicialización de la base de datos falló o ya estaba lista.")
    except Exception as e:
        print(f"❌ Error al inicializar BD al inicio: {e}")
        print(traceback.format_exc())
else:
    print("⚠️ Módulo 'bot' no disponible para inicializar BD al inicio.")

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

# === RUTA: FORMULARIO AMIGABLE PARA ACTIVAR FINCA ===
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
        <small style="color: #888;">🔒 Solo para uso del administrador.</small>
    </body>
    </html>
    '''

# === RUTA: ACTIVAR FINCA CON EMPLEADOS ===
@app.route("/activar-finca-con-empleados")
def activar_finca_con_empleados():
    nombre_finca = request.args.get("nombre", "").strip()
    telefono_dueno = request.args.get("telefono_dueno", "").strip()
    empleados_raw = request.args.get("empleados", "").strip()
    
    if not nombre_finca or not telefono_dueno:
        return "❌ Faltan datos: nombre y número del dueño son obligatorios.", 400

    def formatear_numero(num):
        solo_digitos = re.sub(r'\D', '', num)
        if len(solo_digitos) == 10 and solo_digitos.startswith('3'):
            return f"whatsapp:+57{solo_digitos}"
        return None

    dueno_formateado = formatear_numero(telefono_dueno)
    if not dueno_formateado:
        return f"❌ Número de dueño inválido: {telefono_dueno}. Debe ser 10 dígitos y empezar con 3.", 400

    lista_empleados = []
    if empleados_raw:
        for num in empleados_raw.split(","):
            num = num.strip()
            if num:
                emp_formateado = formatear_numero(num)
                if emp_formateado:
                    lista_empleados.append(emp_formateado)
                else:
                    return f"❌ Número de empleado inválido: {num}.", 400
    
    if len(lista_empleados) > 3:
        return "❌ Máximo 3 empleados permitidos.", 400

    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada", 500

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                clave_secreta = secrets.token_urlsafe(16)
                
                cur.execute("""
                    INSERT INTO fincas (nombre, telefono_dueño, suscripcion_activa, vencimiento_suscripcion, clave_secreta)
                    VALUES (%s, %s, %s, CURRENT_DATE + INTERVAL '30 days', %s)
                    ON CONFLICT (nombre) DO UPDATE 
                    SET telefono_dueño = EXCLUDED.telefono_dueño,
                        suscripcion_activa = EXCLUDED.suscripcion_activa,
                        clave_secreta = EXCLUDED.clave_secreta
                    RETURNING id
                """, (nombre_finca, dueno_formateado, True, clave_secreta))
                finca_id = cur.fetchone()[0]

                cur.execute("""
                    INSERT INTO usuarios (telefono_whatsapp, nombre, rol, finca_id)
                    VALUES (%s, %s, 'dueño', %s)
                    ON CONFLICT (telefono_whatsapp) DO UPDATE 
                    SET finca_id = EXCLUDED.finca_id
                """, (dueno_formateado, "Dueño", finca_id))

                for emp in lista_empleados:
                    cur.execute("""
                        INSERT INTO usuarios (telefono_whatsapp, nombre, rol, finca_id)
                        VALUES (%s, %s, 'trabajador', %s)
                        ON CONFLICT (telefono_whatsapp) DO UPDATE 
                        SET finca_id = EXCLUDED.finca_id
                    """, (emp, "Empleado", finca_id))

                conn.commit()
        
        empleados_txt = ", ".join(lista_empleados) if lista_empleados else "ninguno"
        url_dashboard = f"https://finca-bot-ukhk.onrender.com/finca/{clave_secreta}"
        return (
            f"✅ Finca '{nombre_finca}' activada con éxito.<br>"
            f"• Dueño: {dueno_formateado}<br>"
            f"• Empleados ({len(lista_empleados)}): {empleados_txt}<br>"
            f"• Válida hasta: {datetime.date.today() + datetime.timedelta(days=30)}<br><br>"
            f"🔐 <strong>Dashboard privado:</strong> <a href='{url_dashboard}' target='_blank'>{url_dashboard}</a>"
        ), 200

    except Exception as e:
        return f"❌ Error al activar: {e}", 500

# === RUTA: DASHBOARD POR FINCA (CON TODOS LOS FILTROS - CORREGIDO) ===
@app.route("/finca/<clave>")
def dashboard_finca(clave):
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada", 500

        hoy = datetime.date.today()
        
        # === OBTENER TODOS LOS FILTROS ===
        fecha_inicio_str = request.args.get("fecha_inicio")
        fecha_fin_str = request.args.get("fecha_fin")
        especie_filter = request.args.get("especie", "")
        corral_filter = request.args.get("corral", "")
        tipo_actividad_filter = request.args.get("tipo_actividad", "")

        # === PROCESAR FILTRO DE FECHAS ===
        if fecha_inicio_str and fecha_fin_str:
            try:
                fecha_inicio = datetime.datetime.strptime(fecha_inicio_str, "%Y-%m-%d").date()
                fecha_fin = datetime.datetime.strptime(fecha_fin_str, "%Y-%m-%d").date()
                if fecha_inicio.year != hoy.year or fecha_fin.year != hoy.year:
                    periodo_txt = " (fuera de rango - mostrando mes actual)"
                    fecha_inicio = hoy.replace(day=1)
                    fecha_fin = hoy
                elif fecha_inicio > fecha_fin:
                    periodo_txt = " (fecha inválida - mostrando mes actual)"
                    fecha_inicio = hoy.replace(day=1)
                    fecha_fin = hoy
                else:
                    periodo_txt = f" ({fecha_inicio.strftime('%d/%m')} al {fecha_fin.strftime('%d/%m')})"
            except:
                periodo_txt = " (mes actual)"
                fecha_inicio = hoy.replace(day=1)
                fecha_fin = hoy
        else:
            periodo_txt = " (mes actual)"
            fecha_inicio = hoy.replace(day=1)
            fecha_fin = hoy

        filtro_fecha_params = (fecha_inicio.isoformat(), fecha_fin.isoformat())

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT nombre, id FROM fincas WHERE clave_secreta = %s", (clave,))
                finca_row = cur.fetchone()
                if not finca_row:
                    return "❌ Acceso denegado. URL inválida.", 403
                nombre_finca, finca_id = finca_row

                # === OBTENER VALORES ÚNICOS PARA LOS FILTROS (DROPDOWNS) ===
                cur.execute("""
                    SELECT DISTINCT especie FROM animales 
                    WHERE finca_id = %s AND estado = 'activo'
                    ORDER BY especie
                """, (finca_id,))
                especies_disponibles = [row[0] for row in cur.fetchall()]

                cur.execute("""
                    SELECT DISTINCT corral FROM animales 
                    WHERE finca_id = %s AND estado = 'activo' AND corral IS NOT NULL
                    ORDER BY corral
                """, (finca_id,))
                corrales_disponibles = [row[0] for row in cur.fetchall()]

                cur.execute("""
                    SELECT DISTINCT tipo_actividad FROM registros 
                    WHERE finca_id = %s
                    ORDER BY tipo_actividad
                """, (finca_id,))
                tipos_actividad_disponibles = [row[0] for row in cur.fetchall()]

                # === INVENTARIO DE ANIMALES (CON FILTROS) ===
                inventario_query = """
                    SELECT especie, marca_o_arete, categoria, peso, corral
                    FROM animales
                    WHERE finca_id = %s AND estado = 'activo'
                """
                inventario_params = [finca_id]
                
                if especie_filter:
                    inventario_query += " AND especie = %s"
                    inventario_params.append(especie_filter)
                
                if corral_filter:
                    inventario_query += " AND corral = %s"
                    inventario_params.append(corral_filter)
                
                inventario_query += " ORDER BY especie, marca_o_arete"
                
                cur.execute(inventario_query, tuple(inventario_params))
                inventario = cur.fetchall()

                # === CONTAR ANIMALES POR ESPECIE ===
                cur.execute("""
                    SELECT especie, COUNT(*) 
                    FROM animales 
                    WHERE finca_id = %s AND estado = 'activo'
                    GROUP BY especie
                """, (finca_id,))
                animales_por_especie = cur.fetchall()
                bovinos = sum(c for esp, c in animales_por_especie if esp == 'bovino')
                porcinos = sum(c for esp, c in animales_por_especie if esp == 'porcino')
                otros = sum(c for esp, c in animales_por_especie if esp not in ['bovino', 'porcino'])

                # === ESTADO DE SANIDAD POR ANIMAL (CON FILTROS) ===
                sanidad_query = """
                SELECT 
                    a.marca_o_arete,
                    a.especie,
                    a.peso,
                    a.corral,
                    a.estado,
                    (SELECT sa.fecha || ' | ' || sa.tratamiento FROM salud_animal sa 
                     WHERE sa.id_externo = a.id_externo AND sa.tipo = 'vacuna' 
                     ORDER BY sa.fecha DESC LIMIT 1) AS ultima_vacuna,
                    (SELECT sa.fecha || ' | ' || sa.tratamiento FROM salud_animal sa 
                     WHERE sa.id_externo = a.id_externo AND sa.tipo = 'desparasitación' 
                     ORDER BY sa.fecha DESC LIMIT 1) AS ultima_desparasitacion,
                    (SELECT sa.fecha || ' | ' || sa.tratamiento FROM salud_animal sa 
                     WHERE sa.id_externo = a.id_externo AND sa.tipo = 'reproducción' 
                     ORDER BY sa.fecha DESC LIMIT 1) AS ultima_reproduccion
                FROM animales a
                WHERE a.finca_id = %s AND a.estado = 'activo'
                """
                sanidad_params = [finca_id]
                
                if especie_filter:
                    sanidad_query += " AND a.especie = %s"
                    sanidad_params.append(especie_filter)
                
                if corral_filter:
                    sanidad_query += " AND a.corral = %s"
                    sanidad_params.append(corral_filter)
                
                sanidad_query += " ORDER BY a.especie, a.marca_o_arete"
                
                cur.execute(sanidad_query, tuple(sanidad_params))
                sanidad_animales = cur.fetchall()

                # === MOVIMIENTOS (CON FILTROS) ===
                movimientos_query = """
                    SELECT fecha, tipo_actividad, detalle, lugar, cantidad, valor, observacion
                    FROM registros
                    WHERE finca_id = %s AND fecha BETWEEN %s AND %s
                """
                movimientos_params = [finca_id, fecha_inicio.isoformat(), fecha_fin.isoformat()]
                
                if tipo_actividad_filter:
                    movimientos_query += " AND tipo_actividad = %s"
                    movimientos_params.append(tipo_actividad_filter)
                
                movimientos_query += " ORDER BY fecha DESC, id DESC LIMIT 500"
                
                cur.execute(movimientos_query, tuple(movimientos_params))
                registros = cur.fetchall()

                # === FINANZAS (CORREGIDO - salida_animal cuenta como ingreso) ===
                finanzas_query = """
                    SELECT 
                        SUM(CASE WHEN tipo_actividad IN ('produccion', 'salida_animal') THEN valor ELSE 0 END),
                        SUM(CASE WHEN tipo_actividad = 'gasto' THEN valor ELSE 0 END) +
                        SUM(CASE WHEN jornales > 0 THEN valor ELSE 0 END)
                    FROM registros
                    WHERE finca_id = %s AND fecha BETWEEN %s AND %s
                """
                finanzas_params = [finca_id, fecha_inicio.isoformat(), fecha_fin.isoformat()]
                
                if tipo_actividad_filter:
                    finanzas_query += " AND tipo_actividad = %s"
                    finanzas_params.append(tipo_actividad_filter)
                
                cur.execute(finanzas_query, tuple(finanzas_params))
                finanzas = cur.fetchone()
                ingresos = finanzas[0] or 0
                gastos = finanzas[1] or 0
                balance = ingresos - gastos
                
                # === KPIs ADICIONALES (FASE 2) ===
                # 1. Total animales activos
                cur.execute("""
                SELECT COUNT(*) FROM animales
                WHERE finca_id = %s AND estado = 'activo'
                """, (finca_id,))
                total_animales = cur.fetchone()[0]

                # 2. Días restantes de suscripción
                cur.execute("""
                SELECT vencimiento_suscripcion FROM fincas
                WHERE id = %s
                """, (finca_id,))
                vencimiento = cur.fetchone()[0]
                dias_suscripcion = (vencimiento - hoy).days if vencimiento else 0

                # 3. Movimientos en el periodo
                cur.execute("""
                SELECT COUNT(*) FROM registros
                WHERE finca_id = %s AND fecha BETWEEN %s AND %s
                """, (finca_id, fecha_inicio.isoformat(), fecha_fin.isoformat()))
                total_movimientos = cur.fetchone()[0]

        # === CONTAR FILTROS ACTIVOS ===
        filtros_activos_count = sum(1 for f in [especie_filter, corral_filter, tipo_actividad_filter] if f)

        # === TEXTO Y COLOR PARA EL BALANCE (CORREGIDO - calcular DESPUÉS de finanzas) ===
        balance_txt = "Positivo" if balance >= 0 else "Negativo"
        balance_color = "#28a745" if balance >= 0 else "#dc3545"

        # === FUNCIÓN PARA CALCULAR ESTADO DE SANIDAD ===
        def calcular_estado_sanidad(fecha_ultima, dias_vencimiento=30):
            if not fecha_ultima:
                return "—"
            try:
                ultima = datetime.datetime.strptime(fecha_ultima, "%Y-%m-%d").date()
                dias_desde = (hoy - ultima).days
                if dias_desde <= dias_vencimiento:
                    return "✅"
                elif dias_desde <= dias_vencimiento * 2:
                    return "⚠️"
                else:
                    return "❌"
            except:
                return "—"

        # === GENERAR HTML ===
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <link rel="preconnect" href="https://fonts.googleapis.com">
            <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>{nombre_finca} - Finca Digital</title>
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
            <style>
            * {{ box-sizing: border-box; margin: 0; padding: 0; }}
            body {{
                font-family: 'Segoe UI', Arial, sans-serif;
                max-width: 1400px;
                margin: 0 auto;
                padding: 20px;
                background: #f5f7fa;
            }}
            h1 {{ color: #198754; text-align: center; margin-bottom: 10px; }}
            h2 {{ color: #2c3e50; font-size: 1.3em; margin: 30px 0 15px 0; font-weight: 600; }}
            h3 {{ color: #2c3e50; font-size: 1em; margin: 0 0 10px 0; }}

            /* TARJETAS DE RESUMEN */
            .resumen {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 15px;
                margin: 20px 0;
            }}
            .tarjeta {{
                background: white;
                padding: 20px;
                border-radius: 10px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                border-left: 4px solid;
            }}
            .tarjeta.ingresos {{ border-left-color: #28a745; }}
            .tarjeta.gastos {{ border-left-color: #dc3545; }}
            .tarjeta.balance {{ border-left-color: #0d6efd; }}
            .tarjeta.animales {{ border-left-color: #6f42c1; }}
            .tarjeta.suscripcion {{ border-left-color: #fd7e14; }}
            .tarjeta.movimientos {{ border-left-color: #20c997; }}
            .tarjeta h3 {{ color: #6c757d; font-size: 0.75em; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 8px; }}
            .tarjeta .valor {{
                font-size: 1.5em;
                font-weight: bold;
                color: #2c3e50;
                margin: 8px 0;
            }}
            .tarjeta.ingresos .valor {{ color: #28a745; }}
            .tarjeta.gastos .valor {{ color: #dc3545; }}
            .tarjeta.balance .valor {{ color: #0d6efd; }}
            .tarjeta small {{ color: #6c757d; font-size: 0.85em; }}

            /* FILTROS */
            .filtro-fechas {{
                background: white;
                padding: 20px;
                border-radius: 10px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                margin: 20px 0;
            }}
            .filtro-fechas h3 {{
                margin-top: 0;
                color: #2c3e50;
                font-size: 1.1em;
                margin-bottom: 15px;
            }}
            .filtro-form {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                gap: 15px;
                align-items: end;
            }}
            .filtro-form label {{
                display: block;
                font-size: 0.85em;
                margin-bottom: 5px;
                color: #6c757d;
                font-weight: 600;
            }}
            .filtro-form input[type="date"],
            .filtro-form select {{
                padding: 10px;
                border: 2px solid #e9ecef;
                border-radius: 6px;
                font-size: 0.9em;
                width: 100%;
            }}
            .filtro-form input[type="date"]:focus,
            .filtro-form select:focus {{
                outline: none;
                border-color: #198754;
            }}
            .filtro-form button {{
                background: linear-gradient(135deg, #198754 0%, #146c43 100%);
                color: white;
                padding: 10px 20px;
                border: none;
                border-radius: 6px;
                cursor: pointer;
                font-size: 0.9em;
                font-weight: 600;
                transition: transform 0.2s;
            }}
            .filtro-form button:hover {{
                transform: translateY(-2px);
                box-shadow: 0 4px 12px rgba(25, 135, 84, 0.3);
            }}
            .btn-limpiar {{
                padding: 10px 20px;
                color: #6c757d;
                text-decoration: none;
                border: 2px solid #e9ecef;
                border-radius: 6px;
                text-align: center;
                display: block;
                font-weight: 600;
                transition: all 0.2s;
            }}
            .btn-limpiar:hover {{
                background: #f8f9fa;
                color: #198754;
                border-color: #198754;
            }}

            /* GRÁFICOS */
            .graficos-container {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
                gap: 20px;
                margin: 25px 0;
            }}
            .grafico-card {{
                background: white;
                padding: 25px;
                border-radius: 10px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            }}
            .grafico-card h3 {{
                color: #2c3e50;
                font-size: 1.1em;
                margin-bottom: 20px;
                text-align: center;
            }}

            /* ===== TABLAS - INDEPENDIENTES UNA DEBAJO DE OTRA ===== */
            .tabla-container {{
                background: white;
                border-radius: 10px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                margin: 25px 0;
                overflow: hidden;
                width: 100%;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 0;
                background: white;
            }}
            th, td {{
                border: none;
                border-bottom: 1px solid #e9ecef;
                padding: 12px 15px;
                text-align: left;
                font-size: 0.9em;
            }}
            th {{
                background: linear-gradient(135deg, #198754 0%, #146c43 100%);
                color: white;
                font-weight: 600;
                font-size: 0.85em;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }}
            tr:nth-child(even) {{ background-color: #f8f9fa; }}
            tr:nth-child(odd) {{ background-color: white; }}
            tr:hover {{ background-color: #e9f7ef; }}
            tr:last-child td {{ border-bottom: none; }}

            /* SANIDAD - SCROLL HORIZONTAL SOLO SI ES NECESARIO */
            .tabla-sanidad {{
                overflow-x: auto;
                -webkit-overflow-scrolling: touch;
                width: 100%;
            }}
            .tabla-sanidad table {{
                min-width: 1000px;
            }}
            .tabla-sanidad th, 
            .tabla-sanidad td {{
                white-space: nowrap;
                padding: 12px 15px;
            }}

            /* BOTONES */
            .btn-export {{
                display: inline-flex;
                align-items: center;
                gap: 10px;
                background: linear-gradient(135deg, #198754 0%, #146c43 100%);
                color: white;
                padding: 15px 30px;
                text-decoration: none;
                border-radius: 8px;
                font-weight: 600;
                box-shadow: 0 3px 10px rgba(25, 135, 84, 0.3);
                margin: 20px 0;
                transition: transform 0.2s;
            }}
            .btn-export:hover {{
                transform: translateY(-2px);
                box-shadow: 0 5px 15px rgba(25, 135, 84, 0.4);
            }}

            /* LEYENDA Y FILTROS */
            .leyenda-sanidad {{
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
                padding: 15px 20px;
                border-radius: 8px;
                margin-top: 15px;
                font-size: 0.85em;
                color: #495057;
                border-left: 4px solid #198754;
            }}
            .filtros-activos {{
                background: linear-gradient(135deg, #e9f7ef 0%, #d4edda 100%);
                border: 2px solid #28a745;
                padding: 10px 15px;
                border-radius: 8px;
                margin: 15px 0;
                font-size: 0.85em;
                color: #155724;
                font-weight: 600;
            }}
            .filtros-activos strong {{
                color: #198754;
            }}

            /* FOOTER */
            .footer {{
                margin-top: 40px;
                padding: 20px;
                text-align: center;
                font-size: 0.85em;
                color: #6c757d;
                border-top: 2px solid #e9ecef;
                background: white;
                border-radius: 10px;
            }}

            /* RESPONSIVE */
            @media (max-width: 768px) {{
                body {{ padding: 10px; }}
                h1 {{ font-size: 1.5em; }}
                h2 {{ font-size: 1.1em; }}
                .tarjeta .valor {{ font-size: 1.2em; }}
                .resumen {{ grid-template-columns: repeat(2, 1fr); gap: 10px; }}
                .graficos-container {{ grid-template-columns: 1fr; }}
                .filtro-form {{ grid-template-columns: 1fr; }}
                th, td {{ padding: 10px; font-size: 0.85em; }}
                .tabla-sanidad table {{ font-size: 0.8em; min-width: 900px; }}
            }}
            </style>

        </head>
        <body>
            <h1>📊 Dashboard - {nombre_finca}</h1>
            
            <div style="text-align: center;">
                <a href="/finca/{clave}/exportar-excel" class="btn-export">📥 EXPORTAR A EXCEL</a>
            </div>
            
            <!-- FILTROS COMBINADOS -->
            <div class="filtro-fechas">
                <h3 style="margin-top: 0; color: #2c3e50;">🔍 Filtros del Dashboard{periodo_txt}</h3>
                <form method="GET" class="filtro-form">
                    <div>
                        <label>📅 Desde:</label>
                        <input type="date" name="fecha_inicio" value="{fecha_inicio}" min="{hoy.replace(month=1, day=1)}" max="{hoy}">
                    </div>
                    <div>
                        <label>📅 Hasta:</label>
                        <input type="date" name="fecha_fin" value="{fecha_fin}" min="{hoy.replace(month=1, day=1)}" max="{hoy}">
                    </div>
                    <div>
                        <label>🐮 Especie:</label>
                        <select name="especie">
                            <option value="">Todas</option>
                            <option value="bovino" {"selected" if especie_filter == "bovino" else ""}>Bovinos</option>
                            <option value="porcino" {"selected" if especie_filter == "porcino" else ""}>Porcinos</option>
                        </select>
                    </div>
                    <div>
                        <label>🏠 Corral:</label>
                        <select name="corral">
                            <option value="">Todos</option>
                            {"".join(f'<option value="{c}" {"selected" if corral_filter == c else ""}>{c}</option>' for c in corrales_disponibles)}
                        </select>
                    </div>
                    <div>
                        <label>📝 Actividad:</label>
                        <select name="tipo_actividad">
                            <option value="">Todas</option>
                            {"".join(f'<option value="{t}" {"selected" if tipo_actividad_filter == t else ""}>{t.replace("_", " ").title()}</option>' for t in tipos_actividad_disponibles)}
                        </select>
                    </div>
                    <div>
                        <button type="submit">🔍 Filtrar</button>
                    </div>
                    <div>
                        <a href="/finca/{clave}" class="btn-limpiar">🔄 Limpiar</a>
                    </div>
                </form>
                
"""
        if filtros_activos_count > 0:
            html += f'<div class="filtros-activos">📌 Filtros activos: <strong>{filtros_activos_count} filtros aplicados</strong></div>'
        
        html += f"""
            </div>
            
            <!-- TARJETAS FINANCIERAS -->
            <div class="resumen">
                <div class="tarjeta ingresos">
                    <h3>💰 Ingresos</h3>
                    <div class="valor">${ingresos:,.0f}</div>
                    <small style="color: #6c757d;">Periodo seleccionado</small>
                </div>
                <div class="tarjeta gastos">
                    <h3>🔴 Gastos</h3>
                    <div class="valor">${gastos:,.0f}</div>
                    <small style="color: #6c757d;">Periodo seleccionado</small>
                </div>
                <div class="tarjeta balance">
                    <h3>📈 Balance</h3>
                    <div class="valor" style="color: {balance_color};">${balance:,.0f}</div>
                    <small style="color: #6c757d;">{balance_txt}</small>
                </div>
            <!-- TARJETAS KPIs ADICIONALES (FASE 2) -->
            <div class="resumen">
                <div class="tarjeta animales">
                    <h3>🐮 Total Animales</h3>
                    <div class="valor">{total_animales}</div>
                    <small style="color: #6c757d;">Activos en inventario</small>
                </div>
                <div class="tarjeta suscripcion">
                    <h3>📅 Días Suscripción</h3>
                    <div class="valor">{dias_suscripcion}</div>
                    <small style="color: #6c757d;">Días restantes</small>
                </div>
                <div class="tarjeta movimientos">
                <h3>📝 Movimientos</h3>
                <div class="valor">{total_movimientos}</div>
                <small style="color: #6c757d;">En el periodo</small>
            </div>
            </div>
            <!-- GRÁFICOS -->
            <div class="graficos-container">
                <div class="grafico-card">
                    <h3>📊 Ingresos vs Gastos</h3>
                    <canvas id="graficoFinanciero"></canvas>
                </div>
                <div class="grafico-card">
                    <h3>🐮🐷 Distribución de Animales</h3>
                    <canvas id="graficoAnimales"></canvas>
                </div>
            </div>

            <!-- TABLA DE SANIDAD ANIMAL -->
            <h2>💉 Estado de Sanidad Animal</h2>
            <div class="tabla-container tabla-sanidad">
                <table>
                    <thead>
                        <tr>
                            <th>Animal</th>
                            <th>Especie</th>
                            <th>Peso</th>
                            <th>Corral</th>
                            <th>🧬 Última Vacuna</th>
                            <th>🪱 Última Desparasitación</th>
                            <th>🤰 Último Evento Reproductivo</th>
                            <th>Estado</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        
        for marca, especie, peso, corral, estado, vac, desp, rep in sanidad_animales:
            especie_txt = "🐮 Bovino" if especie == "bovino" else "🐷 Porcino" if especie == "porcino" else "🦘 Otro"
            peso_str = f"{peso:.1f} kg" if peso else "—"
            corral_str = corral or "—"
            
            vac_fecha = vac.split(' | ')[0] if vac and ' | ' in vac else vac
            desp_fecha = desp.split(' | ')[0] if desp and ' | ' in desp else desp
            rep_fecha = rep.split(' | ')[0] if rep and ' | ' in rep else rep
            
            vac_icon = calcular_estado_sanidad(vac_fecha)
            desp_icon = calcular_estado_sanidad(desp_fecha)
            rep_icon = calcular_estado_sanidad(rep_fecha, dias_vencimiento=45)
            
            estado_general = "🟢" if estado == "activo" else "🔴"
            
            vac_txt = vac if vac else "—"
            desp_txt = desp if desp else "—"
            rep_txt = rep if rep else "—"
            
            html += f"""
                        <tr>
                            <td><strong>{marca}</strong></td>
                            <td>{especie_txt}</td>
                            <td>{peso_str}</td>
                            <td>{corral_str}</td>
                            <td>{vac_icon} <small style="color: #6c757d;">{vac_txt}</small></td>
                            <td>{desp_icon} <small style="color: #6c757d;">{desp_txt}</small></td>
                            <td>{rep_icon} <small style="color: #6c757d;">{rep_txt}</small></td>
                            <td>{estado_general}</td>
                        </tr>
            """
        
        if not sanidad_animales:
            html += """
                        <tr>
                            <td colspan="8" style="text-align: center; color: #6c757d;">
                                No hay animales registrados con estos filtros
                            </td>
                        </tr>
            """

        html += """
                    </tbody>
                </table>
            </div>
            <div class="leyenda-sanidad">
                <strong>Leyenda:</strong> 
                ✅ Al día (&lt;30 días) • 
                ⚠️ Próximo (30-60 días) • 
                ❌ Vencido (&gt;60 días) • 
                — Sin registro
            </div>

            <!-- INVENTARIO -->
            <h2>📋 Inventario de Animales Activos</h2>
            <div class="tabla-container">
            <table>
            
                <thead><tr><th>Especie</th><th>Marca</th><th>Categoría</th><th>Peso (kg)</th><th>Corral</th></tr></thead>
                <tbody>
        """
        
        if inventario:
            for esp, marca, cat, peso, corral in inventario:
                especie_txt = "Bovino" if esp == "bovino" else "Porcino" if esp == "porcino" else esp.title()
                peso_str = f"{peso:.1f}" if peso else "—"
                cat_str = cat or "—"
                corral_str = corral or "—"
                html += f"<tr><td>{especie_txt}</td><td>{marca}</td><td>{cat_str}</td><td>{peso_str}</td><td>{corral_str}</td></tr>"
        else:
            html += "<tr><td colspan='5'>No hay animales registrados con estos filtros</td></tr>"

        html += """
                </tbody>
            </table>
            </div>

            <!-- MOVIMIENTOS -->
            <h2>📝 Últimos Movimientos</h2>
            <div class="tabla-container">
            <table>
            
                <thead><tr><th>Fecha</th><th>Tipo</th><th>Detalle</th><th>Lugar</th><th>Cant.</th><th>Valor</th><th>Obs.</th></tr></thead>
                <tbody>
        """
        for reg in registros:
            valor_str = f"${reg[5]:,.0f}" if reg[5] and reg[5] > 0 else "—"
            html += f"<tr><td>{reg[0]}</td><td>{reg[1]}</td><td>{reg[2]}</td><td>{reg[3]}</td><td>{reg[4] or ''}</td><td>{valor_str}</td><td>{reg[6] or ''}</td></tr>"

        html += f"""
                </tbody>
            </table>
            </div>

            <!-- SCRIPT GRÁFICOS -->
            <script>
            const datosFinancieros = {{
                ingresos: {ingresos},
                gastos: {gastos},
                balance: {balance}
            }};
            const datosAnimales = {{
                bovinos: {bovinos},
                porcinos: {porcinos},
                otros: {otros}
            }};

            const ctxFin = document.getElementById('graficoFinanciero').getContext('2d');
            new Chart(ctxFin, {{
                type: 'bar',
                data: {{
                    labels: ['Ingresos', 'Gastos', 'Balance'],
                    datasets: [{{
                        label: 'COP',
                        data: [datosFinancieros.ingresos, datosFinancieros.gastos, datosFinancieros.balance],
                        backgroundColor: [
                            'rgba(40, 167, 69, 0.85)',
                            'rgba(220, 53, 69, 0.85)',
                            datosFinancieros.balance >= 0 ? 'rgba(0, 123, 255, 0.85)' : 'rgba(255, 193, 7, 0.85)'
                        ],
                        borderColor: [
                            'rgba(40, 167, 69, 1)',
                            'rgba(220, 53, 69, 1)',
                            datosFinancieros.balance >= 0 ? 'rgba(0, 123, 255, 1)' : 'rgba(255, 193, 7, 1)'
                        ],
                        borderWidth: 2,
                        borderRadius: 8
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            backgroundColor: 'rgba(0,0,0,0.85)',
                            callbacks: {{
                                label: function(ctx) {{
                                    return '$ ' + ctx.parsed.y.toLocaleString('es-CO') + ' COP';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{ beginAtZero: true, grid: {{ color: 'rgba(0,0,0,0.05)' }} }},
                        x: {{ grid: {{ display: false }} }}
                    }}
                }}
            }});

            const ctxAnim = document.getElementById('graficoAnimales').getContext('2d');
            new Chart(ctxAnim, {{
                type: 'doughnut',
                data: {{
                    labels: ['Bovinos', 'Porcinos', 'Otros'],
                    datasets: [{{
                        data: [datosAnimales.bovinos, datosAnimales.porcinos, datosAnimales.otros],
                        backgroundColor: [
                            'rgba(25, 135, 84, 0.9)',
                            'rgba(13, 110, 253, 0.9)',
                            'rgba(255, 193, 7, 0.9)'
                        ],
                        borderColor: '#fff',
                        borderWidth: 3,
                        hoverOffset: 15
                    }}]
                }},
                options: {{
                    responsive: true,
                    cutout: '65%',
                    plugins: {{
                        legend: {{ position: 'bottom', labels: {{ padding: 15, usePointStyle: true }} }}
                    }}
                }}
            }});
            </script>

            <div class="footer">
                🔒 Datos confidenciales. No compartas esta URL.<br>
                💡 Finca Digital © {datetime.date.today().year}
            </div>
        </body>
        </html>
        """
        return html

    except Exception as e:
        print(f"❌ Error dashboard: {e}")
        print(traceback.format_exc())
        return f"❌ Error al cargar el dashboard: {e}", 500

# === RUTA: CONSULTAR MI FINCA_ID ===
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
                    SELECT f.id, f.nombre FROM fincas f
                    JOIN usuarios u ON f.id = u.finca_id
                    WHERE u.telefono_whatsapp = %s
                """, (telefono,))
                row = cur.fetchone()
                if row:
                    return f"📱 Tu finca: <strong>{row[1]}</strong><br>🆔 ID: <strong>{row[0]}</strong>", 200
                else:
                    return "❌ No estás registrado en ninguna finca.", 404
    except Exception as e:
        return f"❌ Error: {e}", 500

# === RUTA: REINICIAR BD ===
@app.route("/reiniciar-bd")
def reiniciar_bd():
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada", 500
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS registros, salud_animal, animales, usuarios, fincas CASCADE")
                conn.commit()
        if bot and hasattr(bot, 'inicializar_bd'):
            if bot.inicializar_bd():
                return "✅ Base de datos reiniciada.", 200
        return "⚠️ Módulo bot no disponible.", 500
    except Exception as e:
        return f"❌ Error: {e}", 500

# === RUTA: EXPORTAR A EXCEL (CON PESTAÑA DE SANIDAD ANIMAL) ===
@app.route("/finca/<clave>/exportar-excel")
def exportar_finca_excel(clave):
    try:
        import pandas as pd
        from io import BytesIO
        
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada", 500

        fecha_inicio_str = request.args.get("fecha_inicio")
        fecha_fin_str = request.args.get("fecha_fin")
        hoy = datetime.date.today()
        
        if fecha_inicio_str and fecha_fin_str:
            try:
                fecha_inicio = datetime.datetime.strptime(fecha_inicio_str, "%Y-%m-%d").date()
                fecha_fin = datetime.datetime.strptime(fecha_fin_str, "%Y-%m-%d").date()
            except:
                fecha_inicio = hoy.replace(day=1)
                fecha_fin = hoy
        else:
            fecha_inicio = hoy.replace(day=1)
            fecha_fin = hoy

        with psycopg2.connect(database_url) as conn:
            cur = conn.cursor()
            cur.execute("SELECT nombre, id FROM fincas WHERE clave_secreta = %s", (clave,))
            finca_row = cur.fetchone()
            if not finca_row:
                return "❌ Acceso denegado.", 403
            nombre_finca, finca_id = finca_row

            # === 1. INVENTARIO DE ANIMALES (sin filtro de fecha) ===
            df_animales = pd.read_sql_query("""
                SELECT especie, marca_o_arete AS marca, categoria, peso, corral, estado, fecha_registro
                FROM animales WHERE finca_id = %s
                ORDER BY especie, marca_o_arete
            """, conn, params=(finca_id,))
            df_animales['especie'] = df_animales['especie'].apply(
                lambda x: 'Bovino' if x == 'bovino' else 'Porcino' if x == 'porcino' else x.title()
            )

            # === 2. MOVIMIENTOS (CON filtro de fechas) ===
            df_registros = pd.read_sql_query("""
                SELECT fecha, tipo_actividad AS tipo, detalle, lugar, cantidad, valor, observacion, jornales
                FROM registros WHERE finca_id = %s AND fecha BETWEEN %s AND %s
                ORDER BY fecha DESC LIMIT 500
            """, conn, params=(finca_id, fecha_inicio.isoformat(), fecha_fin.isoformat()))

            # === 3. SANIDAD ANIMAL (NUEVO - CON filtro de fechas) ===
            df_sanidad = pd.read_sql_query("""
                SELECT 
                    sa.fecha,
                    sa.tipo,
                    sa.tratamiento,
                    a.marca_o_arete AS animal,
                    a.especie,
                    sa.observacion
                FROM salud_animal sa
                LEFT JOIN animales a ON sa.id_externo = a.id_externo
                WHERE sa.finca_id = %s AND sa.fecha BETWEEN %s AND %s
                ORDER BY sa.fecha DESC
            """, conn, params=(finca_id, fecha_inicio.isoformat(), fecha_fin.isoformat()))

            # === 4. FINANZAS (CON filtro de fechas) ===
            cur.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN tipo_actividad IN ('produccion', 'salida_animal') THEN valor ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN tipo_actividad = 'gasto' THEN valor ELSE 0 END) + 
                    SUM(CASE WHEN jornales > 0 THEN valor ELSE 0 END), 0)
                FROM registros WHERE finca_id = %s AND fecha BETWEEN %s AND %s
            """, (finca_id, fecha_inicio.isoformat(), fecha_fin.isoformat()))
            finanzas = cur.fetchone()
            cur.close()

        # === CREAR ARCHIVO EXCEL ===
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            
            # 📊 Hoja 1: Resumen Financiero
            df_resumen = pd.DataFrame([{
                'Finca': nombre_finca,
                'Periodo': f"{fecha_inicio.strftime('%d/%m')} al {fecha_fin.strftime('%d/%m')}",
                'Fecha exportación': hoy.strftime('%d/%m/%Y'),
                'Ingresos': f"${finanzas[0]:,.0f} COP",
                'Gastos': f"${finanzas[1]:,.0f} COP",
                'Balance': f"${finanzas[0]-finanzas[1]:,.0f} COP"
            }])
            df_resumen.to_excel(writer, sheet_name='📊 Resumen', index=False)
            
            # 🐮🐷 Hoja 2: Inventario de Animales
            if not df_animales.empty:
                df_animales.to_excel(writer, sheet_name='🐮🐷 Inventario', index=False)
            
            # 📝 Hoja 3: Movimientos/Registros
            if not df_registros.empty:
                df_registros.to_excel(writer, sheet_name='📝 Movimientos', index=False)
            
            # 💉 Hoja 4: Sanidad Animal (NUEVO)
            if not df_sanidad.empty:
                df_sanidad.to_excel(writer, sheet_name='💉 Sanidad Animal', index=False)

        output.seek(0)
        filename = f"Finca_{nombre_finca.replace(' ','_')}_{hoy.strftime('%Y%m%d')}.xlsx"
        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        as_attachment=True, download_name=filename)

    except ImportError:
        return "❌ Librerías Excel no instaladas.", 500
    except Exception as e:
        print(f"❌ Error exportar Excel: {e}")
        print(traceback.format_exc())
        return f"❌ Error: {e}", 500

# ============================================================================
# === RUTA: FORMULARIO WEB PROFESIONAL PARA INGRESO MANUAL DE DATOS ===
# ============================================================================
@app.route("/finca/<clave>/ingreso-manual")
def ingreso_manual_datos(clave):
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada", 500

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT nombre, id FROM fincas WHERE clave_secreta = %s", (clave,))
                finca_row = cur.fetchone()
                if not finca_row:
                    return "❌ Acceso denegado. URL inválida.", 403
                nombre_finca, finca_id = finca_row

        html = f"""
        <!DOCTYPE html>
        <html lang="es">
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>Ingreso Manual - {nombre_finca} | Finca Digital</title>
            <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
            <style>
                * {{ box-sizing: border-box; margin: 0; padding: 0; }}
                body {{
                    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    padding: 40px 20px;
                }}
                .container {{ max-width: 800px; margin: 0 auto; }}
                .header {{ text-align: center; margin-bottom: 40px; color: white; }}
                .header h1 {{ font-size: 2.5em; font-weight: 700; margin-bottom: 10px; }}
                .form-card {{
                    background: white;
                    border-radius: 20px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                    overflow: hidden;
                }}
                .form-header {{
                    background: linear-gradient(135deg, #198754 0%, #146c43 100%);
                    color: white;
                    padding: 30px;
                    text-align: center;
                }}
                .form-body {{ padding: 40px; }}
                .form-group {{ margin-bottom: 25px; }}
                .form-group label {{
                    display: block;
                    font-weight: 600;
                    color: #2c3e50;
                    margin-bottom: 8px;
                }}
                .form-group input,
                .form-group select,
                .form-group textarea {{
                    width: 100%;
                    padding: 14px 18px;
                    border: 2px solid #e9ecef;
                    border-radius: 12px;
                    font-size: 1em;
                }}
                .form-group textarea {{ resize: vertical; min-height: 100px; }}
                .form-row {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 20px;
                }}
                .btn-submit {{
                    background: linear-gradient(135deg, #198754 0%, #146c43 100%);
                    color: white;
                    border: none;
                    padding: 18px 40px;
                    font-size: 1.1em;
                    font-weight: 600;
                    border-radius: 12px;
                    cursor: pointer;
                    width: 100%;
                }}
                .btn-back {{
                    display: inline-flex;
                    align-items: center;
                    gap: 8px;
                    color: #6c757d;
                    text-decoration: none;
                    margin-top: 25px;
                    padding: 12px 20px;
                    border: 2px solid #e9ecef;
                    border-radius: 10px;
                }}
                .info-box {{
                    background: linear-gradient(135deg, #e9f7ef 0%, #d4edda 100%);
                    border-left: 4px solid #28a745;
                    padding: 20px;
                    border-radius: 10px;
                    margin-bottom: 30px;
                }}
                .info-box ul {{ color: #155724; margin-left: 20px; }}
                @media (max-width: 768px) {{
                    .form-row {{ grid-template-columns: 1fr; }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🌱 Finca Digital</h1>
                    <p>Registro manual de actividades - {nombre_finca}</p>
                </div>
                <div class="form-card">
                    <div class="form-header">
                        <h2>📝 Nueva Actividad</h2>
                    </div>
                    <div class="form-body">
                        <div class="info-box">
                            <h4>💡 Consejos:</h4>
                            <ul>
                                <li>Para <strong>animales</strong>, incluye marcas: <code>marca LG01 peso 450 kg</code></li>
                                <li>Para <strong>gastos</strong>, especifica el concepto claramente</li>
                            </ul>
                        </div>
                        <form method="POST" action="/finca/{clave}/guardar-manual" id="registroForm">
                            <div class="form-group">
                                <label>📋 Tipo de Actividad *</label>
                                <select name="tipo" required>
                                    <option value="">Selecciona...</option>
                                    <option value="siembra">🌱 Siembra</option>
                                    <option value="produccion">🌾 Producción</option>
                                    <option value="sanidad_animal">💉 Sanidad Animal</option>
                                    <option value="ingreso_animal">🐷 Ingreso Animales</option>
                                    <option value="salida_animal">🐄 Salida Animales</option>
                                    <option value="gasto">💰 Gasto</option>
                                    <option value="labor">🛠️ Labor</option>
                                </select>
                            </div>
                            <div class="form-group">
                                <label>📦 Detalle *</label>
                                <input type="text" name="detalle" required>
                            </div>
                            <div class="form-row">
                                <div class="form-group">
                                    <label>🔢 Cantidad</label>
                                    <input type="number" name="cantidad" step="0.1" min="0">
                                </div>
                                <div class="form-group">
                                    <label>💰 Valor (COP)</label>
                                    <input type="number" name="valor" value="0" min="0">
                                </div>
                            </div>
                            <div class="form-group">
                                <label>📍 Lugar</label>
                                <input type="text" name="lugar">
                            </div>
                            <div class="form-group">
                                <label>📝 Observación</label>
                                <textarea name="observacion" placeholder="marca LG01 peso 450 kg"></textarea>
                            </div>
                            <div class="form-group">
                                <label>👷 Jornales</label>
                                <input type="number" name="jornales" value="0" min="0">
                            </div>
                            <button type="submit" class="btn-submit">✅ Guardar Registro</button>
                        </form>
                        <div style="text-align: center;">
                            <a href="/finca/{clave}" class="btn-back">← Volver al Dashboard</a>
                        </div>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        return html

    except Exception as e:
        print(f"❌ Error formulario manual: {e}")
        return f"❌ Error: {e}", 500

# ============================================================================
# === RUTA: PROCESAR Y GUARDAR DATOS DEL FORMULARIO MANUAL ===
# ============================================================================
@app.route("/finca/<clave>/guardar-manual", methods=["POST"])
def guardar_manual_datos(clave):
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada", 500

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT nombre, id FROM fincas WHERE clave_secreta = %s", (clave,))
                finca_row = cur.fetchone()
                if not finca_row:
                    return "❌ Acceso denegado.", 403
                nombre_finca, finca_id = finca_row

                cur.execute("SELECT id FROM usuarios WHERE finca_id = %s AND rol = 'dueño' LIMIT 1", (finca_id,))
                usuario_row = cur.fetchone()
                usuario_id = usuario_row[0] if usuario_row else None

        tipo = request.form.get("tipo", "")
        detalle = request.form.get("detalle", "").strip()
        cantidad = request.form.get("cantidad")
        valor = request.form.get("valor", 0)
        lugar = request.form.get("lugar", "").strip()
        observacion = request.form.get("observacion", "").strip()
        jornales = request.form.get("jornales", 0)

        try:
            cantidad = float(cantidad) if cantidad else None
            valor = float(valor) if valor else 0
            jornales = int(float(jornales)) if jornales else 0
        except:
            pass

        if not tipo or not detalle:
            return "❌ Tipo y detalle son obligatorios", 400

        if bot and hasattr(bot, 'guardar_registro'):
            mensaje_completo = f"{detalle} {lugar} {observacion}".strip()
            
            bot.guardar_registro(
                tipo_actividad=tipo,
                accion=tipo,
                detalle=detalle,
                lugar=lugar,
                cantidad=cantidad,
                valor=valor,
                unidad="manual_web",
                observacion=observacion,
                jornales=jornales,
                finca_id=finca_id,
                usuario_id=usuario_id,
                mensaje_completo=mensaje_completo
            )

            # === PROCESAR INGRESO ANIMAL ===
            animales_registrados = 0
            if tipo == "ingreso_animal" and observacion:
                marcas = re.findall(r"marca\s+([a-z0-9-]+)", observacion, re.IGNORECASE)
                
                for marca in marcas:
                    marca_upper = marca.upper()
                    peso_valor = None
                    pattern = r"marca\s+" + re.escape(marca) + r".*?peso\s*(\d+(?:\.\d+)?)\s*kg"
                    peso_match = re.search(pattern, observacion, re.IGNORECASE)
                    if peso_match:
                        peso_valor = float(peso_match.group(1))

                    especie = "bovino"
                    if any(p in detalle.lower() for p in ["cerdo", "lechón", "cerda", "chancho", "porcino"]):
                        especie = "porcino"

                    categoria = None
                    if "ternera" in detalle.lower(): categoria = "ternera"
                    elif "ternero" in detalle.lower(): categoria = "ternero"
                    elif "vaca" in detalle.lower(): categoria = "vaca"
                    elif "toro" in detalle.lower(): categoria = "toro"
                    elif "lechón" in detalle.lower(): categoria = "lechón"
                    elif "cerda" in detalle.lower(): categoria = "cerda"

                    with psycopg2.connect(database_url) as conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO animales (especie, id_externo, marca_o_arete, categoria, corral, estado, peso, finca_id)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (id_externo) DO UPDATE
                                SET peso = EXCLUDED.peso, estado = EXCLUDED.estado, categoria = EXCLUDED.categoria
                            """, (
                                especie,
                                f"V-M-{marca_upper}" if especie == "bovino" else f"C-{marca_upper}",
                                marca_upper,
                                categoria,
                                lugar,
                                "activo",
                                peso_valor,
                                finca_id
                            ))
                            conn.commit()
                            animales_registrados += 1

            # === PROCESAR SALIDA ANIMAL (COMPATIBLE RENDER GRATIS) ===
            animales_vendidos = 0
            if tipo == "salida_animal" and observacion:
                marcas = re.findall(r"marca\s+([a-z0-9-]+)", observacion, re.IGNORECASE)
                marcas = [m.upper() for m in marcas]
                
                if marcas:
                    for marca in marcas:
                        try:
                            with psycopg2.connect(database_url) as conn:
                                with conn.cursor() as cur:
                                    cur.execute("""
                                        SELECT id_externo FROM animales
                                        WHERE (marca_o_arete = %s OR id_externo LIKE %s)
                                        AND finca_id = %s
                                        AND estado = 'activo'
                                    """, (marca, f"%{marca}%", finca_id))
                                    row = cur.fetchone()
                                    
                                    if row:
                                        id_externo = row[0]
                                        cur.execute("""
                                            UPDATE animales 
                                            SET estado = 'vendido',
                                                observaciones = %s
                                            WHERE id_externo = %s
                                        """, (f"Vendido: {detalle} - {observacion}", id_externo))
                                        conn.commit()
                                        animales_vendidos += 1
                                        print(f"✅ Animal {marca} marcado como vendido")
                                    else:
                                        print(f"⚠️ Animal {marca} no encontrado o ya está vendido")
                        except Exception as e:
                            print(f"❌ Error al actualizar salida para {marca}: {e}")

            # === PROCESAR SANIDAD ANIMAL ===
            if tipo == "sanidad_animal" and observacion:
                detalle_lower = detalle.lower()
                if any(kw in detalle_lower for kw in ["vacuna", "aftosa", "brucelosis"]):
                    tipo_sanidad = "vacuna"
                elif any(kw in detalle_lower for kw in ["desparasit", "garrapata", "gusano"]):
                    tipo_sanidad = "desparasitación"
                elif any(kw in detalle_lower for kw in ["monta", "insemin", "preñez", "celo", "reproduccion", "reproducción"]):
                    tipo_sanidad = "reproducción"
                else:
                    tipo_sanidad = "sanidad"
                
                marcas = re.findall(r"marca\s+([a-z0-9-]+)", observacion, re.IGNORECASE)
                marcas = [m.upper() for m in marcas]
                
                if marcas:
                    for marca in marcas:
                        try:
                            with psycopg2.connect(database_url) as conn:
                                with conn.cursor() as cur:
                                    cur.execute("""
                                        SELECT id_externo FROM animales
                                        WHERE (marca_o_arete = %s OR id_externo LIKE %s)
                                        AND finca_id = %s
                                    """, (marca, f"%{marca}%", finca_id))
                                    row = cur.fetchone()
                                    
                                    if row:
                                        id_externo = row[0]
                                        cur.execute("""
                                            INSERT INTO salud_animal (id_externo, tipo, tratamiento, fecha, observacion, finca_id)
                                            VALUES (%s, %s, %s, %s, %s, %s)
                                        """, (
                                            id_externo,
                                            tipo_sanidad,
                                            detalle,
                                            datetime.date.today().isoformat(),
                                            observacion,
                                            finca_id
                                        ))
                                        conn.commit()
                                        print(f"✅ Sanidad guardada para {marca}: {tipo_sanidad}")
                                    else:
                                        print(f"⚠️ Animal {marca} no encontrado")
                        except Exception as e:
                            print(f"❌ Error al guardar sanidad para {marca}: {e}")

            # === PÁGINA DE ÉXITO ===
            html = f"""
            <!DOCTYPE html>
            <html lang="es">
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <title>✅ Registro Exitoso</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        min-height: 100vh;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        padding: 20px;
                    }}
                    .success-card {{
                        background: white;
                        border-radius: 20px;
                        padding: 50px;
                        text-align: center;
                        max-width: 500px;
                    }}
                    .success-icon {{
                        font-size: 4em;
                        color: #28a745;
                        margin-bottom: 20px;
                    }}
                    h1 {{ color: #28a745; }}
                    .info {{
                        background: #f8f9fa;
                        border-radius: 12px;
                        padding: 20px;
                        margin: 25px 0;
                        text-align: left;
                    }}
                    .info-row {{
                        display: flex;
                        justify-content: space-between;
                        padding: 10px 0;
                        border-bottom: 1px solid #e9ecef;
                    }}
                    .btn {{
                        padding: 14px 28px;
                        border-radius: 10px;
                        text-decoration: none;
                        margin: 10px;
                        display: inline-block;
                    }}
                    .btn-primary {{ background: #198754; color: white; }}
                    .btn-secondary {{ background: white; color: #198754; border: 2px solid #198754; }}
                </style>
            </head>
            <body>
                <div class="success-card">
                    <div class="success-icon">✓</div>
                    <h1>¡Registro Exitoso!</h1>
                    <p>Los datos han sido guardados en <strong>{nombre_finca}</strong></p>
                    <div class="info">
                        <div class="info-row">
                            <span>📋 Tipo</span>
                            <span>{tipo.replace('_', ' ').title()}</span>
                        </div>
                        <div class="info-row">
                            <span>📦 Detalle</span>
                            <span>{detalle}</span>
                        </div>
                        <div class="info-row">
                            <span>💰 Valor</span>
                            <span>${valor:,.0f} COP</span>
                        </div>
                        {f'<div class="info-row"><span>🐮 Animales</span><span>{animales_registrados} registrados</span></div>' if animales_registrados > 0 else ''}
                        {f'<div class="info-row"><span>💸 Vendidos</span><span>{animales_vendidos} actualizados</span></div>' if animales_vendidos > 0 else ''}
                    </div>
                    <div>
                        <a href="/finca/{clave}/ingreso-manual" class="btn btn-secondary">📝 Registrar Otro</a>
                        <a href="/finca/{clave}" class="btn btn-primary">📊 Dashboard</a>
                    </div>
                </div>
            </body>
            </html>
            """
            return html

        else:
            return "❌ Módulo bot no disponible", 500

    except Exception as e:
        print(f"❌ Error guardar manual: {e}")
        print(traceback.format_exc())
        return f"❌ Error: {e}", 500

# === INICIO DEL SERVIDOR ===
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🌍 Servidor iniciando en http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)