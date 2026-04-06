# -* coding: utf-8 -*--
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

# === AGREGAR DESPUÉS DE LOS IMPORTS ===
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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

# === RUTA: DASHBOARD POR FINCA (CORREGIDO - TABLAS INDEPENDIENTES) ===
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
                
                                # === MODO EDICIÓN? ===
                edit_id = request.args.get("edit_id")
                datos_editar = None
                titulo_form = "📝 Nueva Actividad"
                accion_form = f"/finca/{clave}/guardar-manual"
                texto_boton = "✅ Guardar Registro"

                if edit_id:
                    try:
                        cur.execute("SELECT * FROM registros WHERE id = %s AND finca_id = %s", (edit_id, finca_id))
                        datos_editar = cur.fetchone()
                        if datos_editar:
                            titulo_form = "✏️ Editar Registro"
                            accion_form = f"/finca/{clave}/actualizar-manual/{edit_id}"
                            texto_boton = "🔄 Actualizar Registro"
                    except Exception as e:
                        logger.error(f"Error cargando edición: {e}")
                
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
                    SELECT id, fecha, tipo_actividad, detalle, lugar, cantidad, valor, observacion
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
                
                # === FINANZAS ===
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
                
                # === KPIs ADICIONALES ===
                cur.execute("""
                    SELECT COUNT(*) FROM animales
                    WHERE finca_id = %s AND estado = 'activo'
                """, (finca_id,))
                total_animales = cur.fetchone()[0]
                
                cur.execute("""
                    SELECT vencimiento_suscripcion FROM fincas
                    WHERE id = %s
                """, (finca_id,))
                vencimiento = cur.fetchone()[0]
                dias_suscripcion = (vencimiento - hoy).days if vencimiento else 0
                
                cur.execute("""
                    SELECT COUNT(*) FROM registros
                    WHERE finca_id = %s AND fecha BETWEEN %s AND %s
                """, (finca_id, fecha_inicio.isoformat(), fecha_fin.isoformat()))
                total_movimientos = cur.fetchone()[0]
                
                # === CONTAR FILTROS ACTIVOS ===
                filtros_activos_count = sum(1 for f in [especie_filter, corral_filter, tipo_actividad_filter] if f)
                
                # === TEXTO Y COLOR PARA EL BALANCE ===
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
        h2 {{ color: #2c3e50; font-size: 1.4em; margin: 30px 0 20px 0; font-weight: 600; border-bottom: 3px solid #198754; padding-bottom: 10px; }}
        h3 {{ color: #2c3e50; font-size: 1em; margin: 0 0 10px 0; }}
        
        /* BOTÓN EXPORTAR */
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
            margin: 20px auto;
            transition: transform 0.2s;
            text-align: center;
        }}
        .btn-export:hover {{
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(25, 135, 84, 0.4);
        }}
        /* BOTÓN INGRESO MANUAL */
        .btn-manual {{
            display: inline-flex;
            align-items: center;
            gap: 10px;
            background: linear-gradient(135deg, #0d6efd 0%, #0a58ca 100%);
            color: white;
            padding: 15px 30px;
            text-decoration: none;
            border-radius: 8px;
            font-weight: 600;
            box-shadow: 0 3px 10px rgba(13, 110, 253, 0.3);
            margin: 20px 10px;
            transition: transform 0.2s;
            text-align: center;
        }}
        .btn-manual:hover {{
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(13, 110, 253, 0.4);
        }}
        
        /* FILTROS */
        .filtro-fechas {{
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            margin: 20px 0;
        }}
        .filtro-fechas h3 {{
            margin-top: 0;
            color: #2c3e50;
            font-size: 1.2em;
            margin-bottom: 20px;
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
        
        /* TARJETAS DE RESUMEN */
        .resumen {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 25px 0;
        }}
        .tarjeta {{
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            border-left: 5px solid;
        }}
        .tarjeta.ingresos {{ border-left-color: #28a745; }}
        .tarjeta.gastos {{ border-left-color: #dc3545; }}
        .tarjeta.balance {{ border-left-color: #0d6efd; }}
        .tarjeta.animales {{ border-left-color: #6f42c1; }}
        .tarjeta.suscripcion {{ border-left-color: #fd7e14; }}
        .tarjeta.movimientos {{ border-left-color: #20c997; }}
        .tarjeta h3 {{ color: #6c757d; font-size: 0.8em; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 10px; }}
        .tarjeta .valor {{
            font-size: 2em;
            font-weight: bold;
            color: #2c3e50;
            margin: 10px 0;
        }}
        .tarjeta.ingresos .valor {{ color: #28a745; }}
        .tarjeta.gastos .valor {{ color: #dc3545; }}
        .tarjeta.balance .valor {{ color: #0d6efd; }}
        .tarjeta small {{ color: #6c757d; font-size: 0.9em; }}
        
        /* GRÁFICOS */
        .graficos-container {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 25px;
            margin: 30px 0;
        }}
        .grafico-card {{
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }}
        .grafico-card h3 {{
            color: #2c3e50;
            font-size: 1.2em;
            margin-bottom: 20px;
            text-align: center;
        }}
        
        /* ===== TABLAS - INDEPENDIENTES UNA DEBAJO DE OTRA ===== */
        .tabla-section {{
            background: white;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            margin: 30px 0;
            overflow: hidden;
            width: 100%;
        }}
        .tabla-wrapper {{
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
            width: 100%;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 0;
            background: white;
            min-width: 800px;
        }}
        th, td {{
            border: none;
            border-bottom: 1px solid #e9ecef;
            padding: 14px 18px;
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
            position: sticky;
            top: 0;
        }}
        tr:nth-child(even) {{ background-color: #f8f9fa; }}
        tr:nth-child(odd) {{ background-color: white; }}
        tr:hover {{ background-color: #e9f7ef; }}
        tr:last-child td {{ border-bottom: none; }}
        
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
        
        /* FOOTER */
        .footer {{
            margin-top: 50px;
            padding: 25px;
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
            h1 {{ font-size: 1.6em; }}
            h2 {{ font-size: 1.2em; }}
            .tarjeta .valor {{ font-size: 1.5em; }}
            .resumen {{ grid-template-columns: repeat(2, 1fr); gap: 12px; }}
            .graficos-container {{ grid-template-columns: 1fr; }}
            .filtro-form {{ grid-template-columns: 1fr; }}
            th, td {{ padding: 12px 14px; font-size: 0.85em; }}
            table {{ min-width: 700px; font-size: 0.8em; }}
        }}
    </style>
</head>
<body>
    <h1>📊 Dashboard - {nombre_finca}</h1>
    
    <div style="text-align: center;">
        <a href="/finca/{clave}/ingreso-manual" class="btn-manual">📝 INGRESO MOVIMIENTOS FINCA</a>
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
    </div>
    
    <!-- TARJETAS KPIs ADICIONALES -->
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
    
    <!-- TABLA DE SANIDAD ANIMAL - FULL WIDTH -->
    <h2>💉 Estado de Sanidad Animal</h2>
    <div class="tabla-section">
        <div class="tabla-wrapper">
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
                        <td colspan="8" style="text-align: center; color: #6c757d; padding: 30px;">
                            No hay animales registrados con estos filtros
                        </td>
                    </tr>
"""
                html += """
                </tbody>
            </table>
        </div>
    </div>
    <div class="leyenda-sanidad">
        <strong>Leyenda:</strong>
        ✅ Al día (&lt;90 días) •
        ⚠️ Próximo (90-120 días) •
        ❌ Vencido (&gt;120 días) •
        — Sin registro
    </div>
    
    <!-- INVENTARIO - FULL WIDTH -->
    <h2>📋 Inventario de Animales Activos</h2>
    <div class="tabla-section">
        <div class="tabla-wrapper">
            <table>
                <thead>
                    <tr>
                        <th>Especie</th>
                        <th>Marca</th>
                        <th>Categoría</th>
                        <th>Peso (kg)</th>
                        <th>Corral</th>
                    </tr>
                </thead>
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
                    html += "<tr><td colspan='5' style='text-align: center; color: #6c757d; padding: 30px;'>No hay animales registrados con estos filtros</td></tr>"
                
                html += """
                </tbody>
            </table>
        </div>
    </div>
    
    <!-- MOVIMIENTOS - FULL WIDTH -->
    <h2>📝 Últimos Movimientos</h2>
    <div class="tabla-section">
        <div class="tabla-wrapper">
            <table>
                <thead>
                    <tr>
                        <th>Fecha</th>
                        <th>Tipo</th>
                        <th>Detalle</th>
                        <th>Lugar</th>
                        <th>Cant.</th>
                        <th>Valor</th>
                        <th>Obs.</th>
                    </tr>
                </thead>
                <tbody>
"""
                for reg in registros:
                    # AHORA: reg[0]=id, reg[1]=fecha, reg[2]=tipo, reg[3]=detalle, reg[4]=lugar, reg[5]=cantidad, reg[6]=valor, reg[7]=observacion
                    id_registro = reg[0]
                    valor_str = f"${reg[6]:,.0f}" if reg[6] and reg[6] > 0 else "—"
                    html += f"<tr><td>{reg[1]}</td><td>{reg[2]}</td><td>{reg[3]}</td><td>{reg[4] or ''}</td><td>{reg[5] or ''}</td><td>{valor_str}</td><td>{reg[7] or ''}</td><td><a href='/finca/{clave}/ingreso-manual?edit_id={id_registro}' style='color:#007bff; text-decoration:none;'>✏️ Editar</a></td></tr>"
                
                if not registros:
                    html += "<tr><td colspan='8' style='text-align: center; color: #6c757d; padding: 30px;'>No hay movimientos en este periodo</td></tr>"
                
                html += f"""
                </tbody>
            </table>
        </div>
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

# === RUTA: RENOVAR SUSCRIPCIÓN (SOLO ADMIN) ===
@app.route("/admin/renovar/<telefono>")
def admin_renovar_suscripcion(telefono):
    """Renueva suscripción vía URL. Solo para uso del administrador."""
    # Aquí podrías agregar autenticación básica si lo deseas
    telefono_formateado = f"whatsapp:+57{telefono}" if not telefono.startswith("whatsapp:") else telefono
    if bot and hasattr(bot, 'renovar_suscripcion'):
        resultado = bot.renovar_suscripcion(telefono_formateado, dias_extension=30)
        return f"<pre>{resultado}</pre>"
    return "❌ Módulo bot no disponible"

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
# === RUTA: FORMULARIO WEB PROFESIONAL PARA INGRESO MANUAL DE DATOS (MEJORADO) ===
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
                 
                # === 🆕 MODO EDICIÓN? (AGREGAR ESTO) ===
                edit_id = request.args.get("edit_id")
                datos_editar = None
                titulo_form = "📝 Nueva Actividad"
                accion_form = f"/finca/{clave}/guardar-manual"
                texto_boton = "✅ Guardar Registro"

                if edit_id:
                    try:
                        cur.execute("SELECT * FROM registros WHERE id = %s AND finca_id = %s", (edit_id, finca_id))
                        datos_editar = cur.fetchone()
                        if datos_editar:
                            titulo_form = "✏️ Editar Registro"
                            accion_form = f"/finca/{clave}/actualizar-manual/{edit_id}"
                            texto_boton = "🔄 Actualizar Registro"
                    except Exception as e:
                        logger.error(f"Error cargando edición: {e}")
                
                # === OBTENER LUGARES FRECUENTES PARA AUTO-SUGERENCIA ===
                cur.execute("""
                    SELECT DISTINCT lugar FROM registros
                    WHERE finca_id = %s AND lugar IS NOT NULL
                    ORDER BY lugar
                    LIMIT 10
                """, (finca_id,))
                lugares_frecuentes = [row[0] for row in cur.fetchall()]
                
                # === GENERAR HTML DE SUGERENCIAS (fuera del f-string principal) ===
                if lugares_frecuentes:
                    sugerencias_html = "".join(
                    f'<span class="sugerencia-tag" onclick="copiarSugerencia(this)">{lugar}</span>'
                    for lugar in lugares_frecuentes
                    )
                    sugerencias_container = f'<div class="sugerencias-container">{sugerencias_html}</div>'
                else:
                    sugerencias_container = ''
                
                # === OBTENER ANIMALES ACTIVOS PARA SELECCIÓN RÁPIDA ===
                cur.execute("""
                    SELECT marca_o_arete, especie FROM animales
                    WHERE finca_id = %s AND estado = 'activo'
                    ORDER BY especie, marca_o_arete
                    LIMIT 50
                """, (finca_id,))
                animales_activos = cur.fetchall()
                
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
        .container {{ max-width: 900px; margin: 0 auto; }}
        
        /* HEADER */
        .header {{ 
            text-align: center; 
            margin-bottom: 30px; 
            color: white; 
        }}
        .header h1 {{ 
            font-size: 2.2em; 
            font-weight: 700; 
            margin-bottom: 8px; 
            text-shadow: 0 2px 4px rgba(0,0,0,0.2);
        }}
        .header p {{ 
            font-size: 1.1em; 
            opacity: 0.95; 
        }}
        
        /* BREADCRUMBS */
        .breadcrumbs {{
            background: rgba(255,255,255,0.15);
            padding: 12px 20px;
            border-radius: 10px;
            margin-bottom: 25px;
            backdrop-filter: blur(10px);
        }}
        .breadcrumbs a {{
            color: white;
            text-decoration: none;
            font-weight: 500;
            transition: opacity 0.2s;
        }}
        .breadcrumbs a:hover {{ opacity: 0.8; }}
        .breadcrumbs span {{ color: rgba(255,255,255,0.7); }}
        
        /* FORM CARD */
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
        .form-header h2 {{ 
            font-size: 1.8em; 
            font-weight: 600; 
        }}
        .form-header p {{ 
            opacity: 0.9; 
            margin-top: 8px; 
        }}
        .form-body {{ padding: 40px; }}
        
        /* FORM GROUPS */
        .form-group {{ margin-bottom: 25px; }}
        .form-group label {{
            display: block;
            font-weight: 600;
            color: #2c3e50;
            margin-bottom: 8px;
            font-size: 0.95em;
        }}
        .form-group label .required {{
            color: #dc3545;
            margin-left: 3px;
        }}
        .form-group input,
        .form-group select,
        .form-group textarea {{
            width: 100%;
            padding: 14px 18px;
            border: 2px solid #e9ecef;
            border-radius: 12px;
            font-size: 1em;
            transition: all 0.2s;
            font-family: inherit;
        }}
        .form-group input:focus,
        .form-group select:focus,
        .form-group textarea:focus {{
            outline: none;
            border-color: #198754;
            box-shadow: 0 0 0 3px rgba(25, 135, 84, 0.15);
        }}
        .form-group textarea {{ 
            resize: vertical; 
            min-height: 100px; 
        }}
        .form-group small {{
            display: block;
            color: #6c757d;
            font-size: 0.85em;
            margin-top: 6px;
        }}
        
        /* FORM ROW */
        .form-row {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
        }}
        
        /* CAMPOS DINÁMICOS */
        .campo-dinamico {{
            display: none;
            background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
            padding: 20px;
            border-radius: 12px;
            margin-top: 15px;
            border-left: 4px solid #198754;
        }}
        .campo-dinamico.activo {{
            display: block;
            animation: slideDown 0.3s ease;
        }}
        @keyframes slideDown {{
            from {{ opacity: 0; transform: translateY(-10px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}
        
        /* BOTONES */
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
            transition: all 0.2s;
            box-shadow: 0 4px 15px rgba(25, 135, 84, 0.3);
        }}
        .btn-submit:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(25, 135, 84, 0.4);
        }}
        .btn-submit:active {{
            transform: translateY(0);
        }}
        
        /* BOTÓN VOLVER */
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
            transition: all 0.2s;
            font-weight: 500;
        }}
        .btn-back:hover {{
            background: #f8f9fa;
            color: #198754;
            border-color: #198754;
        }}
        
        /* INFO BOX */
        .info-box {{
            background: linear-gradient(135deg, #e9f7ef 0%, #d4edda 100%);
            border-left: 4px solid #28a745;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .info-box h4 {{ 
            color: #155724; 
            margin-bottom: 12px; 
            font-size: 1em;
        }}
        .info-box ul {{ 
            color: #155724; 
            margin-left: 20px; 
            font-size: 0.9em;
        }}
        .info-box li {{ margin-bottom: 6px; }}
        .info-box code {{
            background: rgba(0,0,0,0.08);
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 0.9em;
        }}
        
        /* SUGERENCIAS */
        .sugerencias-container {{
            margin-top: 8px;
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
        }}
        .sugerencia-tag {{
            background: #e9ecef;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            cursor: pointer;
            transition: all 0.2s;
            color: #495057;
        }}
        .sugerencia-tag:hover {{
            background: #198754;
            color: white;
        }}
        
        /* VALIDACIÓN */
        .form-group.error input,
        .form-group.error select,
        .form-group.error textarea {{
            border-color: #dc3545;
        }}
        .error-message {{
            color: #dc3545;
            font-size: 0.85em;
            margin-top: 6px;
            display: none;
        }}
        .form-group.error .error-message {{
            display: block;
        }}
        
        /* RESPONSIVE */
        @media (max-width: 768px) {{
            body {{ padding: 20px 10px; }}
            .form-body {{ padding: 25px; }}
            .form-row {{ grid-template-columns: 1fr; }}
            .header h1 {{ font-size: 1.8em; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- BREADCRUMBS -->
        <div class="breadcrumbs">
            <a href="/finca/{clave}">📊 Dashboard</a>
            <span> / </span>
            <a href="/finca/{clave}/ingreso-manual">📝 Ingreso Manual</a>
        </div>
        
        <div class="header">
            <h1>🌱 Finca Digital</h1>
            <p>Registro manual de actividades - <strong>{nombre_finca}</strong></p>
        </div>
        
        <div class="form-card">
            <div class="form-header">
                <h2>📝 Nueva Actividad</h2>
                <p>Completa el formulario para registrar un movimiento</p>
            </div>
            
            <div class="form-body">
                <!-- INFO BOX -->
                <div class="info-box">
                    <h4>💡 Consejos para un registro efectivo:</h4>
                    <ul>
                        <li>Para <strong>animales</strong>, usa el formato: <code>marca LG01 peso 450 kg</code></li>
                        <li>Para <strong>gastos</strong>, especifica el concepto claramente (ej: "Compra de concentrado")</li>
                        <li>Para <strong>sanidad</strong>, incluye el tipo de tratamiento y animal afectado</li>
                        <li>Los campos marcados con <span class="required">*</span> son obligatorios</li>
                    </ul>
                </div>
                
                <!-- FORMULARIO -->
                <form method="POST" action="/finca/{clave}/guardar-manual" id="registroForm" novalidate>
                    
                    <!-- TIPO DE ACTIVIDAD -->
                    <div class="form-group" id="group-tipo">
                        <label>📋 Tipo de Actividad <span class="required">*</span></label>
                        <select name="tipo" id="tipo" required onchange="mostrarCamposDinamicos()">
                            <option value="">Selecciona una opción...</option>
                            <option value="siembra">🌱 Siembra</option>
                            <option value="produccion">🌾 Producción / Cosecha</option>
                            <option value="sanidad_animal">💉 Sanidad Animal</option>
                            <option value="ingreso_animal">🐷 Ingreso de Animales</option>
                            <option value="salida_animal">🐄 Salida / Venta de Animales</option>
                            <option value="gasto">💰 Gasto / Compra</option>
                            <option value="labor">🛠️ Labor / Jornal</option>
                        </select>
                        <small class="error-message">Por favor selecciona un tipo de actividad</small>
                    </div>
                    
                    <!-- CAMPOS DINÁMICOS PARA ANIMALES -->
                    <div id="campos-animales" class="campo-dinamico">
                        <h4 style="margin-bottom: 15px; color: #2c3e50;">🐮 Información de Animales</h4>
                        
                        <div class="form-group">
                            <label>🏷️ Seleccionar Animal (opcional)</label>
                            <select name="animal_seleccion" id="animal_seleccion">
                                <option value="">-- Buscar animal registrado --</option>
                                {"".join(f'<option value="{marca}">{marca} - {especie.title()}</option>' for marca, especie in animales_activos)}
                            </select>
                            <small>Si seleccionas un animal, se completará automáticamente en la observación</small>
                        </div>
                        
                        <div class="form-row">
                            <div class="form-group">
                                <label>⚖️ Peso Promedio (kg)</label>
                                <input type="number" name="peso_promedio" step="0.1" min="0" placeholder="Ej: 450">
                            </div>
                            <div class="form-group">
                                <label>📊 Cantidad de Animales</label>
                                <input type="number" name="cantidad_animales" min="1" value="1">
                            </div>
                        </div>
                    </div>
                    
                    <!-- CAMPOS DINÁMICOS PARA SANIDAD -->
                    <div id="campos-sanidad" class="campo-dinamico">
                        <h4 style="margin-bottom: 15px; color: #2c3e50;">💉 Detalles de Sanidad</h4>
                        
                        <div class="form-group">
                            <label>🧪 Tipo de Tratamiento</label>
                            <select name="tipo_sanidad" id="tipo_sanidad">
                                <option value="">Selecciona...</option>
                                <option value="vacuna">💉 Vacunación</option>
                                <option value="desparasitacion">🪱 Desparasitación</option>
                                <option value="reproduccion">🤰 Reproducción</option>
                                <option value="tratamiento">🏥 Tratamiento Médico</option>
                            </select>
                        </div>
                    </div>
                    
                    <!-- DETALLE -->
                    <div class="form-group" id="group-detalle">
                        <label>📦 Detalle de la Actividad <span class="required">*</span></label>
                        <input type="text" name="detalle" id="detalle" required 
                                placeholder="Ej: Compra de concentrado, Vacunación aftosa, Venta de 2 novillos..."
                                value="{datos_editar[4] if datos_editar else ''}">
                        <small class="error-message">El detalle es obligatorio</small>
                    </div>
                    
                    <!-- FILA: CANTIDAD Y VALOR -->
                    <div class="form-row">
                        <div class="form-group" id="group-cantidad">
                            <label>🔢 Cantidad</label>
                            <input type="number" name="cantidad" id="cantidad" step="0.1" min="0" 
                                    placeholder="Ej: 10"
                                    value="{datos_editar[6] if datos_editar else ''}">
                            <small>Unidades, kilogramos, litros, etc.</small>
                        </div>
                        <div class="form-group" id="group-valor">
                            <label>💰 Valor Total (COP)</label>
                            <input type="text" name="valor" id="valor" 
                            value="{int(datos_editar[7]) if datos_editar and datos_editar[7] else '0'}" 
                            pattern="[0-9]*" 
                            oninput="this.value = this.value.replace(/[^0-9]/g, '')" 
                            placeholder="Ej: 500000">
                            <small style="color: #6c757d; font-size: 0.85em; margin-top: 6px;"> Solo números. Ej: 700000 para setecientos mil </small>
                        </div>
                    </div>
                    
                    <!-- LUGAR -->
                    <div class="form-group" id="group-lugar">
                        <label>📍 Lugar / Corral</label>
                        <input type="text" name="lugar" id="lugar" 
                                placeholder="Ej: Corral 1, Potrero Norte, Bodega..."
                                value="{datos_editar[5] if datos_editar else ''}">
                        {sugerencias_container}
                    </div>
                    
                    <!-- OBSERVACIÓN -->
                    <div class="form-group" id="group-observacion">
                        <label>📝 Observación</label>
                        <textarea name="observacion" id="observacion" 
                                    placeholder="Ej: marca LG01 peso 450 kg, marca LG02 peso 480 kg...">{datos_editar[9] if datos_editar else ''}</textarea>
                        <small>Para animales: usa el formato <code>marca XXX peso YYY kg</code></small>
                    </div>
                    

                    <!-- JORNALES -->
                    <div class="form-group" id="group-jornales">
                        <label>👷 Número de Jornales</label>
                        <input type="number" name="jornales" id="jornales" 
                                value="{datos_editar[10] if datos_editar and datos_editar[10] else '0'}" 
                                min="0" 
                                placeholder="Ej: 2">
                        <small>Si la actividad involucra pago por jornales</small>
                    </div>
                    
                    <!-- BOTÓN SUBMIT -->
                    <button type="submit" class="btn-submit" id="btnSubmit">
                        ✅ Guardar Registro
                    </button>
                </form>
                
                <!-- BOTÓN VOLVER -->
                <div style="text-align: center;">
                    <a href="/finca/{clave}" class="btn-back">← Volver al Dashboard</a>
                </div>
            </div>
        </div>
    </div>
    
    <!-- SCRIPTS -->
    <script>
        // === MOSTRAR CAMPOS DINÁMICOS SEGÚN TIPO ===
        function mostrarCamposDinamicos() {{
            const tipo = document.getElementById('tipo').value;
            const camposAnimales = document.getElementById('campos-animales');
            const camposSanidad = document.getElementById('campos-sanidad');
            
            // Ocultar todos
            camposAnimales.classList.remove('activo');
            camposSanidad.classList.remove('activo');
            
            // Mostrar según tipo
            if (['ingreso_animal', 'salida_animal'].includes(tipo)) {{
                camposAnimales.classList.add('activo');
            }}
            if (tipo === 'sanidad_animal') {{
                camposAnimales.classList.add('activo');
                camposSanidad.classList.add('activo');
            }}
        }}
        
        // === COPIAR SUGERENCIA AL INPUT ===
        function copiarSugerencia(element) {{
            document.getElementById('lugar').value = element.textContent;
            document.getElementById('lugar').focus();
        }}
        
        // === SELECCIONAR ANIMAL AUTOMÁTICAMENTE ===
        document.getElementById('animal_seleccion')?.addEventListener('change', function() {{
            const marca = this.value;
            if (marca) {{
                const observacion = document.getElementById('observacion');
                const current = observacion.value.trim();
                observacion.value = current ? current + ', marca ' + marca : 'marca ' + marca;
            }}
        }});
        
        // === VALIDACIÓN DEL FORMULARIO ===
        document.getElementById('registroForm').addEventListener('submit', function(e) {{
            let valido = true;
            
            // Limpiar errores previos
            document.querySelectorAll('.form-group').forEach(g => g.classList.remove('error'));
            
            // Validar tipo
            const tipo = document.getElementById('tipo');
            if (!tipo.value) {{
                document.getElementById('group-tipo').classList.add('error');
                valido = false;
            }}
            
            // Validar detalle
            const detalle = document.getElementById('detalle');
            if (!detalle.value.trim()) {{
                document.getElementById('group-detalle').classList.add('error');
                valido = false;
            }}
            
            // Si no es válido, prevenir envío
            if (!valido) {{
                e.preventDefault();
                document.getElementById('btnSubmit').textContent = '⚠️ Completa los campos obligatorios';
                setTimeout(() => {{
                    document.getElementById('btnSubmit').textContent = '✅ Guardar Registro';
                }}, 3000);
            }}
        }});
        
        // === FORMATEAR VALOR AL ESCRIBIR ===
        document.getElementById('valor')?.addEventListener('input', function(e) {{
            let val = this.value.replace(/[^0-9]/g, '');
            if (val) {{
                this.value = parseInt(val).toLocaleString('es-CO');
            }}
        }});
    </script>
</body>
</html>
"""
                return html
    except Exception as e:
        print(f"❌ Error formulario manual: {e}")
        return f"❌ Error: {e}", 500

# ============================================================================
# === RUTA: PROCESAR Y GUARDAR DATOS DEL FORMULARIO MANUAL (VERSIÓN MEJORADA) ===
# ============================================================================
@app.route("/finca/<clave>/guardar-manual", methods=["POST"])
def guardar_manual_datos(clave):
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            logger.error("❌ DATABASE_URL no configurada")
            return "❌ DATABASE_URL no configurada", 500
        
        # === 1. VALIDAR FINCA Y USUARIO ===
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT nombre, id FROM fincas WHERE clave_secreta = %s", (clave,))
                finca_row = cur.fetchone()
                if not finca_row:
                    logger.warning(f"⚠️ Acceso denegado para clave: {clave}")
                    return "❌ Acceso denegado.", 403
                nombre_finca, finca_id = finca_row
                
                cur.execute("SELECT id FROM usuarios WHERE finca_id = %s AND rol = 'dueño' LIMIT 1", (finca_id,))
                usuario_row = cur.fetchone()
                usuario_id = usuario_row[0] if usuario_row else None

        # === 2. OBTENER Y PROCESAR DATOS DEL FORMULARIO ===
        tipo = request.form.get("tipo", "")
        detalle = request.form.get("detalle", "").strip()
        cantidad = request.form.get("cantidad")
        valor = request.form.get("valor", 0)
        lugar = request.form.get("lugar", "").strip()
        observacion = request.form.get("observacion", "").strip()
        jornales = request.form.get("jornales", 0)

        # === 3. VALIDACIONES DE SEGURIDAD (MEJORA #1) ===
        if not tipo or not detalle:
            return "❌ Tipo y detalle son obligatorios", 400
        
        if len(detalle) < 3:
            return "❌ El detalle debe tener al menos 3 caracteres", 400
        
        # Procesar valores numéricos
        try:
            cantidad = float(cantidad) if cantidad else None
        except (ValueError, TypeError):
            cantidad = None
            
        try:
            if valor:
                valor_str = str(valor).replace('.', '').replace(',', '')
                valor = float(valor_str) if valor_str else 0
            else:
                valor = 0
        except (ValueError, TypeError):
            valor = 0
            
        try:
            jornales = int(float(jornales)) if jornales else 0
        except (ValueError, TypeError):
            jornales = 0
        
        # Validar que no sean negativos
        if valor < 0:
            return "❌ El valor no puede ser negativo", 400
        if cantidad and cantidad < 0:
            return "❌ La cantidad no puede ser negativa", 400
        if jornales < 0:
            return "❌ Los jornales no pueden ser negativos", 400

        # === 4. TRANSACCIÓN ÚNICA PARA TODAS LAS OPERACIONES (MEJORA #2) ===
        animales_registrados = 0
        animales_vendidos = 0
        
        with psycopg2.connect(database_url) as conn:  # UNA SOLA CONEXIÓN
            with conn.cursor() as cur:
                # 4.1 Guardar Registro Principal
                fecha_hoy = datetime.date.today().isoformat()
                fecha_registro = datetime.datetime.now().isoformat()
                mensaje_completo = f"{detalle} {lugar} {observacion}".strip()
                
                cur.execute('''
                    INSERT INTO registros 
                    (fecha, tipo_actividad, accion, detalle, lugar, cantidad, valor, unidad, observacion, jornales, fecha_registro, finca_id, usuario_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    fecha_hoy, tipo, tipo, detalle, lugar, cantidad, valor, "manual_web", observacion, jornales, fecha_registro, finca_id, usuario_id
                ))
                logger.info(f"✅ Registro principal guardado: {tipo} - {detalle}")
                
                # 4.2 Procesar Ingreso de Animales (si aplica)
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
                        animales_registrados += 1
                        logger.info(f"🐮 Animal registrado: {marca_upper}")
                
                # 4.3 Procesar Salida/Venta de Animales (si aplica)
                if tipo == "salida_animal" and observacion:
                    marcas = re.findall(r"marca\s+([a-z0-9-]+)", observacion, re.IGNORECASE)
                    marcas = [m.upper() for m in marcas]
                    for marca in marcas:
                        try:
                            cur.execute("""
                                SELECT id_externo FROM animales
                                WHERE (marca_o_arete = %s OR id_externo LIKE %s)
                                AND finca_id = %s AND estado = 'activo'
                            """, (marca, f"%{marca}%", finca_id))
                            row = cur.fetchone()
                            if row:
                                id_externo = row[0]
                                cur.execute("""
                                    UPDATE animales SET estado = 'vendido', observaciones = %s WHERE id_externo = %s
                                """, (f"Vendido: {detalle} - {observacion}", id_externo))
                                animales_vendidos += 1
                                logger.info(f"💸 Animal vendido: {marca}")
                        except Exception as e:
                            logger.warning(f"⚠️ Error venta {marca}: {e}")
                
                # 4.4 Procesar Sanidad Animal (si aplica)
                if tipo == "sanidad_animal" and observacion:
                    detalle_lower = detalle.lower()
                    tipo_sanidad = "sanidad"
                    if any(kw in detalle_lower for kw in ["vacuna", "vacunacion", "vacunación", "aftosa", "carbon", "carbón", "brucelosis","peste"]):
                        tipo_sanidad = "vacuna"
                    elif any(kw in detalle_lower for kw in ["desparasit", "lavado"," lombriz", "purga", " purgante", "nuche", "vitamin" "garrapata", "gusano"]):
                        tipo_sanidad = "desparasitación"
                    elif any(kw in detalle_lower for kw in ["monta", "insemin", "preñez", "celo", "reproduccion", "reproducción", "inseminacion", "servicio"]):
                        tipo_sanidad = "reproducción"
                    
                    marcas = re.findall(r"marca\s+([a-z0-9-]+)", observacion, re.IGNORECASE)
                    marcas = [m.upper() for m in marcas]
                    
                    for marca in marcas:
                        try:
                            cur.execute("SELECT id_externo FROM animales WHERE (marca_o_arete = %s OR id_externo LIKE %s) AND finca_id = %s", (marca, f"%{marca}%", finca_id))
                            row = cur.fetchone()
                            if row:
                                id_externo = row[0]
                                cur.execute("""
                                    INSERT INTO salud_animal (id_externo, tipo, tratamiento, fecha, observacion, finca_id)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                """, (id_externo, tipo_sanidad, detalle, fecha_hoy, observacion, finca_id))
                                logger.info(f"💉 Sanidad guardada: {marca} - {tipo_sanidad}")
                        except Exception as e:
                            logger.warning(f"⚠️ Error sanidad {marca}: {e}")
                
                # 4.5 COMMIT ÚNICO AL FINAL (TODO O NADA)
                conn.commit()
                logger.info(f"✅ Transacción completada: {animales_registrados} animales, {animales_vendidos} vendidos")

        # === 5. GENERAR PÁGINA DE ÉXITO ===
        html = f"""
        <!DOCTYPE html>
        <html lang="es">
        <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>✅ Registro Exitoso - {nombre_finca}</title>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
        <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: 'Inter', sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 20px; }}
        .success-card {{ background: white; border-radius: 20px; padding: 40px; max-width: 600px; width: 100%; box-shadow: 0 20px 60px rgba(0,0,0,0.3); }}
        .success-icon {{ font-size: 4em; color: #28a745; margin-bottom: 15px; text-align: center; }}
        h1 {{ color: #28a745; font-size: 1.8em; margin-bottom: 10px; font-weight: 700; text-align: center; }}
        .info-box {{ background: #f8f9fa; border-radius: 12px; padding: 25px; margin: 25px 0; }}
        .info-row {{ display: flex; justify-content: space-between; padding: 12px 0; border-bottom: 1px solid #dee2e6; }}
        .info-row:last-child {{ border-bottom: none; }}
        .acciones {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin: 25px 0; }}
        .btn {{ padding: 14px 20px; border-radius: 10px; text-decoration: none; text-align: center; font-weight: 600; display: flex; align-items: center; justify-content: center; gap: 8px; }}
        .btn-primary {{ background: #198754; color: white; }}
        .btn-secondary {{ background: white; color: #198754; border: 2px solid #198754; }}
        </style>
        </head>
        <body>
        <div class="success-card">
            <div class="success-icon">✅</div>
            <h1>¡Registro Exitoso!</h1>
            <p style="text-align:center; color:#6c757d;">Guardado en <strong>{nombre_finca}</strong></p>
            <div class="info-box">
                <div class="info-row"><span>📋 Tipo</span><span>{tipo.replace('_', ' ').title()}</span></div>
                <div class="info-row"><span>📦 Detalle</span><span>{detalle}</span></div>
                <div class="info-row"><span>💰 Valor</span><span>${int(valor):,} COP</span></div>
                <div class="info-row"><span>📍 Lugar</span><span>{lugar if lugar else '—'}</span></div>
                <div class="info-row"><span>📅 Fecha</span><span>{datetime.date.today().strftime('%d/%m/%Y')}</span></div>
                {f'<div class="info-row"><span>🐮 Animales</span><span>{animales_registrados} registrados</span></div>' if animales_registrados > 0 else ''}
                {f'<div class="info-row"><span>💸 Vendidos</span><span>{animales_vendidos} actualizados</span></div>' if animales_vendidos > 0 else ''}
            </div>
            <div class="acciones">
                <a href="/finca/{clave}/ingreso-manual" class="btn btn-secondary">📝 Otro Registro</a>
                <a href="/finca/{clave}" class="btn btn-primary">📊 Dashboard</a>
            </div>
        </div>
        </body>
        </html>
        """
        return html

    except Exception as e:
        logger.error(f"❌ Error guardar manual: {e}")
        logger.error(traceback.format_exc())
        return f"❌ Error: {e}", 500
# ============================================================================
# === RUTA: PROCESAR EDICIÓN DE DATOS (UPDATE - NO INSERT) ===
# ============================================================================
@app.route("/finca/<clave>/actualizar-manual/<int:registro_id>", methods=["POST"])
def actualizar_manual_datos(clave, registro_id):
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            logger.error("❌ DATABASE_URL no configurada")
            return "❌ DATABASE_URL no configurada", 500
        
        # === 1. VALIDAR FINCA (SEGURIDAD) ===
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT nombre, id FROM fincas WHERE clave_secreta = %s", (clave,))
                finca_row = cur.fetchone()
                if not finca_row:
                    logger.warning(f"⚠️ Acceso denegado para clave: {clave}")
                    return "❌ Acceso denegado.", 403
                nombre_finca, finca_id = finca_row
                
                # === 2. VERIFICAR QUE EL REGISTRO PERTENECE A ESTA FINCA ===
                cur.execute("SELECT id FROM registros WHERE id = %s AND finca_id = %s", (registro_id, finca_id))
                if not cur.fetchone():
                    logger.warning(f"⚠️ Intento de editar registro {registro_id} de otra finca")
                    return "❌ Registro no encontrado o no pertenece a esta finca.", 404

        # === 3. OBTENER DATOS DEL FORMULARIO ===
        tipo = request.form.get("tipo", "")
        detalle = request.form.get("detalle", "").strip()
        cantidad = request.form.get("cantidad")
        valor = request.form.get("valor", 0)
        lugar = request.form.get("lugar", "").strip()
        observacion = request.form.get("observacion", "").strip()
        jornales = request.form.get("jornales", 0)

        # === 4. VALIDACIONES ===
        if not tipo or not detalle:
            return "❌ Tipo y detalle son obligatorios", 400
        
        if len(detalle) < 3:
            return "❌ El detalle debe tener al menos 3 caracteres", 400
        
        # Procesar valores numéricos
        try:
            cantidad = float(cantidad) if cantidad else None
        except (ValueError, TypeError):
            cantidad = None
            
        try:
            if valor:
                valor_str = str(valor).replace('.', '').replace(',', '')
                valor = float(valor_str) if valor_str else 0
            else:
                valor = 0
        except (ValueError, TypeError):
            valor = 0
            
        try:
            jornales = int(float(jornales)) if jornales else 0
        except (ValueError, TypeError):
            jornales = 0
        
        # Validar que no sean negativos
        if valor < 0:
            return "❌ El valor no puede ser negativo", 400
        if cantidad and cantidad < 0:
            return "❌ La cantidad no puede ser negativa", 400
        if jornales < 0:
            return "❌ Los jornales no pueden ser negativos", 400

        # === 5. ACTUALIZAR REGISTRO (UPDATE - NO INSERT) ===
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE registros 
                    SET tipo_actividad = %s, accion = %s, detalle = %s, lugar = %s, 
                        cantidad = %s, valor = %s, observacion = %s, jornales = %s
                    WHERE id = %s AND finca_id = %s
                """, (tipo, tipo, detalle, lugar, cantidad, valor, observacion, jornales, registro_id, finca_id))
                
                conn.commit()
                
                if cur.rowcount == 0:
                    logger.warning(f"⚠️ No se actualizó ningún registro para ID {registro_id}")
                    return "❌ No se pudo actualizar el registro.", 404

        # === 6. GENERAR PÁGINA DE ÉXITO ===
        html = f"""
        <html><head><meta http-equiv="refresh" content="2;url=/finca/{clave}" /></head>
        <body style="font-family:sans-serif; text-align:center; padding:50px;">
            <h1 style="color:green;">✅ Registro Actualizado</h1>
            <p>Registro #{registro_id} en {nombre_finca}</p>
            <p>Redirigiendo al dashboard...</p>
            <a href="/finca/{clave}">Volver ahora</a>
        </body></html>
        """
        return html

    except Exception as e:
        logger.error(f"❌ Error actualizar manual: {e}")
        logger.error(traceback.format_exc())
        return f"❌ Error: {e}", 500

# === INICIO DEL SERVIDOR ===
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🌍 Servidor iniciando en http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)