# -*- coding: utf-8 -*-
"""
app.py - Webhook para WhatsApp + Twilio + Gestión completa de fincas y empleados
Versión: Dashboard con filtro de fechas, gráficos 2D y exportación a Excel
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
        url_dashboard = f"https://finca-bot.onrender.com/finca/{clave_secreta}"
        return (
            f"✅ Finca '{nombre_finca}' activada con éxito.<br>"
            f"• Dueño: {dueno_formateado}<br>"
            f"• Empleados ({len(lista_empleados)}): {empleados_txt}<br>"
            f"• Válida hasta: {datetime.date.today() + datetime.timedelta(days=30)}<br><br>"
            f"🔐 <strong>Dashboard privado:</strong> <a href='{url_dashboard}' target='_blank'>{url_dashboard}</a>"
        ), 200

    except Exception as e:
        return f"❌ Error al activar: {e}", 500

# === RUTA: DASHBOARD POR FINCA (COMPLETO CON FILTRO DE FECHAS) ===
@app.route("/finca/<clave>")
def dashboard_finca(clave):
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada", 500

        hoy = datetime.date.today()
        
        # === OBTENER FILTRO DE FECHAS ===
        fecha_inicio_str = request.args.get("fecha_inicio")
        fecha_fin_str = request.args.get("fecha_fin")

        if fecha_inicio_str and fecha_fin_str:
            try:
                fecha_inicio = datetime.datetime.strptime(fecha_inicio_str, "%Y-%m-%d").date()
                fecha_fin = datetime.datetime.strptime(fecha_fin_str, "%Y-%m-%d").date()
                # Validar que sean del año actual
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
            # Comportamiento por defecto: mes actual
            periodo_txt = " (mes actual)"
            fecha_inicio = hoy.replace(day=1)
            fecha_fin = hoy

        # Usar parámetros seguros para SQL (prevenir inyección)
        filtro_fecha_params = (fecha_inicio.isoformat(), fecha_fin.isoformat())

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT nombre, id FROM fincas WHERE clave_secreta = %s", (clave,))
                finca_row = cur.fetchone()
                if not finca_row:
                    return "❌ Acceso denegado. URL inválida.", 403
                nombre_finca, finca_id = finca_row

                # INVENTARIO (sin filtro de fecha - son animales activos)
                cur.execute("""
                    SELECT especie, marca_o_arete, categoria, peso, corral
                    FROM animales
                    WHERE finca_id = %s AND estado = 'activo'
                    ORDER BY especie, marca_o_arete
                """, (finca_id,))
                inventario = cur.fetchall()

                # CONTAR ANIMALES POR ESPECIE
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

                # MOVIMIENTOS CON FILTRO DE FECHAS
                cur.execute("""
                    SELECT fecha, tipo_actividad, detalle, lugar, cantidad, valor, observacion
                    FROM registros
                    WHERE finca_id = %s AND fecha BETWEEN %s AND %s
                    ORDER BY fecha DESC, id DESC
                    LIMIT 500
                """, (finca_id,) + filtro_fecha_params)
                registros = cur.fetchall()

                # FINANZAS CON FILTRO DE FECHAS
                cur.execute("""
                    SELECT 
                        SUM(CASE WHEN tipo_actividad = 'produccion' THEN valor ELSE 0 END),
                        SUM(CASE WHEN tipo_actividad = 'gasto' THEN valor ELSE 0 END) +
                        SUM(CASE WHEN jornales > 0 THEN valor ELSE 0 END)
                    FROM registros
                    WHERE finca_id = %s AND fecha BETWEEN %s AND %s
                """, (finca_id,) + filtro_fecha_params)
                finanzas = cur.fetchone()
                ingresos = finanzas[0] or 0
                gastos = finanzas[1] or 0
                balance = ingresos - gastos

        # GENERAR HTML
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>{nombre_finca} - Finca Digital</title>
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
            <style>
                * {{ box-sizing: border-box; }}
                body {{ 
                    font-family: 'Segoe UI', Arial, sans-serif; 
                    max-width: 1200px; 
                    margin: 0 auto; 
                    padding: 20px; 
                    background: #f5f7fa;
                }}
                h1 {{ color: #198754; text-align: center; margin-bottom: 10px; }}
                h2 {{ color: #198754; font-size: 1.3em; margin-top: 25px; }}
                h3 {{ color: #2c3e50; font-size: 1.1em; margin: 0 0 15px 0; }}
                .resumen {{ 
                    display: grid; 
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); 
                    gap: 20px; 
                    margin: 25px 0; 
                }}
                .tarjeta {{ 
                    background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
                    padding: 25px; 
                    border-radius: 12px; 
                    box-shadow: 0 4px 15px rgba(0,0,0,0.08);
                    border-left: 5px solid;
                    transition: transform 0.2s ease;
                }}
                .tarjeta:hover {{ transform: translateY(-3px); }}
                .tarjeta.ingresos {{ border-left-color: #28a745; }}
                .tarjeta.gastos {{ border-left-color: #dc3545; }}
                .tarjeta.balance {{ border-left-color: #0d6efd; }}
                .tarjeta h3 {{ color: #6c757d; font-size: 0.9em; text-transform: uppercase; letter-spacing: 1px; }}
                .tarjeta .valor {{ 
                    font-size: 2em; 
                    font-weight: bold; 
                    color: #2c3e50; 
                    margin: 10px 0;
                }}
                .tarjeta.ingresos .valor {{ color: #28a745; }}
                .tarjeta.gastos .valor {{ color: #dc3545; }}
                .tarjeta.balance .valor {{ color: #0d6efd; }}
                .filtro-fechas {{
                    background: white;
                    padding: 20px;
                    border-radius: 12px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.08);
                    margin: 20px 0;
                }}
                .filtro-form {{
                    display: flex;
                    gap: 15px;
                    flex-wrap: wrap;
                    align-items: end;
                }}
                .filtro-form label {{
                    display: block;
                    font-size: 0.9em;
                    margin-bottom: 5px;
                    color: #6c757d;
                }}
                .filtro-form input[type="date"] {{
                    padding: 10px;
                    border: 1px solid #ddd;
                    border-radius: 6px;
                    font-size: 1em;
                }}
                .filtro-form button {{
                    background: #198754;
                    color: white;
                    padding: 10px 20px;
                    border: none;
                    border-radius: 6px;
                    cursor: pointer;
                    font-size: 1em;
                }}
                .filtro-form button:hover {{ background: #146c43; }}
                .btn-limpiar {{
                    padding: 10px 20px;
                    color: #6c757d;
                    text-decoration: none;
                    border: 1px solid #ddd;
                    border-radius: 6px;
                }}
                .graficos-container {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
                    gap: 25px;
                    margin: 30px 0;
                }}
                .grafico-card {{
                    background: white;
                    padding: 25px;
                    border-radius: 12px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.08);
                }}
                table {{ 
                    width: 100%; 
                    border-collapse: collapse; 
                    margin: 20px 0;
                    background: white;
                    border-radius: 8px;
                    overflow: hidden;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
                }}
                th, td {{ 
                    border: 1px solid #e9ecef; 
                    padding: 12px 15px; 
                    text-align: left; 
                }}
                th {{ 
                    background: linear-gradient(135deg, #198754 0%, #146c43 100%);
                    color: white;
                    font-weight: 600;
                }}
                tr:nth-child(even) {{ background-color: #f8f9fa; }}
                tr:hover {{ background-color: #e9f7ef; }}
                .btn-export {{
                    display: inline-flex;
                    align-items: center;
                    gap: 8px;
                    background: linear-gradient(135deg, #198754 0%, #146c43 100%);
                    color: white;
                    padding: 14px 28px;
                    text-decoration: none;
                    border-radius: 8px;
                    font-weight: 600;
                    box-shadow: 0 4px 12px rgba(25, 135, 84, 0.3);
                    transition: all 0.3s ease;
                    margin: 20px 0;
                }}
                .btn-export:hover {{
                    transform: translateY(-2px);
                    box-shadow: 0 6px 20px rgba(25, 135, 84, 0.4);
                }}
                .footer {{ 
                    margin-top: 40px; 
                    padding: 20px;
                    text-align: center;
                    font-size: 0.9em; 
                    color: #6c757d;
                    border-top: 1px solid #e9ecef;
                }}
                @media (max-width: 768px) {{
                    .tarjeta .valor {{ font-size: 1.5em; }}
                    .graficos-container {{ grid-template-columns: 1fr; }}
                    .filtro-form {{ flex-direction: column; align-items: stretch; }}
                    th, td {{ padding: 10px; font-size: 0.9em; }}
                }}
            </style>
        </head>
        <body>
            <h1>📊 Dashboard - {nombre_finca}</h1>
            
            <div style="text-align: center;">
                <a href="/finca/{clave}/exportar-excel" class="btn-export">📥 EXPORTAR A EXCEL</a>
            </div>
            
            <!-- FILTRO DE FECHAS -->
            <div class="filtro-fechas">
                <h3 style="margin-top: 0; color: #2c3e50;">📅 Filtrar por fechas{periodo_txt}</h3>
                <form method="GET" class="filtro-form">
                    <div>
                        <label>Desde:</label>
                        <input type="date" name="fecha_inicio" value="{fecha_inicio}" min="{hoy.replace(month=1, day=1)}" max="{hoy}">
                    </div>
                    <div>
                        <label>Hasta:</label>
                        <input type="date" name="fecha_fin" value="{fecha_fin}" min="{hoy.replace(month=1, day=1)}" max="{hoy}">
                    </div>
                    <button type="submit">🔍 Filtrar</button>
                    <a href="/finca/{clave}" class="btn-limpiar">🔄 Limpiar</a>
                </form>
            </div>
            
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
                    <div class="valor">${balance:,.0f}</div>
                    <small style="color: #6c757d;">{'Positivo' if balance >= 0 else 'Negativo'}</small>
                </div>
            </div>

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

            <h2>📋 Inventario de Animales Activos</h2>
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
            html += "<tr><td colspan='5'>No hay animales registrados</td></tr>"

        html += """
                </tbody>
            </table>

            <h2>📝 Últimos Movimientos</h2>
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

            // Gráfico Financiero
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

            // Gráfico Animales
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

# === RUTA: EXPORTAR A EXCEL ===
@app.route("/finca/<clave>/exportar-excel")
def exportar_finca_excel(clave):
    try:
        import pandas as pd
        from io import BytesIO
        
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ DATABASE_URL no configurada", 500

        # Obtener filtro de fechas (mismo que dashboard)
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

            # Inventario (sin filtro de fecha)
            df_animales = pd.read_sql_query("""
                SELECT especie, marca_o_arete AS marca, categoria, peso, corral
                FROM animales WHERE finca_id = %s AND estado = 'activo'
                ORDER BY especie, marca_o_arete
            """, conn, params=(finca_id,))
            df_animales['especie'] = df_animales['especie'].apply(
                lambda x: 'Bovino' if x == 'bovino' else 'Porcino' if x == 'porcino' else x.title()
            )

            # Movimientos (CON filtro de fechas)
            df_registros = pd.read_sql_query("""
                SELECT fecha, tipo_actividad AS tipo, detalle, lugar, cantidad, valor, observacion
                FROM registros WHERE finca_id = %s AND fecha BETWEEN %s AND %s
                ORDER BY fecha DESC LIMIT 500
            """, conn, params=(finca_id, fecha_inicio.isoformat(), fecha_fin.isoformat()))

            # Finanzas (CON filtro de fechas)
            cur.execute("""
                SELECT 
                    COALESCE(SUM(CASE WHEN tipo_actividad = 'produccion' THEN valor ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN tipo_actividad = 'gasto' THEN valor ELSE 0 END) + 
                    SUM(CASE WHEN jornales > 0 THEN valor ELSE 0 END), 0)
                FROM registros WHERE finca_id = %s AND fecha BETWEEN %s AND %s
            """, (finca_id, fecha_inicio.isoformat(), fecha_fin.isoformat()))
            finanzas = cur.fetchone()
            cur.close()

        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_resumen = pd.DataFrame([{
                'Finca': nombre_finca,
                'Periodo': f"{fecha_inicio.strftime('%d/%m')} al {fecha_fin.strftime('%d/%m')}",
                'Fecha exportación': hoy.strftime('%d/%m/%Y'),
                'Ingresos': f"${finanzas[0]:,.0f} COP",
                'Gastos': f"${finanzas[1]:,.0f} COP",
                'Balance': f"${finanzas[0]-finanzas[1]:,.0f} COP"
            }])
            df_resumen.to_excel(writer, sheet_name='📊 Resumen', index=False)
            if not df_animales.empty:
                df_animales.to_excel(writer, sheet_name='🐮🐷 Inventario', index=False)
            if not df_registros.empty:
                df_registros.to_excel(writer, sheet_name='📝 Movimientos', index=False)

        output.seek(0)
        filename = f"Finca_{nombre_finca.replace(' ','_')}_{hoy.strftime('%Y%m%d')}.xlsx"
        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        as_attachment=True, download_name=filename)

    except ImportError:
        return "❌ Librerías Excel no instaladas.", 500
    except Exception as e:
        print(f"❌ Error exportar Excel: {e}")
        return f"❌ Error: {e}", 500

# === INICIO DEL SERVIDOR ===
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🌍 Servidor iniciando en http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)