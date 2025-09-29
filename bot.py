# -*- coding: utf-8 -*-
"""
bot.py - Sistema de Registro Conversacional para Hacienda La Tática
Versión final con flujo interactivo, valor en gasto y producción, y reportes corregidos.
"""

import os
import psycopg2
import re
import datetime
from urllib.parse import urlparse

print("🔧 Iniciando bot.py...")

# === 1. CONEXIÓN A POSTGRESQL ===
def inicializar_bd():
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            print("❌ DATABASE_URL no está definida. Configúrala en Render.")
            return False

        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS animales (
                id SERIAL PRIMARY KEY,
                especie TEXT NOT NULL,
                id_externo TEXT UNIQUE NOT NULL,
                marca_o_arete TEXT NOT NULL,
                categoria TEXT,
                peso REAL,
                corral TEXT,
                estado TEXT DEFAULT 'sano',
                observaciones TEXT,
                fecha_registro DATE DEFAULT CURRENT_DATE
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS registros (
                id SERIAL PRIMARY KEY,
                fecha TEXT NOT NULL,
                tipo_actividad TEXT NOT NULL,
                accion TEXT,
                detalle TEXT,
                lugar TEXT,
                cantidad REAL,
                valor REAL,
                unidad TEXT,
                observacion TEXT,
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS salud_animal (
                id SERIAL PRIMARY KEY,
                id_externo TEXT NOT NULL,
                tipo TEXT NOT NULL,
                tratamiento TEXT,
                fecha TEXT NOT NULL,
                observacion TEXT,
                FOREIGN KEY (id_externo) REFERENCES animales (id_externo)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS produccion (
                id SERIAL PRIMARY KEY,
                fecha TEXT NOT NULL,
                producto TEXT NOT NULL,
                cantidad REAL,
                unidad TEXT,
                tipo TEXT,
                lugar TEXT,
                animal TEXT,
                observacion TEXT
            )
        ''')

        conn.commit()
        conn.close()
        print("✅ Base de datos PostgreSQL lista.")
        return True
    except Exception as e:
        print(f"❌ Error al conectar con PostgreSQL: {e}")
        return False

try:
    BD_OK = inicializar_bd()
except Exception as e:
    print(f"❌ Error crítico al inicializar BD: {e}")
    BD_OK = False

# === 2. SINÓNIMOS AMPLIADOS ===
SINONIMOS = {
    "siembra": ["sembrar", "sembramos", "siembra", "plantar", "plantamos", "pusimos semilla", "resiembra", "resembramos"],
    "cosecha": ["cosechar", "cosechamos", "cosecha", "cortar", "recolectar", "recolectamos", "descacotamos", "descacotar"],
    "reproduccion": ["monta", "preñez", "preñada", "inseminada", "inseminacion", "cubierta", "cubiertas", "cerda", "verraco", "toro", "novillo", "vaquilla", "nacimiento", "parto"],
    "labor": [
        "fumigar", "fumigamos", "rociar", "castramos", "curamos", "insecticida",
        "herbicida", "castrar", "poda", "control de plagas", "cerca", "cercamos",
        "limpiar", "abonamos", "podar", "podamos", "lavar", "reparar",
        "abonar", "aserrar", "lavado", "macaneadora", "macaneo", "destete", "marcacion", "marcaje", "macaneamos", "reparacion",
        "alimentar", "alimentamos", "dar de comer", "echamos comida", "comida", "alimento", "concentrado"
    ],
    "sanidad_animal": [
        "vacunar", "vacunamos", "vacuna", "inyectar", "pusimos vacuna", "inyección", "vacuno",
        "aftosa", "brucelosis", "fiebre aftosa", "virus", "inmunizar", "refuerzo",
        "desparasitar", "desparasitamos", "purgar", "dar desparasitante", "desparacitamos",
        "lombrices", "parásitos", "gusanos", "vermífugo", "desparacitación", "desparacito",
        "pastilla", "bolo", "gotas", "cinta", "tratamiento", "medicar", "chequeo", "sanidad"
    ],
    "produccion": ["carne", "kg", "kilos", "peso cerdo", "peso vaca", "salieron", "rendimiento", "cosechamos", "sacamos", "leche", "litros", "ordeñar", "producción", "lechón", "ternero", "ternera"],
    "inventario": ["llego", "ingreso", "compramos", "recibimos", "se compro", "se pidio", "se compraron", "pedimos", "nacio", "nacieron"],
    "gasto": ["gasto", "pagamos", "vendimos", "se murieron", "baja", "muertes", "perdida", "se pago", "se vendio", "vendieron", "factura", "compra", "costo"]
}

# === 3. ESTADO DEL USUARIO ===
user_state = {}

# === 4. DETECTAR ACTIVIDAD ===
def detectar_actividad(mensaje):
    mensaje = mensaje.lower()
    for actividad, palabras in SINONIMOS.items():
        for palabra in palabras:
            if palabra in mensaje:
                return actividad
    return "general"

# === 5. REGISTRO DE ANIMAL ===
def extraer_datos_animal(mensaje):
    datos = {"especie": None, "id_externo": None, "marca_o_arete": None, "categoria": None, "corral": None, "peso": None}
    mensaje = mensaje.lower()

    if "cerdo" in mensaje or any(c in mensaje for c in ["lechón", "cerda", "verraco", "ceba"]):
        datos["especie"] = "porcino"
    elif "vaca" in mensaje or any(c in mensaje for c in ["toro", "ternero", "ternera", "novillo", "vaquilla"]):
        datos["especie"] = "bovino"

    arete = re.search(r"(?:arete|chapeta)\s+(\d+)", mensaje)
    if arete:
        num = arete.group(1)
        datos["marca_o_arete"] = num
        datos["id_externo"] = f"C-{num}"

    marca = re.search(r"marca\s+([a-z0-9-]+)", mensaje, re.IGNORECASE)
    if marca:
        cod = marca.group(1).upper()
        datos["marca_o_arete"] = cod
        datos["id_externo"] = f"V-M-{cod}"

    categorias_validas = ["lechón", "cerda", "verraco", "ceba", "toro", "ternero", "ternera", "novillo", "vaquilla", "engorda", "lechera"]
    for cat in categorias_validas:
        if cat in mensaje:
            datos["categoria"] = cat
            break

    corral = re.search(r"(?:corral|lugar)\s+([a-z0-9]+)", mensaje, re.IGNORECASE)
    if corral: datos["corral"] = corral.group(1).upper()

    peso = re.search(r"peso\s*(\d+(?:\.\d+)?)\s*(kg|kilo|kilos)", mensaje)
    if peso: datos["peso"] = float(peso.group(1))

    return datos

def registrar_animal(datos):
    if not datos["especie"]: return "❌ No se detectó especie."
    if not datos["marca_o_arete"]: return "❌ No se encontró arete o marca."

    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url: return "❌ Base de datos no disponible"

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO animales (especie, id_externo, marca_o_arete, categoria, peso, corral, estado)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id_externo) DO NOTHING
                ''', (datos["especie"], datos["id_externo"], datos["marca_o_arete"], datos["categoria"],
                      datos["peso"], datos["corral"], "sano"))
            conn.commit()
        return f"✅ Registrado: {datos['id_externo']} ({datos['categoria'] or datos['especie']})"
    except Exception as e:
        print(f"❌ Error al registrar animal: {e}")
        return "Hubo un error al registrar el animal."

# === 6. GUARDAR REGISTRO GENERAL ===
def guardar_registro(tipo_actividad, accion, detalle, lugar=None, cantidad=None, valor=0, unidad=None, observacion=None):
    print(f"🔍 GUARDANDO REGISTRO: {tipo_actividad} | {detalle} | {lugar} | {cantidad} {unidad} | valor: {valor}")
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            print("❌ DATABASE_URL no está definida")
            return

        fecha = datetime.date.today().isoformat()
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO registros (fecha, tipo_actividad, accion, detalle, lugar, cantidad, valor, unidad, observacion, fecha_registro)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (fecha, tipo_actividad, accion, detalle, lugar, cantidad, valor, unidad, observacion, datetime.datetime.now().isoformat()))
            conn.commit()
        print(f"✅ REGISTRO GUARDADO: {tipo_actividad} - {detalle}")
    except Exception as e:
        print(f"❌ ERROR AL GUARDAR REGISTRO: {e}")
        import traceback
        print(traceback.format_exc())

# === 7. GENERAR REPORTE DETALLADO ===
def generar_reporte(frecuencia="semanal", formato="texto"):
    hoy = datetime.date.today()

    if frecuencia == "diario":
        dias = 1
        titulo = "📅 REPORTE DIARIO"
    elif frecuencia == "semanal":
        dias = 7
        titulo = "📅 REPORTE SEMANAL"
    elif frecuencia == "quincenal":
        dias = 15
        titulo = "📅 REPORTE QUINCENAL"
    elif frecuencia == "mensual":
        dias = 30
        titulo = "📅 REPORTE MENSUAL"
    else:
        return "❌ Frecuencia no válida."

    inicio = hoy - datetime.timedelta(days=dias)
    periodo = f"Del {inicio.strftime('%d/%m')} al {hoy.strftime('%d/%m')}"

    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return "❌ Base de datos no configurada"

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT fecha, tipo_actividad, accion, detalle, lugar, cantidad, valor, unidad, observacion
                    FROM registros WHERE fecha >= %s ORDER BY fecha, tipo_actividad
                """, (inicio.isoformat(),))
                registros = cursor.fetchall()
        print(f"📊 Reporte cargado: {len(registros)} registros desde {inicio}")
    except Exception as e:
        return f"❌ Error al leer la base de datos: {e}"

    if formato == "texto":
        lines = [titulo, periodo, ""]

        siembras = [r for r in registros if r[1] == "siembra"]
        cosechas = [r for r in registros if r[1] == "cosecha"]
        labores = [r for r in registros if r[1] == "labor"]
        sanidad = [r for r in registros if r[1] == "sanidad_animal"]
        produccion = [r for r in registros if r[1] == "produccion"]
        insumos = [r for r in registros if r[1] == "inventario" and "insumo" in (r[2] or '')]
        gastos = [r for r in registros if r[1] == "gasto"]
        jornales = [r for r in registros if "jornal" in (r[2] or r[8] or '')]
        nuevos = [r for r in registros if r[1] == "reproduccion" and "nuevo" in (r[2] or '').lower()]
        gastos_jornales = [r for r in gastos if "jornal" in (r[8] or '').lower()]

        # SIEMBRAS
        if siembras:
            lines.append("🌱 SIEMBRAS")
            for row in siembras:
                desc = f"• {row[3] or 'producto'}"
                if row[4]: desc += f" en {row[4]}"
                if row[5] and row[7]: desc += f" ({row[5]} {row[7]})"
                if row[8]: desc += f". Obs: {row[8]}"
                lines.append(desc)
            lines.append("")

        # COSECHAS
        if cosechas:
            lines.append("🌽 COSECHAS")
            for row in cosechas:
                desc = f"• {row[5] or '?'} {row[7] or ''} de {row[3] or 'producto'}"
                if row[4]: desc += f" del {row[4]}"
                if row[8]: desc += f". Obs: {row[8]}"
                lines.append(desc)
            lines.append("")

        # LABORES
        if labores:
            lines.append("🛠️ LABORES")
            for row in labores:
                desc = f"• {row[3] or 'actividad'}"
                if row[4]: desc += f" en {row[4]}"
                if row[5]:
                    unidad = row[7] or "unidad"
                    desc += f" ({row[5]} {unidad})"
                if any(x in (row[3] or '').lower() for x in ['comida', 'alimento', 'alimentar']):
                    desc = f"🍽️ {desc}"
                if row[8]: desc += f". Obs: {row[8]}"
                lines.append(desc)
            lines.append("")

        # SANIDAD ANIMAL
        if sanidad:
            lines.append("💉 SANIDAD ANIMAL")
            for row in sanidad:
                desc = f"• {row[3] or 'tratamiento'}"
                if row[4]: desc += f" en {row[4]}"
                if row[5]:
                    unidad = row[7] or "dosis"
                    desc += f" ({row[5]} {unidad})"
                if row[8]: desc += f". Obs: {row[8]}"
                lines.append(desc)
            lines.append("")

        # PRODUCCIÓN (con valor de venta)
        if produccion:
            lines.append("🥛🥩 PRODUCCIÓN")
            for row in produccion:
                if not row[3]: continue
                desc = f"• {row[5]} {row[7]} de {row[3]}"
                if row[4]: desc += f" del {row[4]}"
                if row[6] and row[6] > 0:  # valor > 0
                    desc += f" → Venta: ${row[6]:,.0f}"
                if row[8]: desc += f". Obs: {row[8]}"
                lines.append(desc)
            lines.append("")

        # INSUMOS
        if insumos:
            lines.append("🧪 INSUMOS RECIBIDOS")
            for row in insumos:
                desc = f"• {row[5]} {row[7]} de {row[3]}"
                if row[4]: desc += f" para {row[4]}"
                if row[8]: desc += f". Obs: {row[8]}"
                lines.append(desc)
            lines.append("")

        # GASTOS
        if gastos:
            lines.append("💰 GASTOS")
            total = sum(r[6] for r in gastos if r[6] > 0)
            for row in gastos:
                cantidad = row[5] if row[5] is not None else ""
                unidad = row[7] or ""
                valor = row[6] if row[6] > 0 else 0
                desc = f"• {row[3] or 'Gasto'}"
                if cantidad and unidad:
                    desc += f" ({cantidad} {unidad})"
                elif cantidad:
                    desc += f" ({cantidad})"
                if valor > 0:
                    desc += f" → ${valor:,.0f}"
                if row[8]:
                    desc += f". Obs: {row[8]}"
                lines.append(desc)
            if total > 0:
                lines.append(f"  → Total: ${total:,.0f}")
            lines.append("")

        # JORNALES
        total_jornales = sum(r[5] for r in jornales if r[5])
        total_gastado = sum(r[6] for r in gastos_jornales if r[6] > 0)

        if jornales or gastos_jornales:
            lines.append("👷 JORNALES")
            if total_jornales > 0:
                lines.append(f"• Cantidad: {int(total_jornales)} jornales")
            if total_gastado > 0:
                lines.append(f"• Costo total: ${total_gastado:,.0f}")
            else:
                lines.append(f"• Costo estimado: ${int(total_jornales * 15000):,.0f}")
            lines.append("")

        # ANIMALES NUEVOS
        if nuevos:
            lines.append("🆕 ANIMALES NUEVOS")
            for row in nuevos:
                cantidad = row[5] if row[5] is not None else 1
                unidad = row[7] or "unidad"
                desc = f"• {cantidad} {unidad} de {row[3]}"
                if row[4]: desc += f" en {row[4]}"
                if row[8]: desc += f". Obs: {row[8]}"
                lines.append(desc)
            lines.append("")

        lines.append("✅ Todo bajo control. ¡Buen trabajo!")
        return "\n".join(lines)

    return registros

# === 8. COMANDO SECRETO ===
def vaciar_tablas():
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            print("❌ DATABASE_URL no está definida.")
            return "❌ Base de datos no configurada"

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    TRUNCATE TABLE registros, animales, salud_animal, produccion 
                    RESTART IDENTITY CASCADE;
                ''')
            conn.commit()
        return "✅ Base de datos limpiada. Todo listo para empezar de nuevo."
    
    except Exception as e:
        print(f"❌ Error al limpiar BD: {e}")
        return "❌ No se pudo limpiar la base de datos."

# === 9. ESTADO DEL ANIMAL ===
def consultar_estado_animal(arete):
    try:
        database_url = os.environ.get("DATABASE_URL")
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT especie, estado, peso, corral, fecha_registro, observaciones
                    FROM animales WHERE id_externo = %s OR marca_o_arete = %s
                """, (arete, arete))
                row = cursor.fetchone()
                if not row:
                    return f"❌ No encontré ningún animal con arete o marca '{arete}'."

                especie, estado, peso, corral, fecha_reg, obs = row

                cursor.execute("""
                    SELECT tipo, fecha FROM salud_animal 
                    WHERE id_externo = %s ORDER BY fecha DESC LIMIT 2
                """, (arete,))
                salud = cursor.fetchall()
                vacuna = "N/A"
                despar = "N/A"
                for tipo, fecha in salud:
                    if "vacuna" in tipo.lower() or "aftosa" in tipo.lower():
                        vacuna = f"{fecha}"
                    elif "despar" in tipo.lower() or "purgar" in tipo.lower():
                        despar = f"{fecha}"

                return (
                    f"🐷 ANIMAL {arete} ({especie})\n"
                    f"• Estado: {estado}\n"
                    f"• Peso: {peso or 'No registrado'} kg\n"
                    f"• Corral: {corral or 'No asignado'}\n"
                    f"• Última vacuna: {vacuna}\n"
                    f"• Última desparasitación: {despar}\n"
                    f"• Nacido: {fecha_reg}\n"
                    f"• Observaciones: {obs or 'Sin notas'}"
                )
    except Exception as e:
        return "❌ Error al consultar el animal. Inténtalo más tarde."

# === 10. FLUJO CONVERSACIONAL ===
def iniciar_flujo_conversacional(numero, mensaje):
    """Inicia o continúa el flujo conversacional para un número de WhatsApp"""
    if numero not in user_state:
        user_state[numero] = {
            "step": "waiting_for_category",
            "data": {
                "tipo": "",
                "detalle": "",
                "cantidad": None,
                "valor": 0,
                "lugar": "",
                "observacion": ""
            }
        }

    state = user_state[numero]
    msg = mensaje.strip().lower()

    # === PASO 1: ¿Qué vamos a registrar hoy? ===
    if state["step"] == "waiting_for_category":
        if msg in ["fin", "salir", "cancelar", "no", "nada"]:
            del user_state[numero]
            return "✅ ¡Gracias por usar Hacienda La Tática! Vuelve cuando necesites."

        if msg in ["1", "siembra", "siembra", "sembrar"]:
            state["data"]["tipo"] = "siembra"
            state["step"] = "waiting_for_detalle"
            return "🌱 ¿Qué sembraste? (Ej: maíz, arroz, papa)"
        
        elif msg in ["2", "cosecha", "cosechar"]:
            state["data"]["tipo"] = "cosecha"
            state["step"] = "waiting_for_detalle"
            return "🌽 ¿Qué cosechaste? (Ej: papa, café, maíz)"
        
        elif msg in ["3", "sanidad", "vacuna", "desparasitacion", "desparasitar", "inyectar", "tratamiento"]:
            state["data"]["tipo"] = "sanidad_animal"
            state["step"] = "waiting_for_detalle"
            return "💉 ¿Fue una vacuna o una desparasitación? (Ej: vacuna, desparasitación)"
        
        elif msg in ["4", "animal", "nacimiento", "nacio", "nacieron"]:
            state["data"]["tipo"] = "reproduccion"
            state["step"] = "waiting_for_detalle"
            return "🐷 ¿Qué tipo de animal nació? (Ej: lechón, ternera, novillo)"
        
        elif msg in ["5", "gasto", "pagamos", "compra", "costo"]:
            state["data"]["tipo"] = "gasto"
            state["step"] = "waiting_for_detalle"
            return "💰 ¿Qué gastaste? (Ej: jornales, concentrado, medicina)"
        
        elif msg in ["6", "produccion", "producción", "leche", "carne", "kg"]:
            state["data"]["tipo"] = "produccion"
            state["step"] = "waiting_for_detalle"
            return "🥛 ¿Qué produjiste o vendiste? (Ej: leche, carne, huevos)"
        
        elif msg in ["7", "labor", "limpiar", "fumigar", "alimentar", "reparar"]:
            state["data"]["tipo"] = "labor"
            state["step"] = "waiting_for_detalle"
            return "🛠️ ¿Qué labor hiciste? (Ej: limpieza de corral, alimentación, poda)"
        
        else:
            tipo_detectado = detectar_actividad(mensaje)
            if tipo_detectado in SINONIMOS:
                state["data"]["tipo"] = tipo_detectado
                state["step"] = "waiting_for_detalle"
                return f"✅ Entendí: {tipo_detectado}. ¿Qué detalle quieres registrar? (Ej: maíz, vacuna aftosa, limpieza)"

            return (
                "🌿 ¡Hola! Bienvenido a Hacienda La Tática.\n\n"
                "¿Qué actividad vamos a registrar hoy?\n\n"
                "1. 🌱 Siembra\n"
                "2. 🌽 Cosecha\n"
                "3. 💉 Sanidad animal\n"
                "4. 🐷 Nuevo animal\n"
                "5. 💰 Gasto\n"
                "6. 🥛 Producción\n"
                "7. 🛠️ Labor (limpieza, alimentación, etc.)\n\n"
                "O escribe directamente lo que hiciste (ej: 'vacuné', 'sembré maíz')\n\n"
                "Escribe 'fin' para salir."
            )

    # === PASO 2: Detalle ===
    elif state["step"] == "waiting_for_detalle":
        state["data"]["detalle"] = mensaje
        state["step"] = "waiting_for_cantidad"
        return "🔢 ¿Cuántas unidades? (Ej: 5, 10, 1) — o escribe 'ninguna' si no aplica"

    # === PASO 3: Cantidad ===
    elif state["step"] == "waiting_for_cantidad":
        if msg in ["ninguna", "no", "0", "sin"]:
            state["data"]["cantidad"] = None
        else:
            try:
                state["data"]["cantidad"] = float(msg)
            except ValueError:
                return "❌ Por favor, escribe un número (Ej: 5, 10) o 'ninguna'"

        # Si es gasto o produccion, preguntar valor
        if state["data"]["tipo"] in ["gasto", "produccion"]:
            state["step"] = "waiting_for_valor"
            return "💰 ¿Cuál es el valor total? (Ej: 150000) — o escribe '0' si no aplica"
        else:
            state["step"] = "waiting_for_lugar"
            return "📍 ¿Dónde fue? (Ej: corral A, lote 3, bodega)"
    
    # === PASO 3B: Valor (para gasto y produccion) ===
    elif state["step"] == "waiting_for_valor":
        try:
            state["data"]["valor"] = float(msg)
        except ValueError:
            return "❌ Por favor, escribe un número (Ej: 150000)"
        
        state["step"] = "waiting_for_lugar"
        return "📍 ¿Dónde fue? (Ej: corral A, lote 3, bodega)"

    # === PASO 4: Lugar ===
    elif state["step"] == "waiting_for_lugar":
        state["data"]["lugar"] = mensaje
        state["step"] = "waiting_for_observacion"
        return "📝 ¿Quieres agregar alguna observación? (Ej: marcas C-101 a C-105, animales jóvenes, factura #123)\nEscribe 'fin' para guardar sin observación."

    # === PASO 5: Observación ===
    elif state["step"] == "waiting_for_observacion":
        if msg in ["fin", "salir", "listo", "guardar"]:
            state["data"]["observacion"] = ""
        else:
            state["data"]["observacion"] = mensaje

        # Guardar en base de datos
        tipo = state["data"]["tipo"]
        detalle = state["data"]["detalle"]
        cantidad = state["data"]["cantidad"]
        valor = state["data"]["valor"]
        lugar = state["data"]["lugar"]
        observacion = state["data"]["observacion"]

        # Para animales nuevos
        if tipo == "reproduccion":
            datos = extraer_datos_animal(detalle)
            if datos["especie"] and datos["marca_o_arete"]:
                respuesta = registrar_animal(datos)
                guardar_registro(tipo, "nuevo animal", detalle, lugar, 1, 0, "unidad", observacion)
                del user_state[numero]
                return f"{respuesta}\n\n✅ ¡Registro guardado! Vuelve cuando necesites."
            else:
                guardar_registro(tipo, "nuevo animal", detalle, lugar, 1, 0, "unidad", observacion)
                del user_state[numero]
                return f"✅ Registrado: {detalle} en {lugar}\n\n¡Gracias por registrar!"

        # Para otros tipos
        else:
            unidad = "COP" if tipo == "gasto" else "unidad"
            guardar_registro(tipo, tipo, detalle, lugar, cantidad, valor, unidad, observacion)
            del user_state[numero]
            return f"✅ ¡Registrado! {detalle} ({cantidad or 'sin cantidad'}) en {lugar or 'sin lugar'}\n\n¡Gracias por usar Hacienda La Tática!"

    return "❌ Error interno. Intenta de nuevo."

# === 11. PROCESAR MENSAJE WHASTAPP ===
def procesar_mensaje_whatsapp(mensaje, remitente=None):
    print(f"🔍 [BOT] Procesando mensaje: '{mensaje}'")
    mensaje = mensaje.strip()
    if not mensaje:
        return "❌ Mensaje vacío."

    # --- COMANDO SECRETO: limpiar bd ---
    if mensaje.strip().lower() == "limpiar bd":
        if remitente == "whatsapp:+573143539351":  # Tu número
            return vaciar_tablas()
        else:
            return "❌ Acceso denegado. Comando no autorizado."

    # --- COMANDO: estado animal ---
    if mensaje.lower().startswith("estado animal "):
        arete = mensaje.split(" ", 2)[2].strip().upper()
        return consultar_estado_animal(arete)

    # --- COMANDO: ayuda ---
    if mensaje.strip().lower() in ["ayuda", "help", "ayuda?"]:
        return (
            "🌿 ¡Hola! Bienvenido al Bot de Hacienda La Tática.\n\n"
            "Para registrar una actividad, puedes:\n\n"
            "1. Usar el modo conversacional: Solo escribe 'hola' o 'hoy vacuné'\n"
            "2. Usar el modo comando: REGISTRAR: tipo, detalle, cantidad, valor, unidad, lugar, observación\n\n"
            "Ejemplos de comando:\n"
            "🔹 REGISTRAR: siembra, maíz, 5, 0, bolsas, lote 3\n"
            "🔹 REGISTRAR: sanidad, vacuna aftosa, 10, 0, dosis, corral A, marcas M1-M10\n"
            "🔹 REGISTRAR: gasto, jornales, 5, 75000, COP, pago a Juan, factura #123\n"
            "🔹 REGISTRAR: produccion, leche, 8, 160000, litros, corral B, vendida a intermediario\n\n"
            "📊 Reportes:\n"
            "reporte semanal\n"
            "reporte diario\n"
            "reporte mensual\n\n"
            "🔧 ¿Olvidaste algo? Escribe 'ayuda' otra vez."
        )

    # --- MODOS DE REGISTRO ---

    # MODELO ANTIGUO: REGISTRAR: tipo, detalle, ...
    if mensaje.upper().startswith("REGISTRAR:"):
        partes = mensaje.split(":", 1)
        resto = partes[1].strip()
        campos = [campo.strip() for campo in resto.split(",")]
        if len(campos) < 4:
            return "❌ Faltan campos. Usa: REGISTRAR: tipo, detalle, cantidad, valor, unidad, lugar, observación"

        tipo = campos[0].lower()
        detalle = campos[1] if len(campos) > 1 else None
        cantidad = None
        valor = 0.0
        unidad = None
        lugar = None
        observacion = None

        try:
            cantidad = float(campos[2]) if campos[2] else None
        except ValueError:
            pass

        try:
            valor = float(campos[3]) if campos[3] else 0.0
        except ValueError:
            pass

        if len(campos) > 4:
            unidad = campos[4]
        if len(campos) > 5:
            lugar = campos[5].upper()
        if len(campos) > 6:
            observacion = campos[6]

        respuesta_detalle = []

        # Siembra
        if "siembra" in tipo:
            guardar_registro("siembra", "siembra", detalle, lugar, cantidad, 0, unidad, observacion)
            respuesta_detalle.append(f"siembra de {detalle}")

        # Cosecha
        elif "cosecha" in tipo:
            guardar_registro("cosecha", "cosecha", detalle, lugar, cantidad, 0, unidad, observacion)
            respuesta_detalle.append(f"cosecha de {detalle}")

        # Sanidad animal
        elif "sanidad" in tipo or "vacuna" in tipo or "inyectar" in tipo or "desparasitar" in tipo or "purgar" in tipo:
            guardar_registro("sanidad_animal", "sanidad", detalle, lugar, cantidad, valor, unidad, observacion)
            respuesta_detalle.append(f"sanidad: {detalle}")

        # Labor
        elif "labor" in tipo or any(lab in tipo for lab in ["fumigar", "poda", "limpiar", "reparar", "alimentar"]):
            guardar_registro("labor", "labor", detalle, lugar, cantidad, valor, unidad, observacion)
            if cantidad and "jornal" in (unidad or detalle or ''):
                respuesta_detalle.append(f"{int(cantidad)} jornales")
                if valor > 0:
                    guardar_registro("gasto", "pago jornales", detalle, lugar, valor, 0, "COP", f"jornales: {int(cantidad)}")
                    respuesta_detalle.append(f"gasto: ${valor:,.0f}")
            else:
                respuesta_detalle.append(detalle or tipo)

        # Insumo
        elif "insumo" in tipo:
            guardar_registro("inventario", "insumo", detalle, lugar, cantidad, 0, unidad, observacion)
            respuesta_detalle.append(f"{cantidad} {unidad} de {detalle}")

        # Gasto
        elif "gasto" in tipo:
            guardar_registro("gasto", "gasto", detalle, lugar, 0, valor, "COP", observacion)
            respuesta_detalle.append(f"gasto de ${valor:,.0f}")

        # Producción
        elif "produccion" in tipo or "producción" in tipo:
            guardar_registro("produccion", "produccion", detalle, lugar, cantidad, valor, unidad, observacion)
            respuesta_detalle.append(f"{cantidad} {unidad} de {detalle}")

        # Animal nuevo
        elif "animal" in tipo or any(an in tipo for an in ["porcino", "vaca", "cerdo"]):
            datos = extraer_datos_animal(mensaje)
            if not datos["especie"]:
                return "❌ No se detectó especie"
            if not datos["marca_o_arete"]:
                return "❌ Falta el arete"
            
            resultado = registrar_animal(datos)
            guardar_registro(
                "reproduccion",
                "nuevo animal",
                f"{datos['especie']} ({datos['categoria']})",
                datos["corral"],
                1,
                0,
                "unidad",
                f"arete: {datos['marca_o_arete']}"
            )
            return f"✅ Registrado: {datos['id_externo']} ({datos['categoria']})"

        else:
            guardar_registro("general", tipo, detalle, lugar, cantidad, valor, unidad, observacion)
            respuesta_detalle.append(detalle or tipo)

        return f"✅ REGISTRAR registrada: {', '.join(respuesta_detalle)}"

    # --- COMANDOS DE REPORTE ---
    mensaje_lower = mensaje.lower()
    if "reporte" in mensaje_lower:
        if "diario" in mensaje_lower:
            return generar_reporte("diario")
        elif "quincenal" in mensaje_lower:
            return generar_reporte("quincenal")
        elif "mensual" in mensaje_lower:
            return generar_reporte("mensual")
        else:
            return generar_reporte("semanal")

    # --- FLUJO CONVERSACIONAL (si no es comando) ---
    if remitente:
        return iniciar_flujo_conversacional(remitente, mensaje)

    return "❌ No entendí. Escribe 'ayuda' para ver cómo usar el bot."