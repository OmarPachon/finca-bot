# -*- coding: utf-8 -*-
"""
bot.py - Sistema de Registro Conversacional para Hacienda La Tática
Versión FINAL: gestión de animales + jornales con valor + reporte mejorado.
"""

import os
import psycopg2
import re
import datetime
from urllib.parse import urlparse

print("🔧 Iniciando bot.py...")

# === 1. CONEXIÓN A POSTGRESQL CON MIGRACIÓN AUTOMÁTICA ===
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
                estado TEXT DEFAULT 'activo',
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
                jornales INTEGER,
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

        # === MIGRACIÓN: asegurar columna 'jornales' ===
        cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='registros' AND column_name='jornales'
                ) THEN
                    ALTER TABLE registros ADD COLUMN jornales INTEGER;
                END IF;
            END $$;
        """)

        conn.commit()
        conn.close()
        print("✅ Base de datos PostgreSQL lista (con migración de 'jornales').")
        return True
    except Exception as e:
        print(f"❌ Error al conectar con PostgreSQL: {e}")
        return False

try:
    BD_OK = inicializar_bd()
except Exception as e:
    print(f"❌ Error crítico al inicializar BD: {e}")
    BD_OK = False

# === 2. SINÓNIMOS MEJORADOS ===
SINONIMOS = {
    "siembra": [
        "sembrar", "siembra", "plantar", "resiembra", "resembrar",
        "poner cultivo", "echar semilla", "sembramos", "plantamos"
    ],
    "produccion": [
        "cosechar", "cosecha", "recolectar", "cortar", "descacotar",
        "sacar cosecha", "recoger", "cosechamos", "recolectamos", "sacamos",
        "producir", "producción", "rendimiento", "salida",
        "leche", "carne", "huevos", "litros", "kilos", "kg",
        "vendimos", "sacamos producto", "salieron", "rendir",
        "maíz", "papa", "arroz", "cacao", "café", "yuca", "plátano", "frijol", "trigo", "cebolla"
    ],
    "sanidad_animal": [
        "vacunar", "vacuna", "inyectar", "desparasitar", "purgar",
        "medicar", "tratamiento", "sanidad", "chequeo",
        "aftosa", "brucelosis", "desparacitar", "vermífugo",
        "pusimos vacuna", "inyección", "pastilla", "bolo"
    ],
    "ingreso_animal": [
        "nacer", "nació", "nacieron", "nacimiento", "parto",
        "comprar animal", "compramos", "llegó animal", "adquirimos",
        "nuevo animal", "ingreso", "compra de", "lechón", "ternero"
    ],
    "salida_animal": [
        "vender", "vendimos", "venta de", "se murieron", "muertes",
        "baja", "sacrificio", "fallecieron", "perdimos", "salida de",
        "muerto", "vendido"
    ],
    "gasto": [
        "gastar", "gasto", "pagar", "comprar", "compra",
        "factura", "costo", "inversión", "egreso"
    ],
    "labor": [
        "limpiar", "fumigar", "rociar", "castrar", "podar", "abonar",
        "reparar", "alimentar", "dar comida", "echamos comida",
        "lavar", "destetar", "marcar", "marcaje", "cerca", "cercar",
        "trabajar en", "hicimos labor", "actividad en"
    ]
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

# === 5. EXTRAER DATOS DE ANIMAL ===
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

# === 6. REGISTRAR ANIMAL ===
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
                      datos["peso"], datos["corral"], "activo"))
            conn.commit()
        return f"✅ Registrado: {datos['id_externo']} ({datos['categoria'] or datos['especie']})"
    except Exception as e:
        print(f"❌ Error al registrar animal: {e}")
        return "Hubo un error al registrar el animal."

# === 8. GUARDAR REGISTRO GENERAL ===
def guardar_registro(tipo_actividad, accion, detalle, lugar=None, cantidad=None, valor=0, unidad=None, observacion=None, jornales=None):
    print(f"🔍 GUARDANDO REGISTRO: {tipo_actividad} | {detalle} | lugar: {lugar} | cantidad: {cantidad} {unidad} | jornales: {jornales} | valor: {valor}")
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            print("❌ DATABASE_URL no está definida")
            return

        fecha = datetime.date.today().isoformat()
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO registros (fecha, tipo_actividad, accion, detalle, lugar, cantidad, valor, unidad, observacion, jornales, fecha_registro)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (fecha, tipo_actividad, accion, detalle, lugar, cantidad, valor, unidad, observacion, jornales, datetime.datetime.now().isoformat()))
            conn.commit()
        print(f"✅ REGISTRO GUARDADO: {tipo_actividad} - {detalle}")
    except Exception as e:
        print(f"❌ ERROR AL GUARDAR REGISTRO: {e}")
        import traceback
        print(traceback.format_exc())

# === 9. GENERAR REPORTE DETALLADO ===
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
                    SELECT fecha, tipo_actividad, accion, detalle, lugar, cantidad, valor, unidad, observacion, jornales
                    FROM registros WHERE fecha >= %s ORDER BY fecha, tipo_actividad
                """, (inicio.isoformat(),))
                registros = cursor.fetchall()
        print(f"📊 Reporte cargado: {len(registros)} registros desde {inicio}")
    except Exception as e:
        return f"❌ Error al leer la base de datos: {e}"

    if formato == "texto":
        lines = [titulo, periodo, ""]

        siembras = [r for r in registros if r[1] == "siembra"]
        produccion_total = [r for r in registros if r[1] == "produccion"]
        labores = [r for r in registros if r[1] == "labor"]
        sanidad = [r for r in registros if r[1] == "sanidad_animal"]
        ingresos = [r for r in registros if r[1] == "ingreso_animal"]
        salidas = [r for r in registros if r[1] == "salida_animal"]
        gastos = [r for r in registros if r[1] == "gasto"]

        # SIEMBRAS
        if siembras:
            lines.append("🌱 SIEMBRAS")
            for row in siembras:
                desc = f"• {row[3] or 'producto'}"
                if row[4]: desc += f" en {row[4]}"
                if row[5] and row[7]: desc += f" ({row[5]} {row[7]})"
                if row[9] or row[6]:  # jornales o valor
                    extras = []
                    if row[9]: extras.append(f"{row[9]} jornales")
                    if row[6] > 0: extras.append(f"${row[6]:,.0f}")
                    if extras: desc += " → " + ", ".join(extras)
                if row[8]: desc += f". Obs: {row[8]}"
                lines.append(desc)
            lines.append("")

        # PRODUCCIÓN
        if produccion_total:
            vegetal = []
            animal = []
            cultivos = {"maíz", "papa", "arroz", "cacao", "café", "yuca", "plátano", "frijol", "trigo", "cebolla"}
            for row in produccion_total:
                detalle = (row[3] or "").lower()
                if any(cultivo in detalle for cultivo in cultivos):
                    vegetal.append(row)
                else:
                    animal.append(row)

            if vegetal:
                lines.append("🌽 PRODUCCIÓN VEGETAL")
                for row in vegetal:
                    desc = f"• {row[5]} {row[7]} de {row[3]}"
                    if row[4]: desc += f" del {row[4]}"
                    if row[6] > 0: desc += f" → Venta: ${row[6]:,.0f}"
                    if row[9] or (row[6] <= 0 and row[9]):
                        extras = []
                        if row[9]: extras.append(f"{row[9]} jornales")
                        if extras: desc += " → " + ", ".join(extras)
                    if row[8]: desc += f". Obs: {row[8]}"
                    lines.append(desc)
                lines.append("")

            if animal:
                lines.append("🥛🥩 PRODUCCIÓN ANIMAL")
                for row in animal:
                    if not row[3]: continue
                    desc = f"• {row[5]} {row[7]} de {row[3]}"
                    if row[4]: desc += f" del {row[4]}"
                    if row[6] > 0: desc += f" → Venta: ${row[6]:,.0f}"
                    if row[9] or (row[6] <= 0 and row[9]):
                        extras = []
                        if row[9]: extras.append(f"{row[9]} jornales")
                        if extras: desc += " → " + ", ".join(extras)
                    if row[8]: desc += f". Obs: {row[8]}"
                    lines.append(desc)
                lines.append("")

        # INGRESO DE ANIMALES
        if ingresos:
            lines.append("🐷 INGRESO DE ANIMALES")
            for row in ingresos:
                desc = f"• {row[5] or 1} {row[7] or 'unidad'} de {row[3]}"
                if row[4]: desc += f" en {row[4]}"
                if row[6] > 0: desc += f" → Costo: ${row[6]:,.0f}"
                if row[8]: desc += f". Obs: {row[8]}"
                lines.append(desc)
            lines.append("")

        # SALIDA DE ANIMALES
        if salidas:
            lines.append("🐄 SALIDA DE ANIMALES")
            for row in salidas:
                motivo = "Venta" if "venta" in (row[8] or '').lower() or row[6] > 0 else "Muerte"
                desc = f"• {row[5] or 1} {row[7] or 'unidad'} de {row[3]} ({motivo})"
                if row[4]: desc += f" del {row[4]}"
                if row[6] > 0: desc += f" → Ingreso: ${row[6]:,.0f}"
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
                if row[9] or row[6]:
                    extras = []
                    if row[9]: extras.append(f"{row[9]} jornales")
                    if row[6] > 0: extras.append(f"${row[6]:,.0f}")
                    if extras: desc += " → " + ", ".join(extras)
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
                if row[9] or row[6]:
                    extras = []
                    if row[9]: extras.append(f"{row[9]} jornales")
                    if row[6] > 0: extras.append(f"${row[6]:,.0f}")
                    if extras: desc += " → " + ", ".join(extras)
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

        # === SECCIÓN ESPECIAL: COSTO DE JORNALES POR ACTIVIDAD ===
        actividades_con_jornales = [r for r in registros if r[9] and r[9] > 0]
        if actividades_con_jornales:
            lines.append("💵 COSTO DE JORNALES POR ACTIVIDAD")
            total_jornales_general = 0
            total_valor_jornales = 0
            for row in actividades_con_jornales:
                tipo = row[1].replace("_", " ").title()
                jornales = row[9]
                valor = row[6] if row[6] > 0 else 0
                total_jornales_general += jornales
                total_valor_jornales += valor
                desc = f"• {tipo}: {jornales} jornales"
                if valor > 0:
                    desc += f" → ${valor:,.0f}"
                lines.append(desc)
            if total_jornales_general > 0:
                lines.append(f"→ Total jornales: {total_jornales_general}")
            if total_valor_jornales > 0:
                lines.append(f"→ Total valor: ${total_valor_jornales:,.0f}")
            lines.append("")

        lines.append("✅ Todo bajo control. ¡Buen trabajo!")
        return "\n".join(lines)

    return registros

# === 10. COMANDO SECRETO ===
def vaciar_tablas():
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            print("❌ DATABASE_URL no está definida.")
            return "❌ Base de datos no configurada"

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    TRUNCATE TABLE registros, animales, salud_animal 
                    RESTART IDENTITY CASCADE;
                ''')
            conn.commit()
        return "✅ Base de datos limpiada. Todo listo para empezar de nuevo."
    
    except Exception as e:
        print(f"❌ Error al limpiar BD: {e}")
        return "❌ No se pudo limpiar la base de datos."

# === 11. CONSULTAR ESTADO ANIMAL ===
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
                    f"• Registrado: {fecha_reg}\n"
                    f"• Observaciones: {obs or 'Sin notas'}"
                )
    except Exception as e:
        return "❌ Error al consultar el animal. Inténtalo más tarde."

# === 12. FLUJO CONVERSACIONAL ===
def iniciar_flujo_conversacional(numero, mensaje):
    if numero not in user_state:
        user_state[numero] = {
            "step": "waiting_for_category",
            "data": {
                "tipo": "", "detalle": "", "cantidad": None, "valor": 0,
                "unidad": "", "lugar": "", "observacion": "", "jornales": 0, "subtipo": ""
            }
        }

    state = user_state[numero]
    msg = mensaje.strip().lower()

    if msg in ["fin", "salir", "cancelar", "no", "nada", "0"]:
        del user_state[numero]
        return "✅ ¡Gracias por usar Hacienda La Tática! Vuelve cuando necesites."

    if state["step"] == "waiting_for_category":
        if msg in [str(i) for i in range(1, 8)]:
            user_state[numero] = {
                "step": "waiting_for_category",
                "data": {
                    "tipo": "", "detalle": "", "cantidad": None, "valor": 0,
                    "unidad": "", "lugar": "", "observacion": "", "jornales": 0, "subtipo": ""
                }
            }
            state = user_state[numero]

        if msg in ["1", "siembra", "sembrar"]:
            state["data"]["tipo"] = "siembra"
            state["step"] = "waiting_for_detalle"
            return "🌱 ¿Qué sembraste? (Ej: maíz, cacao)"

        elif msg in ["2", "produccion", "cosecha", "leche", "carne"]:
            state["data"]["tipo"] = "produccion"
            state["step"] = "waiting_for_detalle"
            return "🌾 ¿Qué produjiste o cosechaste? (Ej: papa, leche)"

        elif msg in ["3", "sanidad", "vacuna", "desparasitar"]:
            state["data"]["tipo"] = "sanidad_animal"
            state["step"] = "waiting_for_detalle"
            return "💉 ¿Fue vacuna o desparasitación? (Ej: aftosa, gusanos)"

        elif msg in ["4", "ingreso", "compra", "nacimiento"]:
            state["data"]["tipo"] = "ingreso_animal"
            state["step"] = "waiting_for_subtipo"
            return "🐷 ¿Es por nacimiento o compra de animales?"

        elif msg in ["5", "salida", "venta", "muerte"]:
            state["data"]["tipo"] = "salida_animal"
            state["step"] = "waiting_for_subtipo"
            return "🐄 ¿Es por venta o muerte de animales?"

        elif msg in ["6", "gasto", "pagamos", "compra"]:
            state["data"]["tipo"] = "gasto"
            state["step"] = "waiting_for_detalle"
            return "💰 ¿Qué gastaste? (Ej: medicina, jornales)"

        elif msg in ["7", "labor", "limpiar", "alimentar"]:
            state["data"]["tipo"] = "labor"
            state["step"] = "waiting_for_detalle"
            return "🛠️ ¿Qué labor hiciste? (Ej: alimentación, limpieza)"

        else:
            tipo_detectado = detectar_actividad(mensaje)
            if tipo_detectado in SINONIMOS:
                state["data"]["tipo"] = tipo_detectado
                if tipo_detectado in ["ingreso_animal", "salida_animal"]:
                    state["step"] = "waiting_for_subtipo"
                    return "❓ ¿Nacimiento/compra o venta/muerte?"
                else:
                    state["step"] = "waiting_for_detalle"
                    return f"✅ Entendí: {tipo_detectado}. ¿Qué detalle quieres registrar?"

            return (
                "🌿 ¡Hola! Bienvenido a Hacienda La Tática.\n\n"
                "¿Qué actividad vamos a registrar hoy?\n\n"
                "1. 🌱 Siembra\n"
                "2. 🌾 Producción (cosecha, leche, carne)\n"
                "3. 💉 Sanidad animal\n"
                "4. 🐷 Ingreso de animales (nacimientos, compras)\n"
                "5. 🐄 Salida de animales (ventas, muertes)\n"
                "6. 💰 Gasto\n"
                "7. 🛠️ Labor\n\n"
                "Escribe 'fin' o '0' para salir."
            )

    elif state["step"] == "waiting_for_subtipo":
        tipo = state["data"]["tipo"]
        if tipo == "ingreso_animal":
            if "nac" in msg or "parto" in msg:
                state["data"]["subtipo"] = "nacimiento"
                state["step"] = "waiting_for_detalle"
                return "🐷 ¿Qué tipo de animal nació? (Ej: lechón, ternera)"
            elif "compra" in msg or "compramos" in msg:
                state["data"]["subtipo"] = "compra"
                state["step"] = "waiting_for_detalle"
                return "🐷 ¿Qué animal compraste? (Ej: cerda, toro)"
            else:
                return "❓ Por favor, especifica: ¿nacimiento o compra?"

        elif tipo == "salida_animal":
            if "venta" in msg or "vendimos" in msg:
                state["data"]["subtipo"] = "venta"
                state["step"] = "waiting_for_detalle"
                return "🐄 ¿Qué animal vendiste? (Ej: cerdos, terneros)"
            elif "muerte" in msg or "murieron" in msg:
                state["data"]["subtipo"] = "muerte"
                state["step"] = "waiting_for_detalle"
                return "🐄 ¿Qué animal murió? (Ej: ternero, cerda)"
            else:
                return "❓ Por favor, especifica: ¿venta o muerte?"

    elif state["step"] == "waiting_for_detalle":
        state["data"]["detalle"] = mensaje
        state["step"] = "waiting_for_cantidad"
        return "🔢 ¿Cuántas unidades? (Ej: 3, 10) — o 'ninguna'"

    elif state["step"] == "waiting_for_cantidad":
        if msg in ["ninguna", "no", "0", "sin"]:
            state["data"]["cantidad"] = None
        else:
            try:
                state["data"]["cantidad"] = float(msg)
            except ValueError:
                return "❌ Por favor, escribe un número (Ej: 3) o 'ninguna'"
        state["step"] = "waiting_for_unidad"
        return "📦 ¿En qué unidad? (Ej: animales, cabezas, kg)"

    elif state["step"] == "waiting_for_unidad":
        state["data"]["unidad"] = mensaje
        tipo = state["data"]["tipo"]

        actividades_con_jornales = ["siembra", "produccion", "labor", "sanidad_animal"]
        
        if tipo in actividades_con_jornales:
            state["step"] = "waiting_for_jornales"
            return "👷 ¿Cuántos jornales se usaron? (Ej: 2) — o '0' si no aplica"
        elif tipo in ["ingreso_animal", "salida_animal", "gasto"]:
            state["step"] = "waiting_for_valor"
            return "💰 ¿Valor en COP? (Ej: 500000) — o '0' si no aplica"
        else:
            state["step"] = "waiting_for_lugar"
            return "📍 ¿Dónde fue? (Ej: corral A, lote 3)"

    elif state["step"] == "waiting_for_jornales":
        try:
            state["data"]["jornales"] = int(float(msg))
        except ValueError:
            return "❌ Por favor, escribe un número entero (Ej: 2) o '0'"
        state["step"] = "waiting_for_valor"
        return "💰 ¿Valor total de los jornales en COP? (Ej: 60000) — o '0' si no aplica"

    elif state["step"] == "waiting_for_valor":
        try:
            state["data"]["valor"] = float(msg)
        except ValueError:
            return "❌ Por favor, escribe un número (Ej: 60000)"
        state["step"] = "waiting_for_lugar"
        return "📍 ¿Dónde fue? (Ej: lote 3, corral A)"

    elif state["step"] == "waiting_for_lugar":
        state["data"]["lugar"] = mensaje
        state["step"] = "waiting_for_observacion"
        return "📝 ¿Observación? (Ej: aretes C-101 a C-103)\nEscribe 'fin' para guardar."

    elif state["step"] == "waiting_for_observacion":
        if msg in ["fin", "salir", "listo", "guardar", "0"]:
            state["data"]["observacion"] = ""
        else:
            state["data"]["observacion"] = mensaje

        tipo = state["data"]["tipo"]
        subtipo = state["data"]["subtipo"]
        detalle = state["data"]["detalle"]
        cantidad = state["data"]["cantidad"]
        valor = state["data"]["valor"]
        unidad = state["data"]["unidad"]
        lugar = state["data"]["lugar"]
        observacion = state["data"]["observacion"]
        jornales = state["data"]["jornales"]

        if tipo == "ingreso_animal":
            if subtipo == "nacimiento":
                datos = extraer_datos_animal(detalle + " " + (observacion or ""))
                registrar_animal(datos)
            guardar_registro(tipo, subtipo, detalle, lugar, cantidad, valor, unidad, observacion, jornales)

        elif tipo == "salida_animal":
            guardar_registro(tipo, subtipo, detalle, lugar, cantidad, valor, unidad, observacion, jornales)

        else:
            guardar_registro(tipo, tipo, detalle, lugar, cantidad, valor, unidad, observacion, jornales)

        del user_state[numero]
        return f"✅ ¡Registrado! {detalle}\n\n¡Gracias por usar Hacienda La Tática!"

    return "❌ Error interno. Intenta de nuevo."

# === 13. PROCESAR MENSAJE WHASTAPP ===
def procesar_mensaje_whatsapp(mensaje, remitente=None):
    print(f"🔍 [BOT] Procesando mensaje: '{mensaje}'")
    mensaje = mensaje.strip()
    if not mensaje:
        return "❌ Mensaje vacío."

    if mensaje.strip().lower() == "limpiar bd":
        if remitente == "whatsapp:+573143539351":
            return vaciar_tablas()
        else:
            return "❌ Acceso denegado."

    if mensaje.lower().startswith("estado animal "):
        arete = mensaje.split(" ", 2)[2].strip().upper()
        return consultar_estado_animal(arete)

    if mensaje.strip().lower() in ["ayuda", "help"]:
        return (
            "🌿 Bienvenido a Hacienda La Tática.\n\n"
            "Usa el menú o escribe libremente.\n"
            "Ej: 'vendimos 3 cerdos', 'nacieron 5 lechones'\n\n"
            "📊 Reportes: 'reporte semanal'\n"
            "🔍 Estado: 'estado animal C-101'\n"
            "🚪 Salir: 'fin' o '0'"
        )

    if mensaje.upper().startswith("REGISTRAR:"):
        partes = mensaje.split(":", 1)
        resto = partes[1].strip()
        campos = [c.strip() for c in resto.split(",")]
        if len(campos) < 4:
            return "❌ Usa: REGISTRAR: tipo, detalle, cantidad, valor, unidad, lugar, obs"

        tipo = campos[0].lower()
        if "cosecha" in tipo: tipo = "produccion"
        if "compra animal" in tipo: tipo = "ingreso_animal"
        if "venta animal" in tipo or "muerte" in tipo: tipo = "salida_animal"

        detalle = campos[1] if len(campos) > 1 else ""
        cantidad = float(campos[2]) if len(campos) > 2 and campos[2] not in ["", "ninguna"] else None
        valor = float(campos[3]) if len(campos) > 3 and campos[3] else 0.0
        unidad = campos[4] if len(campos) > 4 else ""
        lugar = campos[5].upper() if len(campos) > 5 else ""
        observacion = campos[6] if len(campos) > 6 else ""
        jornales = int(float(campos[7])) if len(campos) > 7 and campos[7] else 0

        guardar_registro(tipo, tipo, detalle, lugar, cantidad, valor, unidad, observacion, jornales)
        return f"✅ Registrado: {tipo} - {detalle}"

    if "reporte" in mensaje.lower():
        freq = "semanal"
        if "diario" in mensaje.lower(): freq = "diario"
        elif "mensual" in mensaje.lower(): freq = "mensual"
        elif "quincenal" in mensaje.lower(): freq = "quincenal"
        return generar_reporte(freq)

    if remitente:
        return iniciar_flujo_conversacional(remitente, mensaje)

    return "❌ No entendí. Escribe 'ayuda'."