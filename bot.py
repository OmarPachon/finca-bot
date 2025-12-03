# -*- coding: utf-8 -*-
"""
bot.py - Sistema de Registro Conversacional Multi-Finca
Versión FINAL: menús obligatorios + multi-finca + empleados + lógica completa
"""

import os
import psycopg2
import re
import datetime
from urllib.parse import urlparse

print("🔧 Iniciando bot.py (versión multi-finca con menús)...")

# === 1. CONEXIÓN A POSTGRESQL CON MIGRACIÓN AUTOMÁTICA ===
def inicializar_bd():
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            print("❌ DATABASE_URL no está definida. Configúrala en Render.")
            return False

        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()

        # Tablas principales
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fincas (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(100) UNIQUE NOT NULL,
                telefono_dueño VARCHAR(20) UNIQUE NOT NULL,
                suscripcion_activa BOOLEAN DEFAULT TRUE,
                vencimiento_suscripcion DATE DEFAULT (CURRENT_DATE + INTERVAL '30 days')
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS usuarios (
                id SERIAL PRIMARY KEY,
                telefono_whatsapp VARCHAR(20) UNIQUE NOT NULL,
                nombre VARCHAR(100),
                rol VARCHAR(20) NOT NULL CHECK (rol IN ('dueño', 'supervisor', 'trabajador')),
                finca_id INTEGER NOT NULL REFERENCES fincas(id) ON DELETE CASCADE
            )
        ''')

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
                fecha_registro DATE DEFAULT CURRENT_DATE,
                finca_id INTEGER REFERENCES fincas(id) ON DELETE SET NULL
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
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                finca_id INTEGER REFERENCES fincas(id) ON DELETE SET NULL,
                usuario_id INTEGER REFERENCES usuarios(id) ON DELETE SET NULL
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
                finca_id INTEGER REFERENCES fincas(id) ON DELETE SET NULL,
                FOREIGN KEY (id_externo) REFERENCES animales (id_externo)
            )
        ''')

        # Migración: asegurar columna 'jornales'
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
        print("✅ Base de datos lista (multi-finca + menús).")
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
        "resembramos", "sembramos", "plantamos"
    ],
    "produccion": [
        "cosechar", "cosecha", "recolectar", "cortar", "descacotar",
        "cosechamos", "recolectamos","producir", "producción","leche", "carne", "huevos", "litros", "kilos", "kg",
        "vendimos", "sacamos producto", "salieron","maíz", "papa", "arroz", "cacao", "café", "yuca", "plátano", "frijol", "trigo", "cebolla"
    ],
    "sanidad_animal": [
        "vacunar", "vacuna", "inyectar", "desparasitar", "purgar",
        "medicar", "tratamiento", "sanidad", "chequeo",
        "aftosa", "brucelosis", "desparasitamos", "vermífugo",
        "vacunamos", "inyectamos", "pastilla", "bolo", "curacion","cirugia"
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

# === 4. FUNCIONES DE SOPORTE ===
def obtener_usuario_por_whatsapp(telefono):
    try:
        database_url = os.environ.get("DATABASE_URL")
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT u.id, u.nombre, u.rol, u.finca_id, f.nombre AS finca_nombre, f.suscripcion_activa
                    FROM usuarios u
                    JOIN fincas f ON u.finca_id = f.id
                    WHERE u.telefono_whatsapp = %s
                """, (telefono,))
                row = cursor.fetchone()
                if row:
                    return {
                        "id": row[0],
                        "nombre": row[1],
                        "rol": row[2],
                        "finca_id": row[3],
                        "finca_nombre": row[4],
                        "suscripcion_activa": row[5]
                    }
    except Exception as e:
        print(f"❌ Error al buscar usuario: {e}")
    return None

def registrar_nueva_finca(nombre_finca, remitente):
    try:
        nombre_finca = nombre_finca.strip()
        if len(nombre_finca) < 3:
            return "❌ El nombre debe tener al menos 3 caracteres."

        database_url = os.environ.get("DATABASE_URL")
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM usuarios WHERE telefono_whatsapp = %s", (remitente,))
                if cursor.fetchone():
                    return "❌ Ya estás registrado en una finca."

                cursor.execute("""
                    INSERT INTO fincas (nombre, telefono_dueño)
                    VALUES (%s, %s)
                    RETURNING id
                """, (nombre_finca, remitente))
                finca_id = cursor.fetchone()[0]

                cursor.execute("""
                    INSERT INTO usuarios (telefono_whatsapp, nombre, rol, finca_id)
                    VALUES (%s, %s, 'dueño', %s)
                """, (remitente, "Dueño", finca_id))
            conn.commit()
        vencimiento = datetime.date.today() + datetime.timedelta(days=30)
        return (
            f"✅ ¡Finca '{nombre_finca}' registrada!\n"
            f"Tu suscripción es válida hasta el {vencimiento.strftime('%d/%m/%Y')}.\n\n"
            "Elige una opción para registrar actividades:\n"
            "1. 🌱 Siembra\n"
            "2. 🌾 Producción\n"
            "3. 💉 Sanidad animal\n"
            "4. 🐷 Ingreso de animales\n"
            "5. 🐄 Salida de animales\n"
            "6. 💰 Gasto\n"
            "7. 🛠️ Labor\n\n"
            "Escribe 'fin' para salir."
        )
    except psycopg2.IntegrityError as e:
        error_str = str(e)
        if "unique" in error_str.lower() and ("nombre" in error_str.lower() or "fincas_nombre" in error_str.lower()):
            return "❌ Ya existe una finca con ese nombre. Usa otro."
        else:
            return f"❌ Error de integridad: {error_str[:100]}"
    except Exception as e:
        import traceback
        error_msg = str(e)
        # Devuelve un extracto del error en WhatsApp (máx 160 caracteres)
        return f"❌ ERROR: {error_msg[:120]}"
def detectar_actividad(mensaje):
    mensaje = mensaje.lower()
    for actividad, palabras in SINONIMOS.items():
        for palabra in palabras:
            if palabra in mensaje:
                return actividad
    return "general"

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

def guardar_registro(tipo_actividad, accion, detalle, lugar=None, cantidad=None, valor=0, unidad=None, observacion=None, jornales=None, finca_id=None, usuario_id=None):
    print(f"🔍 GUARDANDO REGISTRO en finca {finca_id}: {tipo_actividad} | {detalle}")
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            print("❌ DATABASE_URL no está definida")
            return

        fecha = datetime.date.today().isoformat()
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO registros (fecha, tipo_actividad, accion, detalle, lugar, cantidad, valor, unidad, observacion, jornales, fecha_registro, finca_id, usuario_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (fecha, tipo_actividad, accion, detalle, lugar, cantidad, valor, unidad, observacion, jornales, datetime.datetime.now().isoformat(), finca_id, usuario_id))
            conn.commit()
        print(f"✅ REGISTRO GUARDADO en finca {finca_id}")
    except Exception as e:
        print(f"❌ ERROR AL GUARDAR REGISTRO: {e}")
        import traceback
        print(traceback.format_exc())

def generar_reporte(frecuencia="semanal", formato="texto", finca_id=None):
    if finca_id is None:
        return "❌ No se puede generar reporte sin finca."

    hoy = datetime.date.today()
    dias_map = {"diario": 1, "semanal": 7, "quincenal": 15, "mensual": 30}
    dias = dias_map.get(frecuencia, 7)
    inicio = hoy - datetime.timedelta(days=dias)
    periodo = f"Del {inicio.strftime('%d/%m')} al {hoy.strftime('%d/%m')}"

    try:
        database_url = os.environ.get("DATABASE_URL")
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT fecha, tipo_actividad, accion, detalle, lugar, cantidad, valor, unidad, observacion, jornales
                    FROM registros 
                    WHERE fecha >= %s AND finca_id = %s 
                    ORDER BY fecha, tipo_actividad
                """, (inicio.isoformat(), finca_id))
                registros = cursor.fetchall()
        print(f"📊 Reporte cargado: {len(registros)} registros desde {inicio} (finca {finca_id})")
    except Exception as e:
        return f"❌ Error al leer la base de datos: {e}"

    if formato == "texto":
        lines = [f"📅 REPORTE {frecuencia.upper()}", periodo, ""]

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
                if row[9] or row[6]:
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

        # SECCIÓN DE JORNALES
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

def vaciar_tablas():
    try:
        database_url = os.environ.get("DATABASE_URL")
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

# === 5. FLUJO CONVERSACIONAL COMPLETO (tu lógica original, adaptada) ===
def iniciar_flujo_conversacional_existente(mensaje, user_key, state):
    msg = mensaje.strip().lower()

    if state["step"] == "waiting_for_category":
        if msg in ["fin", "salir", "cancelar", "no", "nada"]:
            if user_key in user_state:
                del user_state[user_key]
            return "✅ ¡Gracias por usar Finca Digital! Vuelve cuando necesites."

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
                "🌿 Elige una opción:\n"
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
        state["completed"] = True
        return "¡Listo para guardar!"

    return "❌ Error interno. Intenta de nuevo."

def iniciar_flujo_conversacional_con_finca(mensaje, usuario_info):
    user_key = usuario_info["id"]
    if user_key not in user_state:
        user_state[user_key] = {
            "step": "waiting_for_category",
            "data": {
                "tipo": "", "detalle": "", "cantidad": None, "valor": 0,
                "unidad": "", "lugar": "", "observacion": "", "jornales": 0, "subtipo": ""
            },
            "usuario_info": usuario_info
        }

    state = user_state[user_key]
    respuesta = iniciar_flujo_conversacional_existente(mensaje, user_key, state)

    if state.get("completed"):
        datos = state["data"]
        tipo = datos["tipo"]
        subtipo = datos["subtipo"]
        detalle = datos["detalle"]
        cantidad = datos["cantidad"]
        valor = datos["valor"]
        unidad = datos["unidad"]
        lugar = datos["lugar"]
        observacion = datos["observacion"]
        jornales = datos["jornales"]
        finca_id = usuario_info["finca_id"]
        usuario_id = usuario_info["id"]

        if tipo == "ingreso_animal" and subtipo == "nacimiento":
            datos_animal = extraer_datos_animal(detalle + " " + (observacion or ""))
            if datos_animal.get("id_externo"):
                try:
                    database_url = os.environ.get("DATABASE_URL")
                    with psycopg2.connect(database_url) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute('''
                                INSERT INTO animales (especie, id_externo, marca_o_arete, categoria, peso, corral, estado, finca_id)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (id_externo) DO NOTHING
                            ''', (
                                datos_animal.get("especie"),
                                datos_animal.get("id_externo"),
                                datos_animal.get("marca_o_arete"),
                                datos_animal.get("categoria"),
                                datos_animal.get("peso"),
                                datos_animal.get("corral"),
                                "activo",
                                finca_id
                            ))
                        conn.commit()
                except Exception as e:
                    print(f"❌ Error al registrar animal con finca: {e}")

        guardar_registro(
            tipo, 
            subtipo if tipo in ["ingreso_animal", "salida_animal"] else tipo,
            detalle, lugar, cantidad, valor, unidad, observacion, jornales,
            finca_id=finca_id,
            usuario_id=usuario_id
        )

        if user_key in user_state:
            del user_state[user_key]
        return f"✅ ¡Registrado en {usuario_info['finca_nombre']}! {detalle}"

    return respuesta

# === 6. ENTRADA PRINCIPAL: MENÚ OBLIGATORIO ===
def procesar_mensaje_whatsapp(mensaje, remitente=None):
    print(f"🔍 [BOT] Procesando mensaje: '{mensaje}' de {remitente}")
    if not remitente:
        return "❌ Error: remitente no identificado."

    mensaje = mensaje.strip()
    if not mensaje:
        return "❌ Mensaje vacío."

    # Si ya está en proceso de registrar finca (segunda respuesta)
    if remitente in user_state and user_state[remitente].get("esperando_nombre_finca"):
        nombre_finca = mensaje
        del user_state[remitente]
        return registrar_nueva_finca(nombre_finca, remitente)

    # Ver si ya está registrado
    usuario_info = obtener_usuario_por_whatsapp(remitente)

    if not usuario_info:
        # Nuevo usuario: solo puede registrar finca
        if mensaje.lower() in ["8", "finca", "registrar", "hola", "hi", "buenos días", "buenas", "menu", "ayuda"]:
            user_state[remitente] = {"esperando_nombre_finca": True}
            return (
                "🏡 Bienvenido a Finca Digital.\n\n"
                "Para comenzar, ¿cómo se llama tu finca?\n"
                "(Ej: Hacienda La Tática)"
            )
        else:
            return (
                "🏡 Bienvenido.\n\n"
                "8. 🏡 Registrar mi finca\n\n"
                "Escribe '8' para comenzar."
            )

    if not usuario_info["suscripcion_activa"]:
        return "🔒 Tu suscripción ha expirado. Contacta al administrador."

    # Usuario existente: procesar mensaje en su contexto
    if mensaje.lower().startswith("estado animal "):
        arete = mensaje.split(" ", 2)[2].strip().upper()
        return consultar_estado_animal(arete)

    if "reporte" in mensaje.lower():
        freq = "semanal"
        if "diario" in mensaje.lower(): freq = "diario"
        elif "mensual" in mensaje.lower(): freq = "mensual"
        elif "quincenal" in mensaje.lower(): freq = "quincenal"
        return generar_reporte(frecuencia=freq, formato="texto", finca_id=usuario_info["finca_id"])

    if mensaje.lower().startswith("exportar reporte"):
        return "📎 El reporte en Excel estará disponible pronto en tu WhatsApp."

    if mensaje.strip().lower() in ["ayuda", "help", "menu", "hola"]:
        return (
            f"🌿 Bienvenido a {usuario_info['finca_nombre']}.\n\n"
            "Elige una opción:\n"
            "1. 🌱 Siembra\n"
            "2. 🌾 Producción\n"
            "3. 💉 Sanidad\n"
            "4. 🐷 Ingreso animal\n"
            "5. 🐄 Salida animal\n"
            "6. 💰 Gasto\n"
            "7. 🛠️ Labor\n\n"
            "Escribe 'fin' para salir."
        )

    return iniciar_flujo_conversacional_con_finca(mensaje, usuario_info)