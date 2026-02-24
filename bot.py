# -*- coding: utf-8 -*-
"""
bot.py - Sistema de Registro Conversacional Multi-Finca
Versión FINAL COMERCIAL con reporte entre fechas (año actual) + salida_animal
"""
import os
import psycopg2
import re
import datetime
from urllib.parse import urlparse
print("🔧 Iniciando bot.py (versión con salida_animal)...")

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
            telefono_dueño VARCHAR(25) UNIQUE NOT NULL,
            suscripcion_activa BOOLEAN DEFAULT FALSE,
            vencimiento_suscripcion DATE,
            clave_secreta TEXT UNIQUE
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            telefono_whatsapp VARCHAR(25) UNIQUE NOT NULL,
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
        print("✅ Base de datos lista (multi-finca + suscripción).")
        return True
    except Exception as e:
        print(f"❌ Error al conectar con PostgreSQL: {e}")
        return False

try:
    BD_OK = inicializar_bd()
except Exception as e:
    print(f"❌ Error crítico al inicializar BD: {e}")
    BD_OK = False

# === 2. PALABRAS CLAVE PARA ANIMALES ===
PORCINO_PALABRAS = ["cerdo", "lechón", "cerda", "verraco", "lechon", "lechones", "cochino"]
BOVINO_PALABRAS = ["vaca", "toro", "ternero", "ternera", "novillo", "novilla", "buey", "ganado"]
CATEGORIAS_VALIDAS = ["lechón", "cerda", "verraco", "ceba", "toro", "ternero", "ternera", "novillo", "vaquilla", "engorda", "lechera"]

# === 3. ESTADO DEL USUARIO ===
user_state = {}

# === 4. FUNCIONES DE SOPORTE ===
def obtener_usuario_por_whatsapp(telefono):
    try:
        database_url = os.environ.get("DATABASE_URL")
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT u.id, u.nombre, u.rol, u.finca_id, f.nombre AS finca_nombre, f.suscripcion_activa, f.vencimiento_suscripcion
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
                        "suscripcion_activa": row[5],
                        "vencimiento_suscripcion": row[6]
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
                INSERT INTO fincas (nombre, telefono_dueño, suscripcion_activa, vencimiento_suscripcion)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """, (nombre_finca, remitente, False, None))
                finca_id = cursor.fetchone()[0]
                cursor.execute("""
                INSERT INTO usuarios (telefono_whatsapp, nombre, rol, finca_id)
                VALUES (%s, %s, 'dueño', %s)
                """, (remitente, "Dueño", finca_id))
                conn.commit()
                return (
                    f"🏡 ¡Finca '{nombre_finca}' registrada!\n"
                    "💳 **Para activarla, debes suscribirte mensualmente.**\n"
                    "**Valor:** $100.000 COP/mes\n"
                    "**Incluye:** Tu número (como dueño) + hasta 3 empleados para registrar labores.\n"
                    "**Nequi:** 314 353 9351 (Omar Pachón)\n"
                    "📲 **Al realizar el pago, envía el comprobante y los números de tus empleados:**\n"
                    "- Máximo 3 números de WhatsApp\n"
                    "- *(Tu número ya está registrado como dueño — no lo incluyas)*\n"
                    "- Formato correcto:\n"
                    "  • whatsapp:+573101234567\n"
                    "  • whatsapp:+573119876543\n"
                    "✅ Yo activaré a todos en menos de 1 hora."
                )
    except psycopg2.IntegrityError as e:
        if "unique" in str(e).lower() and "nombre" in str(e).lower():
            return "❌ Ya existe una finca con ese nombre. Usa otro."
        return f"❌ Error de integridad: {str(e)[:100]}"
    except Exception as e:
        error_msg = str(e)
        return f"❌ ERROR: {error_msg[:120]}"

def extraer_datos_animal(mensaje):
    datos = {"especie": None, "id_externo": None, "marca_o_arete": None, "categoria": None, "corral": None, "peso": None}
    mensaje = mensaje.lower()
    if any(p in mensaje for p in PORCINO_PALABRAS):
        datos["especie"] = "porcino"
    elif any(p in mensaje for p in BOVINO_PALABRAS):
        datos["especie"] = "bovino"
    arete = re.search(r"(?:arete|chapeta)\s+(\d+)", mensaje)
    if arete:
        num = arete.group(1)
        datos["marca_o_arete"] = num
        datos["id_externo"] = f"C-{num}" if datos["especie"] == "porcino" else f"V-{num}"
    marca = re.search(r"marca\s+([a-z0-9-]+)", mensaje, re.IGNORECASE)
    if marca:
        cod = marca.group(1).upper()
        datos["marca_o_arete"] = cod
        prefijo = "C-" if datos["especie"] == "porcino" else "V-M-"
        datos["id_externo"] = f"{prefijo}{cod}"
    for cat in CATEGORIAS_VALIDAS:
        if cat in mensaje:
            datos["categoria"] = cat
            break
    corral = re.search(r"(?:corral|lugar)\s+([a-z0-9]+)", mensaje, re.IGNORECASE)
    if corral: datos["corral"] = corral.group(1).upper()
    peso = re.search(r"peso\s*(\d+(?:\.\d+)?)\s*(kg|kilo|kilos)", mensaje)
    if peso: datos["peso"] = float(peso.group(1))
    return datos

def actualizar_peso_animal(marca_o_arete, nuevo_peso, finca_id):
    """Actualiza el peso de un animal si existe en la finca."""
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                UPDATE animales
                SET peso = %s
                WHERE (marca_o_arete = %s OR id_externo = %s)
                AND finca_id = %s
                """, (nuevo_peso, marca_o_arete, marca_o_arete, finca_id))
                if cursor.rowcount > 0:
                    print(f"✅ Peso actualizado: {marca_o_arete} → {nuevo_peso} kg")
                else:
                    print(f"ℹ️ No se encontró el animal {marca_o_arete} para actualizar peso.")
                conn.commit()
    except Exception as e:
        print(f"❌ Error al actualizar peso: {e}")

def guardar_en_salud_animal(id_externo, tipo, tratamiento, observacion, finca_id):
    """Guarda un registro de sanidad en la tabla salud_animal."""
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return
        fecha = datetime.date.today().isoformat()
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                INSERT INTO salud_animal (id_externo, tipo, tratamiento, fecha, observacion, finca_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ''', (id_externo, tipo, tratamiento, fecha, observacion, finca_id))
                conn.commit()
                print(f"✅ Sanidad guardada para {id_externo}")
    except Exception as e:
        print(f"❌ Error al guardar en salud_animal: {e}")

def generar_inventario_animales(finca_id):
    """Genera un resumen del inventario de animales activos en la finca."""
    try:
        database_url = os.environ.get("DATABASE_URL")
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT especie, marca_o_arete, categoria, peso, corral
                FROM animales
                WHERE finca_id = %s AND estado = 'activo'
                ORDER BY especie, marca_o_arete
                """, (finca_id,))
                animales = cursor.fetchall()
                if not animales:
                    return "📋 No hay animales activos registrados en esta finca."
                bovinos = []
                porcinos = []
                otros = []
                for esp, marca, cat, peso, corral in animales:
                    linea = f"• {marca}"
                    if cat:
                        linea += f" – {cat}"
                    if peso:
                        linea += f" – {peso} kg"
                    if corral:
                        linea += f" – {corral}"
                    if esp == "bovino":
                        bovinos.append(linea)
                    elif esp == "porcino":
                        porcinos.append(linea)
                    else:
                        otros.append(linea)
                lines = [
                    "📋 INVENTARIO DE ANIMALES ACTIVOS",
                    f"Fecha: {datetime.date.today().strftime('%d/%b/%Y')}",
                    ""
                ]
                if bovinos:
                    lines.append(f"🐮 BOVINOS ({len(bovinos)})")
                    lines.extend(bovinos)
                    lines.append("")
                if porcinos:
                    lines.append(f"🐷 PORCINOS ({len(porcinos)})")
                    lines.extend(porcinos)
                    lines.append("")
                if otros:
                    lines.append(f"🦘 OTROS ({len(otros)})")
                    lines.extend(otros)
                    lines.append("")
                lines.append(f"✅ Total: {len(animales)} animales activos")
                return "\n".join(lines)
    except Exception as e:
        print(f"❌ Error al generar inventario: {e}")
        return "❌ No se pudo cargar el inventario de animales."

def guardar_registro(tipo_actividad, accion, detalle, lugar=None, cantidad=None, valor=0, unidad=None, observacion=None, jornales=None, finca_id=None, usuario_id=None, mensaje_completo=None):
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
                # === EXTRAER PESO Y MARCA DE CUALQUIER MENSAJE ===
                if mensaje_completo and finca_id:
                    peso_match = re.search(r"peso\s*(\d+(?:\.\d+)?)\s*(?:kg|kilo|kilos)", mensaje_completo, re.IGNORECASE)
                    if peso_match:
                        nuevo_peso = float(peso_match.group(1))
                        marca_match = re.search(r"(?:marca|arete|chapeta)\s+([a-z0-9-]+)", mensaje_completo, re.IGNORECASE)
                        if marca_match:
                            marca_o_arete = marca_match.group(1).upper()
                            actualizar_peso_animal(marca_o_arete, nuevo_peso, finca_id)
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
        total_ingresos = 0
        total_gastos = 0
        total_jornales_valor = 0
        produccion_total = [r for r in registros if r[1] == "produccion"]
        gastos = [r for r in registros if r[1] == "gasto"]
        actividades_con_jornales = [r for r in registros if r[9] and r[9] > 0]
        for row in produccion_total:
            if row[6] > 0:
                total_ingresos += row[6]
        for row in gastos:
            if row[6] > 0:
                total_gastos += row[6]
        for row in actividades_con_jornales:
            if row[6] > 0:
                total_jornales_valor += row[6]
        lines.append("📊 RESUMEN FINANCIERO")
        lines.append(f"• Ingresos: ${total_ingresos:,.0f}")
        lines.append(f"• Gastos: ${total_gastos:,.0f}")
        lines.append(f"• Jornales: ${total_jornales_valor:,.0f}")
        balance = total_ingresos - total_gastos - total_jornales_valor
        lines.append(f"• Balance estimado: ${balance:,.0f}")
        lines.append("")
        if produccion_total:
            vegetal = []
            animal = []
            cultivos = {"maíz", "papa", "arroz", "cacao", "café", "yuca", "plátano", "frijol", "citricos", "cebolla","fruta"}
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
                    if row[8]: desc += f". Obs: {row[8]}"
                    lines.append(desc)
                lines.append("")
        if gastos:
            lines.append("💰 GASTOS")
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
            if total_gastos > 0:
                lines.append(f"→ **TOTAL GASTOS: ${total_gastos:,.0f}**")
                lines.append("")
        if total_jornales_valor > 0:
            lines.append("👷 COSTO TOTAL DE JORNALES")
            lines.append(f"→ **${total_jornales_valor:,.0f}**")
            lines.append("")
        otras_actividades = [r for r in registros if r[1] not in ["produccion", "gasto"]]
        if otras_actividades:
            lines.append("📝 OTRAS ACTIVIDADES")
            for row in otras_actividades:
                tipo = row[1].replace("_", " ").title()
                desc = f"• {tipo}: {row[3] or 'actividad'}"
                if row[4]: desc += f" en {row[4]}"
                if row[5] and row[7]:
                    desc += f" ({row[5]} {row[7]})"
                if row[9]:
                    desc += f" ({row[9]} jornales)"
                if row[8]:
                    desc += f". Obs: {row[8]}"
                lines.append(desc)
            lines.append("")
        lines.append("✅ Todo bajo control. ¡Buen trabajo!")
        return "\n".join(lines)
    return registros

def generar_reporte_personalizado(fecha_inicio, fecha_fin, finca_id=None):
    if finca_id is None:
        return "❌ No se puede generar reporte sin finca."
    try:
        database_url = os.environ.get("DATABASE_URL")
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT fecha, tipo_actividad, accion, detalle, lugar, cantidad, valor, unidad, observacion, jornales
                FROM registros
                WHERE fecha BETWEEN %s AND %s AND finca_id = %s
                ORDER BY fecha
                """, (fecha_inicio.isoformat(), fecha_fin.isoformat(), finca_id))
                registros = cursor.fetchall()
                print(f"📊 Reporte personalizado: {len(registros)} registros del {fecha_inicio} al {fecha_fin}")
    except Exception as e:
        return f"❌ Error al leer la base de datos: {e}"
    if not registros:
        return f"⚠️ No hay actividades registradas del {fecha_inicio.strftime('%d/%m')} al {fecha_fin.strftime('%d/%m')}."
    lines = [
        f"📅 REPORTE PERSONALIZADO",
        f"Del {fecha_inicio.strftime('%d/%m')} al {fecha_fin.strftime('%d/%m')}",
        ""
    ]
    total_ingresos = 0
    total_gastos = 0
    total_jornales_valor = 0
    produccion_total = [r for r in registros if r[1] == "produccion"]
    gastos = [r for r in registros if r[1] == "gasto"]
    actividades_con_jornales = [r for r in registros if r[9] and r[9] > 0]
    for row in produccion_total:
        if row[6] > 0:
            total_ingresos += row[6]
    for row in gastos:
        if row[6] > 0:
            total_gastos += row[6]
    for row in actividades_con_jornales:
        if row[6] > 0:
            total_jornales_valor += row[6]
    lines.append("📊 RESUMEN FINANCIERO")
    lines.append(f"• Ingresos: ${total_ingresos:,.0f}")
    lines.append(f"• Gastos: ${total_gastos:,.0f}")
    lines.append(f"• Jornales: ${total_jornales_valor:,.0f}")
    balance = total_ingresos - total_gastos - total_jornales_valor
    lines.append(f"• Balance estimado: ${balance:,.0f}")
    lines.append("")
    if produccion_total:
        vegetal = []
        animal = []
        cultivos = {"maíz", "papa", "arroz", "cacao", "café", "yuca", "plátano", "frijol", "trigo", "cebolla","frutas","citricos"}
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
                if row[8]: desc += f". Obs: {row[8]}"
                lines.append(desc)
            lines.append("")
    if gastos:
        lines.append("💰 GASTOS")
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
        if total_gastos > 0:
            lines.append(f"→ **TOTAL GASTOS: ${total_gastos:,.0f}**")
            lines.append("")
    if total_jornales_valor > 0:
        lines.append("👷 COSTO TOTAL DE JORNALES")
        lines.append(f"→ **${total_jornales_valor:,.0f}**")
        lines.append("")
    otras_actividades = [r for r in registros if r[1] not in ["produccion", "gasto"]]
    if otras_actividades:
        lines.append("📝 OTRAS ACTIVIDADES")
        for row in otras_actividades:
            tipo = row[1].replace("_", " ").title()
            desc = f"• {tipo}: {row[3] or 'actividad'}"
            if row[4]: desc += f" en {row[4]}"
            if row[5] and row[7]:
                desc += f" ({row[5]} {row[7]})"
            if row[9]:
                desc += f" ({row[9]} jornales)"
            if row[8]:
                desc += f". Obs: {row[8]}"
            lines.append(desc)
        lines.append("")
    lines.append("✅ Todo bajo control. ¡Buen trabajo!")
    return "\n".join(lines)

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
                """, (arete.strip().upper(), arete.strip().upper()))
                row = cursor.fetchone()
                if not row:
                    return f"❌ No encontré ningún animal con marca o arete '{arete}'."
                especie, estado, peso, corral, fecha_reg, obs = row
                cursor.execute("""
                SELECT tipo, tratamiento, fecha, observacion
                FROM salud_animal
                WHERE id_externo = (
                    SELECT id_externo FROM animales
                    WHERE marca_o_arete = %s OR id_externo = %s
                    LIMIT 1
                )
                ORDER BY fecha DESC
                """, (arete.strip().upper(), arete.strip().upper()))
                historial = cursor.fetchall()
                icono = "🐮" if especie == "bovino" else "🐷" if especie == "porcino" else "🦘"
                respuesta = [
                    f"{icono} ANIMAL {arete.strip().upper()} ({especie})",
                    f"• Estado: {estado}",
                    f"• Peso: {peso or 'No registrado'} kg",
                    f"• Corral: {corral or 'No asignado'}",
                    f"• Registrado: {fecha_reg}",
                    f"• Observaciones: {obs or 'Sin notas'}",
                    ""
                ]
                if historial:
                    respuesta.append("💉 HISTORIAL DE SANIDAD")
                    for tipo, tratamiento, fecha, observacion in historial:
                        tratamiento_txt = tratamiento if tratamiento else tipo.title()
                        desc_linea = f"• {tratamiento_txt} – {fecha}"
                        if observacion:
                            desc_linea += f" – {observacion}"
                        respuesta.append(desc_linea)
                else:
                    respuesta.append("💉 Sin registros de sanidad")
                return "\n".join(respuesta)
    except Exception as e:
        return "❌ Error al consultar el animal. Inténtalo más tarde."

# === 5. FLUJO CONVERSACIONAL COMPLETO ===
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
            return "🌱 ¿Qué sembraste? (Ej: maíz, cacao, cafe)"
        elif msg in ["2", "produccion", "cosecha", "leche", "carne"]:
            state["data"]["tipo"] = "produccion"
            state["step"] = "waiting_for_detalle"
            return "🌾 ¿Qué produjiste o cosechaste? (Ej: cacao, cafe, leche, huevos)"
        elif msg in ["3", "sanidad", "vacuna", "desparasitar"]:
            state["data"]["tipo"] = "sanidad_animal"
            state["step"] = "waiting_for_detalle"
            return "💉 ¿Fue vacuna o desparasitación, Inseminación, monta?"
        elif msg in ["4", "ingreso", "compra", "nacimiento", "inventario"]:
            state["data"]["tipo"] = "ingreso_animal"
            state["step"] = "waiting_for_subtipo"
            return "❓ ¿Es por nacimiento, compra o inventario inicial?"
        elif msg in ["5", "salida", "venta", "muerte"]:
            state["data"]["tipo"] = "salida_animal"
            state["step"] = "waiting_for_subtipo"
            return "🐄 ¿Es por venta o muerte de animales?"
        elif msg in ["6", "gasto", "pagamos", "compra"]:
            state["data"]["tipo"] = "gasto"
            state["step"] = "waiting_for_detalle"
            return "💰 ¿Qué gastaste? (Ej: medicina, jornales, insumos)"
        elif msg in ["7", "labor", "macaneo", "abono","cerca"]:
            state["data"]["tipo"] = "labor"
            state["step"] = "waiting_for_detalle"
            return "🛠️ ¿Qué labor hiciste? (Ej: macaneo, abono, corte, reparacion)"
        else:
            return (
                "🌿 Elige una opción:\n"
                "1. 🌱 Siembra\n"
                "2. 🌾 Producción (cosecha, leche, carne)\n"
                "3. 💉 Sanidad animal\n"
                "4. 🐷 Ingreso de animales (nacimientos, compras)\n"
                "5. 🐄 Salida de animales (ventas, muertes)\n"
                "6. 💰 Gasto\n"
                "7. 🛠️ Labor\n"
                "Escribe 'fin' o '0' para salir."
            )
    elif state["step"] == "waiting_for_subtipo":
        tipo = state["data"]["tipo"]
        if tipo == "ingreso_animal":
            if "nac" in msg or "parto" in msg:
                state["data"]["subtipo"] = "nacimiento"
                state["step"] = "waiting_for_detalle"
                return "🐷 ¿Qué tipo de animal nació? (Ej: lechón, ternera, ternero)"
            elif "compra" in msg or "compramos" in msg:
                state["data"]["subtipo"] = "compra"
                state["step"] = "waiting_for_detalle"
                return "🐷 ¿Qué animal compraste? (Ej: vaca, ternero, cerdo, cerda, toro)"
            elif "inventario" in msg or "existencia" in msg or "inicial" in msg:
                state["data"]["subtipo"] = "inventario_inicial"
                state["step"] = "waiting_for_detalle"
                return "📦 ¿Qué animales ya tenías en la finca? (Ej: 5 terneras, 3 cerdas)"
            else:
                return "❓ Por favor, especifica: ¿nacimiento, compra o inventario inicial?"
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
        actividades_con_jornales = ["siembra", "labor", "sanidad_animal"]
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
        return "📝 ¿Observación? (Ej: marca D-01, D-03, T105)\nEscribe 'fin' para guardar."
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
        mensaje_completo = f"{detalle} {lugar} {observacion}".strip()
        
        # === MANEJO ESPECIAL DE INGRESO DE ANIMALES ===
        if tipo == "ingreso_animal":
            if subtipo in ["nacimiento", "compra", "inventario_inicial"]:
                texto_completo = f"{detalle} {observacion}"
                marcas = []
                for match in re.finditer(r"marca\s+([A-Za-z0-9-]+)", texto_completo):
                    marca_capturada = match.group(1).upper()
                    marcas.append(marca_capturada)
                    print(f"🔍 Marca detectada: {marca_capturada}")
                print(f"📊 Total marcas detectadas: {len(marcas)}")
                especie = "bovino"
                if any(p in detalle.lower() for p in ["lechón", "cerda", "verraco", "ceba", "cerdo", "chancho"]):
                    especie = "porcino"
                elif any(p in detalle.lower() for p in ["ternero", "ternera", "toro", "vaca", "novillo", "novilla"]):
                    especie = "bovino"
                animales_registrados = 0
                errores_registro = []
                for marca in marcas:
                    try:
                        prefijo = "C-" if especie == "porcino" else "V-M-"
                        id_externo = f"{prefijo}{marca}"
                        categoria = None
                        if "lechón" in detalle.lower():
                            categoria = "lechón"
                        elif "cerda" in detalle.lower():
                            categoria = "cerda"
                        elif "ternera" in detalle.lower():
                            categoria = "ternera"
                        elif "ternero" in detalle.lower():
                            categoria = "ternero"
                        elif "toro" in detalle.lower():
                            categoria = "toro"
                        elif "vaca" in detalle.lower():
                            categoria = "vaca"
                        peso_valor = None
                        pattern = r"marca\s+" + re.escape(marca) + r".*?peso\s*(\d+(?:\.\d+)?)\s*kg"
                        peso_match = re.search(pattern, texto_completo, re.IGNORECASE)
                        if peso_match:
                            peso_valor = float(peso_match.group(1))
                        database_url = os.environ.get("DATABASE_URL")
                        with psycopg2.connect(database_url) as conn:
                            with conn.cursor() as cursor:
                                cursor.execute('''
                                INSERT INTO animales (especie, id_externo, marca_o_arete, categoria, corral, estado, peso, finca_id)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (id_externo) DO UPDATE
                                SET peso = EXCLUDED.peso, estado = EXCLUDED.estado, categoria = EXCLUDED.categoria
                                ''', (
                                    especie,
                                    id_externo,
                                    marca,
                                    categoria,
                                    lugar,
                                    "activo",
                                    peso_valor,
                                    finca_id
                                ))
                                conn.commit()
                                animales_registrados += 1
                                print(f"✅ Animal registrado: {id_externo} ({marca})")
                    except Exception as e:
                        errores_registro.append(f"{marca}: {str(e)[:50]}")
                        print(f"❌ Error al registrar animal {marca}: {e}")
                guardar_registro(
                    tipo,
                    subtipo,
                    f"{detalle} ({animales_registrados} animales)",
                    lugar, cantidad, valor, unidad, observacion, jornales,
                    finca_id=finca_id,
                    usuario_id=usuario_id,
                    mensaje_completo=mensaje_completo
                )
                respuesta_final = f"✅ ¡Registrado en {usuario_info['finca_nombre']}!"
                if animales_registrados > 0:
                    respuesta_final += f"\n🐮 {animales_registrados} animales guardados en inventario."
                    respuesta_final += f"\n📋 Marcas: {', '.join(marcas)}"
                else:
                    respuesta_final += "\n⚠️ No se detectaron marcas válidas."
                    respuesta_final += "\n💡 Formato correcto: 'marca LG01, marca LG02'"
                if errores_registro:
                    respuesta_final += f"\n❌ Errores: {len(errores_registro)}"
                if user_key in user_state:
                    del user_state[user_key]
                return respuesta_final
        
        # === MANEJO ESPECIAL DE SALIDA DE ANIMALES (NUEVO - CORREGIDO) ===
        elif tipo == "salida_animal":
            if subtipo in ["venta", "muerte"]:
                texto_completo = f"{detalle} {observacion}"
                marcas = []
                for match in re.finditer(r"marca\s+([A-Za-z0-9-]+)", texto_completo):
                    marca_capturada = match.group(1).upper()
                    marcas.append(marca_capturada)
                    print(f"🔍 Marca detectada para venta: {marca_capturada}")
                print(f"📊 Total marcas para venta: {len(marcas)}")
                
                animales_vendidos = 0
                errores_venta = []
                for marca in marcas:
                    try:
                        database_url = os.environ.get("DATABASE_URL")
                        with psycopg2.connect(database_url) as conn:
                            with conn.cursor() as cursor:
                                # Buscar el animal
                                cursor.execute("""
                                SELECT id_externo FROM animales
                                WHERE (marca_o_arete = %s OR id_externo LIKE %s)
                                AND finca_id = %s
                                AND estado = 'activo'
                                """, (marca, f"%{marca}%", finca_id))
                                row = cursor.fetchone()
                                
                                if row:
                                    id_externo = row[0]
                                    # Actualizar estado a 'vendido'
                                    cursor.execute("""
                                    UPDATE animales 
                                    SET estado = 'vendido',
                                        observaciones = %s
                                    WHERE id_externo = %s
                                    """, (f"Vendido: {detalle} - {observacion}", id_externo))
                                    conn.commit()
                                    animales_vendidos += 1
                                    print(f"✅ Animal {marca} marcado como vendido")
                                else:
                                    errores_venta.append(f"{marca}: no encontrado o ya vendido")
                                    print(f"⚠️ Animal {marca} no encontrado o ya está vendido")
                    except Exception as e:
                        errores_venta.append(f"{marca}: {str(e)[:50]}")
                        print(f"❌ Error al actualizar venta para {marca}: {e}")
                
                # Guardar registro de la actividad (siempre, haya marcas o no)
                guardar_registro(
                    tipo,
                    subtipo,
                    f"{detalle} ({animales_vendidos} animales)",
                    lugar, cantidad, valor, unidad, observacion, jornales,
                    finca_id=finca_id,
                    usuario_id=usuario_id,
                    mensaje_completo=mensaje_completo
                )
                
                # ✅ MENSAJE DE CONFIRMACIÓN DETALLADO
                respuesta_final = f"✅ ¡Registrado en {usuario_info['finca_nombre']}!"
                if animales_vendidos > 0:
                    respuesta_final += f"\n💸 {animales_vendidos} animales marcados como vendidos."
                    respuesta_final += f"\n📋 Marcas: {', '.join(marcas)}"
                else:
                    respuesta_final += "\n⚠️ No se detectaron marcas válidas."
                    respuesta_final += "\n💡 Formato correcto: 'marca LG01, marca LG02'"
                if errores_venta:
                    respuesta_final += f"\n❌ Errores: {len(errores_venta)}"
                if user_key in user_state:
                    del user_state[user_key]
                return respuesta_final
        
        # === MANEJO ESPECIAL DE SANIDAD ANIMAL ===
        elif tipo == "sanidad_animal":
            guardar_registro(
                tipo, tipo,
                detalle, lugar, cantidad, valor, unidad, observacion, jornales,
                finca_id=finca_id,
                usuario_id=usuario_id,
                mensaje_completo=mensaje_completo
            )
            texto = mensaje_completo.lower()
            marcas_encontradas = re.findall(r"marca\s+([a-z0-9-]+)", texto, re.IGNORECASE)
            marcas_encontradas = [m.upper() for m in marcas_encontradas]
            parejas = {}
            for match in re.finditer(r"marca\s+([a-z0-9-]+)\s+(?:peso\s+(\d+(?:\.\d+)?)\s*kg)?", texto, re.IGNORECASE):
                marca = match.group(1).upper()
                peso = float(match.group(2)) if match.group(2) else None
                parejas[marca] = peso
            if not parejas and marcas_encontradas:
                for marca in marcas_encontradas:
                    parejas[marca] = None
            for marca, peso in parejas.items():
                try:
                    database_url = os.environ.get("DATABASE_URL")
                    with psycopg2.connect(database_url) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                            SELECT id_externo FROM animales
                            WHERE (marca_o_arete = %s OR id_externo LIKE %s)
                            AND finca_id = %s
                            """, (marca, f"%{marca}%", finca_id))
                            row = cursor.fetchone()
                            if row:
                                id_externo = row[0]
                                detalle_lower = detalle.lower()
                                if any(kw in detalle_lower for kw in ["vacuna", "aftosa", "brucelosis"]):
                                    tipo_sanidad = "vacuna"
                                elif any(kw in detalle_lower for kw in ["desparasit", "garrapata", "gusano"]):
                                    tipo_sanidad = "desparasitación"
                                elif any(kw in detalle_lower for kw in ["monta", "insemin", "preñez", "celo", "reproduccion", "reproducción", "inseminacion", "servicio"]):
                                    tipo_sanidad = "reproducción"
                                else:
                                    tipo_sanidad = "sanidad"
                                guardar_en_salud_animal(id_externo, tipo_sanidad, detalle, observacion, finca_id)
                                if peso is not None:
                                    actualizar_peso_animal(marca, peso, finca_id)
                except Exception as e:
                    print(f"❌ Error al registrar sanidad para {marca}: {e}")
            if user_key in user_state:
                del user_state[user_key]
            return f"✅ ¡Registrado en {usuario_info['finca_nombre']}! {detalle}"
        
        # === OTROS TIPOS DE REGISTRO ===
        else:
            guardar_registro(
                tipo,
                subtipo if tipo in ["ingreso_animal", "salida_animal"] else tipo,
                detalle, lugar, cantidad, valor, unidad, observacion, jornales,
                finca_id=finca_id,
                usuario_id=usuario_id,
                mensaje_completo=mensaje_completo
            )
            if user_key in user_state:
                del user_state[user_key]
            return f"✅ ¡Registrado en {usuario_info['finca_nombre']}! {detalle}"
    
    return respuesta

# === 6. ENTRADA PRINCIPAL: VALIDACIÓN DE VENCIMIENTO AUTOMÁTICO ===
def procesar_mensaje_whatsapp(mensaje, remitente=None):
    print(f"🔍 [BOT] Procesando mensaje: '{mensaje}' de {remitente}")
    if not remitente:
        return "❌ Error: remitente no identificado."
    mensaje = mensaje.strip()
    if not mensaje:
        return "❌ Mensaje vacío."
    if remitente in user_state and user_state[remitente].get("esperando_nombre_finca"):
        nombre_finca = mensaje
        del user_state[remitente]
        return registrar_nueva_finca(nombre_finca, remitente)
    usuario_info = obtener_usuario_por_whatsapp(remitente)
    if not usuario_info:
        if mensaje.lower() in ["8", "finca", "registrar", "hola", "hi", "buenos días", "buenas", "menu", "ayuda"]:
            user_state[remitente] = {"esperando_nombre_finca": True}
            return (
                "🏡 Bienvenido a Finca Digital.\n"
                "Para comenzar, ¿cómo se llama tu finca?\n"
                "(Ej: Hacienda el Frayle)"
            )
        else:
            return (
                "🏡 Bienvenido.\n"
                "8. 🏡 Registrar mi finca\n"
                "Escribe '8' para comenzar."
            )
    hoy = datetime.date.today()
    vencimiento = usuario_info.get("vencimiento_suscripcion")
    suscripcion_activa = usuario_info["suscripcion_activa"]
    if not suscripcion_activa or (vencimiento and hoy > vencimiento):
        return (
            "🔒 Tu suscripción ha expirado.\n"
            "💳 **Renovación mensual:** $50.000 COP\n"
            "**Nequi:** 314 353 9351 (Omar Pachón)\n"
            "Envía comprobante para reactivar tu finca."
        )
    if "reporte" in mensaje.lower():
        rango = re.search(r"reporte.*?del\s+(\d{1,2})/(\d{1,2})\s+al\s+(\d{1,2})/(\d{1,2})", mensaje.lower())
        if rango:
            try:
                d1, m1, d2, m2 = map(int, rango.groups())
                año = hoy.year
                fecha_inicio = datetime.date(año, m1, d1)
                fecha_fin = datetime.date(año, m2, d2)
                if fecha_inicio > fecha_fin:
                    fecha_inicio, fecha_fin = fecha_fin, fecha_inicio
                return generar_reporte_personalizado(fecha_inicio, fecha_fin, finca_id=usuario_info["finca_id"])
            except Exception as e:
                print(f"❌ Error al parsear fechas: {e}")
        freq = "semanal"
        if "diario" in mensaje.lower(): freq = "diario"
        elif "mensual" in mensaje.lower(): freq = "mensual"
        elif "quincenal" in mensaje.lower(): freq = "quincenal"
        return generar_reporte(frecuencia=freq, formato="texto", finca_id=usuario_info["finca_id"])
    if mensaje.lower().startswith("estado animal "):
        arete = mensaje.split(" ", 2)[2].strip()
        return consultar_estado_animal(arete)
    if mensaje.strip().lower() in ["inventario animales", "lista de animales", "inventario"]:
        return generar_inventario_animales(usuario_info["finca_id"])
    if mensaje.lower().startswith("exportar reporte"):
        return "📎 El reporte en Excel estará disponible pronto en tu WhatsApp."
    if mensaje.strip().lower() in ["ayuda", "help", "menu", "hola"]:
        return (
            f"🌿 Bienvenido a {usuario_info['finca_nombre']}.\n"
            "Elige una opción:\n"
            "1. 🌱 Siembra\n"
            "2. 🌾 Producción\n"
            "3. 💉 Sanidad y Reproducción Animal\n"
            "4. 🐷 Ingreso animal\n"
            "5. 🐄 Salida animal\n"
            "6. 💰 Gasto\n"
            "7. 🛠️ Labor\n"
            "Escribe 'fin' para salir."
        )
    return iniciar_flujo_conversacional_con_finca(mensaje, usuario_info)