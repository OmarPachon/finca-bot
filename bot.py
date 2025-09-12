# -*- coding: utf-8 -*-
"""
bot.py - Sistema de registro para finca
Estructura: REGISTRAR: tipo, detalle, cantidad, valor, unidad, lugar, observación
"""

import os
import psycopg2
from urllib.parse import urlparse
import re
import datetime

print("🔧 Iniciando bot.py...")

# === 1. CONEXIÓN A POSTGRESQL ===
def inicializar_bd():
    """Conecta a PostgreSQL y crea tablas si no existen"""
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            print("❌ DATABASE_URL no está definida. Configúrala en Render.")
            return False

        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()

        # Tabla: animales
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

        # Tabla: registros generales
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

        # Tabla: salud_animal
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

        # Tabla: produccion
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

# Llamar a inicializar_bd() al cargar el módulo
try:
    BD_OK = inicializar_bd()
except Exception as e:
    print(f"❌ Error crítico al inicializar BD: {e}")
    BD_OK = False


# === 2. SINÓNIMOS AMPLIADOS ===
SINONIMOS = {
    "siembra": ["sembrar", "sembramos", "siembra", "plantar", "plantamos", "pusimos semilla", "resiembra", "resembramos"],
    "cosecha": ["cosechar", "cosechamos", "cosecha", "cortar", "recolectar", "recolectamos", "descacotamos", "descacotar"],
    "alimentacion": ["alimentar", "alimentamos", "dar de comer", "echamos comida", "comida", "alimento", "concentrado"],
    "vacunacion": ["vacunar", "vacunamos", "vacuna", "inyectar", "pusimos vacuna", "inyección", "vacuno", "aftosa", "brucelosis"],
    "desparasitacion": ["desparasitar", "desparasitamos", "purgar", "dar desparasitante"],
    "reproduccion": ["monta", "preñez", "preñada", "inseminada", "inseminacion", "cubierta", "cubiertas", "cerda", "verraco", "toro", "novillo", "vaquilla", "nacimiento", "parto"],
    "labor": [
        "fumigar", "fumigamos", "rociar", "castramos", "curamos", "insecticida",
        "herbicida", "castrar", "poda", "control de plagas", "cerca", "cercamos",
        "limpiar", "abonamos", "podar", "podamos", "lavar", "arreglar", "reparar",
        "abonar", "aserrar", "lavado", "macaneadora", "macaneo", "destete", "marcacion", "marcaje", "reparacion"
    ],
    "produccion": ["carne", "kg", "kilos", "peso cerdo", "peso vaca", "salieron", "rendimiento", "cosechamos", "sacamos", "leche", "litros", "ordeñar", "producción", "lechón", "ternero", "ternera"],
    "inventario": ["llego", "ingreso", "compramos", "recibimos", "se compro", "se pidio", "se compraron", "pedimos", "nacio", "nacieron"],
    "gasto": ["gasto", "pagamos", "vendimos", "se murieron", "baja", "muertes", "perdida", "se pago", "se vendio", "vendieron", "factura", "compra", "costo"]
}

# === 3. DETECTAR ACTIVIDAD ===
def detectar_actividad(mensaje):
    mensaje = mensaje.lower()
    for actividad, palabras in SINONIMOS.items():
        for palabra in palabras:
            if palabra in mensaje:
                return actividad
    return "general"


# === 4. REGISTRO DE ANIMAL ===
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


# === 5. GUARDAR REGISTRO GENERAL (PostgreSQL) ===
def guardar_registro(tipo_actividad, accion, detalle, lugar=None, cantidad=None, valor=0, unidad=None, observacion=None):
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            return

        fecha = datetime.date.today().isoformat()
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO registros (fecha, tipo_actividad, accion, detalle, lugar, cantidad, valor, unidad, observacion, fecha_registro)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (fecha, tipo_actividad, accion, detalle, lugar, cantidad, valor, unidad, observacion, datetime.datetime.now().isoformat()))
            conn.commit()
    except Exception as e:
        print(f"❌ Error al guardar registro: {e}")


# === 6. GENERAR REPORTE DETALLADO ===
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
    except Exception as e:
        return f"❌ Error al leer la base de datos: {e}"

    if formato == "texto":
        lines = [titulo, periodo, ""]

        siembras = [r for r in registros if r[1] == "siembra"]
        cosechas = [r for r in registros if r[1] == "cosecha"]
        labores = [r for r in registros if r[1] == "labor"]
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
                if row[5] and "jornal" in (row[3] or row[8] or ''): 
                    desc += f" ({row[5]} jornales)"
                if row[8]: desc += f". Obs: {row[8]}"
                lines.append(desc)
            lines.append("")

        # PRODUCCIÓN
        if produccion:
            lines.append("🥛🥩 PRODUCCIÓN")
            for row in produccion:
                if not row[3]: continue
                desc = f"• {row[5]} {row[7]} de {row[3]}"
                if row[4]: desc += f" del {row[4]}"
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
                valor = row[6] if row[6] > 0 else 0
                desc = f"• Gasto: {row[8] or 'sin detalle'}"
                if valor > 0: desc += f" (${valor:,.0f})"
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
                desc = f"• {row[3]}"
                if row[4]: desc += f" en {row[4]}"
                if row[8]: desc += f". Obs: {row[8]}"
                lines.append(desc)
            lines.append("")

        lines.append("✅ Todo bajo control. ¡Buen trabajo!")
        return "\n".join(lines)

    return registros


# === 7. COMANDO SECRETO: limpiar bd ===
def vaciar_tablas():
    """
    Borra todos los datos de las tablas del bot.
    Solo debe ser llamado desde un comando seguro.
    """
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


# === 8. PROCESAR MENSAJE WHASTAPP ===
def procesar_mensaje_whatsapp(mensaje, remitente=None):
    """
    Procesa un mensaje entrante.
    :param mensaje: Contenido del mensaje
    :param remitente: Número del remitente (opcional para comandos secretos)
    """
    print(f"🔍 [BOT] Procesando mensaje: '{mensaje}'")
    mensaje = mensaje.strip()
    if not mensaje:
        print("❌ Mensaje vacío")
        return "❌ Mensaje vacío."

    # --- COMANDO SECRETO: limpiar bd ---
    if mensaje.strip().lower() == "limpiar bd":
        if remitente == "whatsapp:+573143539351":  # Tu número
            return vaciar_tablas()
        else:
            return "❌ Acceso denegado. Comando no autorizado."
    # -------------------------------------

    # Comandos de reporte
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

    # Validación de formato estructurado
    if ":" in mensaje:
        partes = mensaje.split(":", 1)
        verbo = partes[0].strip().upper()
        resto = partes[1].strip()

        if verbo != "REGISTRAR":
            return "❌ Usa solo: REGISTRAR: tipo, detalle, cantidad, valor, unidad, lugar, observación"

        campos = [campo.strip() for campo in resto.split(",")]
        if len(campos) < 4:
            return "❌ Faltan campos. Usa: tipo, detalle, cantidad, valor, unidad, lugar, observación"

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

        # Labor
        elif "labor" in tipo or any(lab in tipo for lab in ["fumigar", "poda", "limpiar", "reparar"]):
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

    return "❌ Formato incorrecto. Usa: REGISTRAR: tipo, detalle, cantidad, valor, unidad, lugar, observación"