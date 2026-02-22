# vaciar_bd.py - Limpia todas las tablas de tu base de datos
import os
import psycopg2

def vaciar_tablas():
    try:
        # Obtener la URL de conexión desde Render
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            print("❌ DATABASE_URL no está definida.")
            return False

        # Conectar a PostgreSQL
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()

        # Vaciar todas las tablas (en orden correcto por dependencias)
        tablas = [
            "salud_animal",
            "produccion",
            "registros",
            "animales"
        ]

        for tabla in tablas:
            cursor.execute(f'TRUNCATE TABLE {tabla} RESTART IDENTITY CASCADE;')
            print(f"✅ {tabla.upper()}: datos eliminados")

        conn.commit()
        conn.close()
        print("🟢 Base de datos limpiada completamente.")
        return True

    except Exception as e:
        print(f"❌ Error al limpiar la base de datos: {e}")
        return False

# === EJECUCIÓN ===
if __name__ == "__main__":
    print("🧹 Iniciando limpieza de base de datos...")
    exit(0) if vaciar_tablas() else exit(1)