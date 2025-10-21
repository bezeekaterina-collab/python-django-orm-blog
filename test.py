import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def main():
    # Параметры подключения
    conn_params = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'admin',
        'database': 'postgres'
    }
    
    schema_name = 'test_schema'
    table_name = 'test_table'
    
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(**conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Создание схемы (если не существует)
        cur.execute(sql.SQL("""
            CREATE SCHEMA IF NOT EXISTS {}
        """).format(sql.Identifier(schema_name)))
        print(f"Схема '{schema_name}' создана или уже существует")
        
        # Создание таблицы (если не существует)
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).format(sql.Identifier(schema_name), sql.Identifier(table_name)))
        print(f"Таблица '{schema_name}.{table_name}' создана или уже существует")
        
        # Добавление одной строки (с проверкой на дубликаты по имени)
        sample_name = 'Тестовая запись'
        cur.execute(sql.SQL("""
            INSERT INTO {}.{} (name)
            SELECT %s
            WHERE NOT EXISTS (
                SELECT 1 FROM {}.{} WHERE name = %s
            )
        """).format(
            sql.Identifier(schema_name), 
            sql.Identifier(table_name),
            sql.Identifier(schema_name), 
            sql.Identifier(table_name)
        ), (sample_name, sample_name))
        print(f"Данные добавлены (если не было дубликата)")
        
        # Получение данных через схему.таблица (находясь в search_path по умолчанию)
        cur.execute(sql.SQL("""
            SELECT id, name, created_at 
            FROM {}.{}
            ORDER BY id
        """).format(sql.Identifier(schema_name), sql.Identifier(table_name)))
        
        rows = cur.fetchall()
        print(f"\nДанные из таблицы '{schema_name}.{table_name}':")
        print("-" * 60)
        for row in rows:
            print(f"ID: {row[0]}, Имя: {row[1]}, Создано: {row[2]}")
        print("-" * 60)
        print(f"Всего записей: {len(rows)}")
        
        # Закрытие соединения
        cur.close()
        conn.close()
        print("\nСоединение закрыто")
        
    except psycopg2.Error as e:
        print(f"Ошибка PostgreSQL: {e}")
    except Exception as e:
        print(f"Общая ошибка: {e}")

if __name__ == "__main__":
    main()