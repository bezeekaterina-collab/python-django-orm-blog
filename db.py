#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pg_query_to_yadisk.py

Выполняет SQL-запрос к PostgreSQL, сохраняет результат в CSV и загружает файл на Yandex.Disk (WebDAV).

Запуск (пример):
  export YANDEX_DISK_TOKEN="ВАШ_ТОКЕН_ЗДЕСЬ"
  python3 pg_query_to_yadisk.py \
    --pg-host localhost --pg-port 5432 --pg-user myuser --pg-database mydb \
    --query "SELECT id, name, created_at FROM users WHERE created_at >= now() - interval '7 days';" \
    --remote-dir "Backups/sql_results" --remote-name "users_last7days.csv"

Примечание: безопаснее хранить токен в переменной окружения YANDEX_DISK_TOKEN.
"""

import os
import argparse
import tempfile
import datetime
import csv
import sys
import requests
import psycopg2
import psycopg2.extras

# ------------------ ФУНКЦИИ ------------------

def run_query_and_write_csv(conn_params, query, local_path, chunk_size=1000):
    """
    Выполнить запрос и записать результат в CSV-файл.
    Использует серверные курсоры (named cursor) для экономии памяти при больших результирующих наборах.
    """
    conn = psycopg2.connect(**conn_params)
    # Используем named cursor для постраничного чтения
    cur = conn.cursor(name="export_cursor", cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.itersize = chunk_size  # сколько строк тянуть за раз
        cur.execute(query)

        # Получаем имена колонок из описания курсора (RealDictCursor даёт dict-like строки)
        fieldnames = list(cur.description[i].name for i in range(len(cur.description)))

        with open(local_path, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # Fetch many rows по итератору
            for row in cur:
                # row — psycopg2.extras.RealDictRow, совместим с DictWriter
                writer.writerow(row)

    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

def ensure_remote_dir_exists(remote_dir_url, auth):
    """
    Попытка создать удалённую директорию на WebDAV (MKCOL).
    Если директория уже существует — сервер ответит 405 или 409, в этом случае игнорируем.
    """
    # MKCOL создаёт одну папку; если нужно рекурсивно — надо создавать части по очереди
    resp = requests.request('MKCOL', remote_dir_url, auth=auth)
    if resp.status_code in (201, 405, 405):  # 201 created, 405 method not allowed (может означать уже есть)
        return True
    # 409 Conflict — возможно родительской папки нет
    return resp.ok

def upload_file_to_yadisk(local_path, remote_url, auth, chunk_size=1024*1024):
    """
    Загружает файл по WebDAV методом PUT. Использует потоковую отправку больших файлов.
    """
    with open(local_path, 'rb') as f:
        # requests.put с data=f отправляет поток и не держит весь файл в памяти
        resp = requests.put(remote_url, auth=auth, data=f, headers={"Connection": "keep-alive"})
    return resp

# ------------------ MAIN ------------------

def main():
    parser = argparse.ArgumentParser(description="Выполнить SQL и загрузить CSV на Yandex.Disk (WebDAV).")
    # Параметры PostgreSQL
    parser.add_argument("--pg-host", default=os.environ.get("PGHOST", "localhost"))
    parser.add_argument("--pg-port", default=os.environ.get("PGPORT", "5432"))
    parser.add_argument("--pg-user", default=os.environ.get("PGUSER", None))
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD", None))
    parser.add_argument("--pg-database", default=os.environ.get("PGDATABASE", None))
    # Запрос
    parser.add_argument("--query", required=True, help="SQL-запрос (обязательно).")
    # Yandex/Disk
    parser.add_argument("--yandex-login", default=os.environ.get("YANDEX_LOGIN", None),
                        help="Логин Яндекса (без @). Можно оставить пустым, тогда будет использован email/логин из env YANDEX_LOGIN.")
    parser.add_argument("--remote-dir", default="Backups",
                        help="Директория на Yandex.Disk (без начального /). Например: Backups/sql_results")
    parser.add_argument("--remote-name", default=None, help="Имя файла на диске. Если не указано — будет сгенерировано.")
    parser.add_argument("--local-temp", default=None, help="Путь к временному файлу (по умолчанию tmpfile).")
    args = parser.parse_args()

    # Получаем токен из окружения (безопаснее)
    yad_token = os.environ.get("YANDEX_DISK_TOKEN")
    if not yad_token:
        print("Ошибка: переменная окружения YANDEX_DISK_TOKEN не установлена.", file=sys.stderr)
        sys.exit(1)

    # Формируем параметры подключения к PostgreSQL
    conn_params = {
        "host": args.pg_host,
        "port": args.pg_port,
        "user": args.pg_user,
        "password": args.pg_password,
        "dbname": args.pg_database,
    }
    # Проверка минимальных параметров
    if not conn_params["user"] or not conn_params["dbname"]:
        print("Ошибка: не заданы параметры подключения к PostgreSQL (pg_user и pg_database).", file=sys.stderr)
        sys.exit(1)

    # Генерация имени файла
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
    remote_filename = args.remote_name or f"{conn_params['dbname']}_query_{ts}.csv"

    # Локальный временный файл
    if args.local_temp:
        local_path = args.local_temp
    else:
        fd, local_path = tempfile.mkstemp(prefix="pg_query_", suffix=".csv")
        os.close(fd)

    try:
        print("Выполняем запрос и записываем результат в CSV:", local_path)
        run_query_and_write_csv(conn_params, args.query, local_path)

        # Подготовка URL для WebDAV
        # В URL путь должен быть закодирован; для простоты используем прямую конкатенацию, 
        # но если в именах есть специальные символы, их следует url-encode.
        base_webdav = "https://webdav.yandex.ru"
        remote_dir = args.remote_dir.strip("/")

        # Формируем полный URL директории и файла
        remote_dir_url = f"{base_webdav}/{remote_dir}"
        remote_file_url = f"{remote_dir_url}/{remote_filename}"

        auth = (args.yandex_login or "", yad_token)

        # Попытка создать директорию (MKCOL). Если родительских каталогов несколько, создаём по частям.
        # Разбиваем remote_dir на части и создаём рекурсивно
        parts = remote_dir.split("/")
        accum = ""
        for p in parts:
            accum = accum + "/" + p if accum else p
            url_part = f"{base_webdav}/{accum}"
            # Игнорируем неудачи, но печатаем предупреждение
            try:
                r = requests.request('MKCOL', url_part, auth=auth)
                if r.status_code in (201, 405):  # 201 created, 405 method not allowed (может означать "уже есть")
                    pass
                elif r.status_code == 409:
                    # parent missing — продолжим, возможно следующая итерация создаст нужный уровень
                    pass
                elif not (200 <= r.status_code < 300):
                    # неудача — предупреждаем, но не прерываем
                    print(f"Предупреждение: MKCOL {url_part} -> статус {r.status_code}")
            except Exception as e:
                print(f"Предупреждение: не удалось выполнить MKCOL {url_part}: {e}")

        print(f"Загружаем {local_path} -> {remote_file_url} ...")
        resp = upload_file_to_yadisk(local_path, remote_file_url, auth)
        if 200 <= resp.status_code < 300:
            print("Файл успешно загружен на Yandex.Disk как", remote_filename)
        else:
            print("Ошибка загрузки:", resp.status_code, resp.text, file=sys.stderr)
            sys.exit(2)

    finally:
        # Чистим временный файл
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
            except Exception as e:
                print("Внимание: не удалось удалить временный файл:", e, file=sys.stderr)

if __name__ == "__main__":
    main()
