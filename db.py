#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import tempfile
import datetime
import csv
import sys
import requests
import psycopg2
import psycopg2.extras

# ---------- ПАРАМЕТРЫ (ваши / тестовые) ----------
PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "admin"
PG_DBNAME = "postgres"

YANDEX_LOGIN = "pznb@yandex.ru"
YANDEX_TOKEN = ""

REMOTE_DIR = "Backups/sql_results"
# -------------------------------------------------

def run_query_and_write_csv(conn_params, query, local_path, chunk_size=1000):
    """
    Выполнить SELECT-запрос и записать результат в CSV.
    Сначала пробует client-side RealDictCursor; если description отсутствует,
    переключается на server-side named cursor.
    """
    conn = psycopg2.connect(**conn_params)

    try:
        # 1) Попытка: обычный (client-side) RealDictCursor
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.itersize = chunk_size
            cur.execute(query)
            if cur.description is not None:
                fieldnames = [desc.name for desc in cur.description]
                with open(local_path, mode="w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    while True:
                        rows = cur.fetchmany(chunk_size)
                        if not rows:
                            break
                        for row in rows:
                            writer.writerow(row)
                return  # успешно записали, выходим
            # если description is None — перейдём к fallback ниже
        finally:
            try:
                cur.close()
            except Exception:
                pass

        # 2) Fallback: server-side named cursor (streaming с сервера)
        srv_cur = conn.cursor(name="export_cursor")
        try:
            srv_cur.itersize = chunk_size
            srv_cur.execute(query)
            if srv_cur.description is None:
                # если и тут нет description — это явно не SELECT
                raise RuntimeError(
                    "Запрос не вернул описания колонок (cur.description is None) — "
                    "возможно, это не SELECT или запрос некорректен."
                )
            fieldnames = [desc.name for desc in srv_cur.description]
            with open(local_path, mode="w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for row in srv_cur:
                    # server-side курсор возвращает кортежи, не dict — сопоставим с именами
                    writer.writerow({fieldnames[i]: row[i] for i in range(len(fieldnames))})
        finally:
            try:
                srv_cur.close()
            except Exception:
                pass

    finally:
        conn.close()


def mkcol_recursive(base_webdav, remote_dir, auth):
    parts = remote_dir.strip("/").split("/")
    accum = ""
    for p in parts:
        accum = f"{accum}/{p}" if accum else p
        url_part = f"{base_webdav}/{accum}"
        try:
            r = requests.request('MKCOL', url_part, auth=auth, timeout=10)
            if r.status_code in (201, 405):
                pass
            elif r.status_code == 409:
                pass
            elif not (200 <= r.status_code < 300):
                print(f"Предупреждение: MKCOL {url_part} вернул {r.status_code}")
        except Exception as e:
            print(f"Предупреждение: MKCOL {url_part} исключение: {e}")

def upload_file_to_yadisk(local_path, remote_url, auth):
    with open(local_path, "rb") as f:
        resp = requests.put(remote_url, auth=auth, data=f, headers={"Connection": "keep-alive"})
    return resp

def main():
    # Пример: используйте SELECT; замените на ваш реальный SELECT
    sql_query = "SELECT * FROM pg_stat_activity LIMIT 100;"

    conn_params = {
        "host": PG_HOST,
        "port": PG_PORT,
        "user": PG_USER,
        "password": PG_PASSWORD,
        "dbname": PG_DBNAME,
    }

    # timezone-aware timestamp -> избегаем DeprecationWarning
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d_%H%M%S")

    local_fd, local_path = tempfile.mkstemp(prefix="pgq_", suffix=".csv")
    os.close(local_fd)

    try:
        print("Выполняем запрос и пишем CSV в:", local_path)
        try:
            run_query_and_write_csv(conn_params, sql_query, local_path)
        except RuntimeError as e:
            print("Ошибка выполнения запроса:", e, file=sys.stderr)
            # удаляем пустой файл, если он был создан
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except Exception:
                    pass
            sys.exit(1)

        base_webdav = "https://webdav.yandex.ru"
        remote_dir = REMOTE_DIR.strip("/")
        remote_filename = f"{PG_DBNAME}_query_{ts}.csv"
        remote_dir_url = f"{base_webdav}/{remote_dir}"
        remote_file_url = f"{remote_dir_url}/{remote_filename}"

        auth = (YANDEX_LOGIN, YANDEX_TOKEN)

        print("Создаём (при необходимости) папки на Yandex.Disk:", remote_dir)
        mkcol_recursive(base_webdav, remote_dir, auth)

        print("Загружаем файл на Yandex.Disk:", remote_file_url)
        resp = upload_file_to_yadisk(local_path, remote_file_url, auth)
        if 200 <= resp.status_code < 300:
            print("Успех: файл загружен как", remote_filename)
        else:
            print("Ошибка загрузки:", resp.status_code, resp.text, file=sys.stderr)
            sys.exit(2)
    finally:
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
            except Exception as e:
                print("Не удалось удалить локальный файл:", e, file=sys.stderr)

if __name__ == "__main__":
    main()
