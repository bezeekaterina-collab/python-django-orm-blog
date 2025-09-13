#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import threading
import tempfile
import datetime
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from dotenv import load_dotenv
load_dotenv()
import db
from db import run_query_and_write_csv, mkcol_recursive, upload_file_to_yadisk

def get_conn_params():
    return {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": int(os.getenv("PG_PORT", 5432)),
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", ""),
        "dbname": os.getenv("PG_DBNAME", "postgres"),
    }

def get_yadisk_auth():
    return (os.getenv("YANDEX_LOGIN", ""), os.getenv("YANDEX_TOKEN", ""))

def timestamp():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d_%H%M%S")

class App:
    def __init__(self, root):
        self.root = root
        root.title("PG → Yandex.Disk")
        frm = ttk.Frame(root, padding=12)
        frm.grid(sticky="nsew")
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)

        ttk.Label(frm, text="SQL (SELECT) :").grid(row=0, column=0, sticky="w")
        self.txt_sql = tk.Text(frm, height=8, width=80)
        self.txt_sql.grid(row=1, column=0, columnspan=3, sticky="nsew", pady=6)
        self.txt_sql.insert("1.0", "SELECT 1 AS test_col, now() AS ts;")

        ttk.Label(frm, text="Remote dir:").grid(row=2, column=0, sticky="w")
        self.ent_remote = ttk.Entry(frm, width=40)
        self.ent_remote.grid(row=2, column=1, sticky="w")
        self.ent_remote.insert(0, os.getenv("REMOTE_DIR", "Backups/sql_results") if os.getenv("REMOTE_DIR") else "Backups/sql_results")

        ttk.Label(frm, text="Remote filename (optional):").grid(row=3, column=0, sticky="w")
        self.ent_name = ttk.Entry(frm, width=40)
        self.ent_name.grid(row=3, column=1, sticky="w")

        self.btn_run = ttk.Button(frm, text="Run and Upload", command=self.on_run)
        self.btn_run.grid(row=4, column=0, pady=8, sticky="w")

        self.btn_choose = ttk.Button(frm, text="Save CSV locally...", command=self.save_local)
        self.btn_choose.grid(row=4, column=1, pady=8, sticky="w")

        self.progress = ttk.Progressbar(frm, mode="indeterminate")
        self.progress.grid(row=5, column=0, columnspan=3, sticky="ew", pady=6)

        ttk.Label(frm, text="Status:").grid(row=6, column=0, sticky="nw")
        self.txt_status = tk.Text(frm, height=8, width=80, state="disabled")
        self.txt_status.grid(row=7, column=0, columnspan=3, sticky="nsew")

        for i in range(3):
            frm.columnconfigure(i, weight=1)
        frm.rowconfigure(7, weight=1)

    def log(self, msg):
        self.txt_status.configure(state="normal")
        self.txt_status.insert("end", msg + "\n")
        self.txt_status.see("end")
        self.txt_status.configure(state="disabled")

    def on_run(self):
        sql = self.txt_sql.get("1.0", "end").strip()
        if not sql:
            messagebox.showerror("Error", "SQL is empty")
            return
        remote_dir = self.ent_remote.get().strip() or "Backups/sql_results"
        remote_name = self.ent_name.get().strip() or None
        self.btn_run.config(state="disabled")
        self.progress.start(10)
        threading.Thread(target=self.worker, args=(sql, remote_dir, remote_name), daemon=True).start()

    def save_local(self):
        sql = self.txt_sql.get("1.0", "end").strip()
        if not sql:
            messagebox.showerror("Error", "SQL is empty")
            return
        f = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files","*.csv"),("All files","*.*")])
        if not f:
            return
        conn_params = get_conn_params()
        try:
            run_query_and_write_csv(conn_params, sql, f)
            messagebox.showinfo("Saved", f"CSV saved to {f}")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def worker(self, sql, remote_dir, remote_name):
        try:
            self.log("Starting query...")
            conn_params = get_conn_params()
            fd, local_path = tempfile.mkstemp(prefix="pgq_", suffix=".csv")
            os.close(fd)
            run_query_and_write_csv(conn_params, sql, local_path)
            self.log(f"Query finished, local file: {local_path}")
            base_webdav = "https://webdav.yandex.ru"
            remote_dir_clean = remote_dir.strip("/")
            fname = remote_name or f"{conn_params.get('dbname','db')}_query_{timestamp()}.csv"
            remote_file_url = f"{base_webdav}/{remote_dir_clean}/{fname}"
            auth = get_yadisk_auth()
            self.log("Ensuring remote dirs...")
            mkcol_recursive(base_webdav, remote_dir_clean, auth)
            self.log(f"Uploading to {remote_file_url} ...")
            resp = upload_file_to_yadisk(local_path, remote_file_url, auth)
            if 200 <= resp.status_code < 300:
                self.log("Upload successful")
                messagebox.showinfo("Success", f"Uploaded as {fname}")
            else:
                self.log(f"Upload failed: {resp.status_code} {resp.text}")
                messagebox.showerror("Upload error", f"{resp.status_code}: {resp.text}")
        except Exception as e:
            self.log(f"Error: {e}")
            messagebox.showerror("Error", str(e))
        finally:
            try:
                if 'local_path' in locals() and os.path.exists(local_path):
                    os.remove(local_path)
            except Exception:
                pass
            self.progress.stop()
            self.btn_run.config(state="normal")

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
