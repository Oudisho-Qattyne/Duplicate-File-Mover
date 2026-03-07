#!/usr/bin/env python3
"""
Duplicate File Mover – Tkinter GUI (Default Theme)

A graphical tool to find duplicate files between two folders and move them
from a source folder to a destination folder, preserving subfolder structure.
Supports name+size and content‑only modes, optional hash verification,
and dry‑runs. All heavy work runs in a background thread so the GUI stays responsive.
"""

import os
import sys
import shutil
import hashlib
import sqlite3
import threading
import queue
from collections import defaultdict
from pathlib import Path

import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext

# ----------------------------------------------------------------------
# Core duplicate detection functions (same as before)
# ----------------------------------------------------------------------
def compute_file_hash(filepath, algorithm='md5', chunk_size=8192):
    hasher = hashlib.new(algorithm)
    with open(filepath, 'rb') as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()

def build_index_memory(ref_folder, method, use_hash, hash_algo, progress_callback=None, cancel_flag=None):
    if method == 'name-size':
        index = defaultdict(list)
    else:
        index = defaultdict(lambda: defaultdict(list))

    ref_folder = os.path.abspath(ref_folder)
    total_files = 0
    # First count files for progress
    for root, dirs, files in os.walk(ref_folder):
        total_files += len(files)
    processed = 0

    for root, dirs, files in os.walk(ref_folder):
        if cancel_flag and cancel_flag():
            return None
        for file in files:
            processed += 1
            if progress_callback:
                progress_callback(processed, total_files)
            full_path = os.path.join(root, file)
            try:
                size = os.path.getsize(full_path)
                if method == 'name-size':
                    key = (file, size)
                    h = compute_file_hash(full_path, hash_algo) if use_hash else None
                    index[key].append((full_path, h))
                else:  # content
                    h = compute_file_hash(full_path, hash_algo)
                    index[size][h].append(full_path)
            except (OSError, PermissionError) as e:
                if progress_callback:
                    progress_callback(msg=f"Warning: {full_path} – {e}")
    return index

def find_duplicates_memory(source_folder, index, method, use_hash, hash_algo, destination,
                           progress_callback=None, cancel_flag=None):
    duplicates = []
    source_folder = os.path.abspath(source_folder)
    # Count files for progress
    total_files = sum(len(files) for _, _, files in os.walk(source_folder))
    scanned = 0

    for root, dirs, files in os.walk(source_folder):
        if cancel_flag and cancel_flag():
            return None
        for file in files:
            scanned += 1
            if progress_callback:
                progress_callback(scanned, total_files)
            source_path = os.path.join(root, file)
            try:
                size = os.path.getsize(source_path)
                if method == 'name-size':
                    key = (file, size)
                    if key not in index:
                        continue
                    candidates = index[key]
                    if not use_hash:
                        rel_path = os.path.relpath(source_path, source_folder)
                        dest_path = os.path.join(destination, rel_path)
                        duplicates.append((source_path, dest_path))
                        if progress_callback:
                            progress_callback(msg=f"Found duplicate: {source_path}")
                        continue
                    source_hash = compute_file_hash(source_path, hash_algo)
                    for cand_path, cand_hash in candidates:
                        if cand_hash is None:
                            cand_hash = compute_file_hash(cand_path, hash_algo)
                        if source_hash == cand_hash:
                            rel_path = os.path.relpath(source_path, source_folder)
                            dest_path = os.path.join(destination, rel_path)
                            duplicates.append((source_path, dest_path))
                            if progress_callback:
                                progress_callback(msg=f"Found duplicate: {source_path}")
                            break
                else:  # content
                    if size not in index:
                        continue
                    source_hash = compute_file_hash(source_path, hash_algo)
                    if source_hash in index[size]:
                        rel_path = os.path.relpath(source_path, source_folder)
                        dest_path = os.path.join(destination, rel_path)
                        duplicates.append((source_path, dest_path))
                        if progress_callback:
                            progress_callback(msg=f"Found duplicate: {source_path}")
            except (OSError, PermissionError) as e:
                if progress_callback:
                    progress_callback(msg=f"Warning: Cannot process {source_path}: {e}")
    return duplicates

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS files_name_size
                 (name TEXT, size INTEGER, path TEXT, hash TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_name_size ON files_name_size (name, size)')
    c.execute('''CREATE TABLE IF NOT EXISTS files_content
                 (size INTEGER, hash TEXT, path TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_size_hash ON files_content (size, hash)')
    conn.commit()
    return conn

def build_index_db(ref_folder, db_path, method, use_hash, hash_algo,
                   progress_callback=None, cancel_flag=None):
    conn = init_db(db_path)
    c = conn.cursor()
    c.execute("DELETE FROM files_name_size")
    c.execute("DELETE FROM files_content")
    conn.commit()

    ref_folder = os.path.abspath(ref_folder)
    batch = []
    batch_size = 1000
    total_files = sum(len(files) for _, _, files in os.walk(ref_folder))
    processed = 0

    for root, dirs, files in os.walk(ref_folder):
        if cancel_flag and cancel_flag():
            conn.close()
            return None
        for file in files:
            processed += 1
            if progress_callback:
                progress_callback(processed, total_files)
            full_path = os.path.join(root, file)
            try:
                size = os.path.getsize(full_path)
                if method == 'name-size':
                    h = compute_file_hash(full_path, hash_algo) if use_hash else None
                    batch.append((file, size, full_path, h))
                else:
                    h = compute_file_hash(full_path, hash_algo)
                    batch.append((size, h, full_path))
                if len(batch) >= batch_size:
                    if method == 'name-size':
                        c.executemany("INSERT INTO files_name_size VALUES (?,?,?,?)", batch)
                    else:
                        c.executemany("INSERT INTO files_content VALUES (?,?,?)", batch)
                    conn.commit()
                    batch.clear()
            except (OSError, PermissionError) as e:
                if progress_callback:
                    progress_callback(msg=f"Warning: {full_path} – {e}")
    if batch:
        if method == 'name-size':
            c.executemany("INSERT INTO files_name_size VALUES (?,?,?,?)", batch)
        else:
            c.executemany("INSERT INTO files_content VALUES (?,?,?)", batch)
        conn.commit()
    return conn

def find_duplicates_db(source_folder, conn, method, use_hash, hash_algo, destination,
                       progress_callback=None, cancel_flag=None):
    duplicates = []
    source_folder = os.path.abspath(source_folder)
    c = conn.cursor()
    total_files = sum(len(files) for _, _, files in os.walk(source_folder))
    scanned = 0

    for root, dirs, files in os.walk(source_folder):
        if cancel_flag and cancel_flag():
            return None
        for file in files:
            scanned += 1
            if progress_callback:
                progress_callback(scanned, total_files)
            source_path = os.path.join(root, file)
            try:
                size = os.path.getsize(source_path)
                if method == 'name-size':
                    if not use_hash:
                        c.execute("SELECT 1 FROM files_name_size WHERE name=? AND size=? LIMIT 1",
                                  (file, size))
                        if c.fetchone():
                            rel_path = os.path.relpath(source_path, source_folder)
                            dest_path = os.path.join(destination, rel_path)
                            duplicates.append((source_path, dest_path))
                            if progress_callback:
                                progress_callback(msg=f"Found duplicate: {source_path}")
                    else:
                        source_hash = compute_file_hash(source_path, hash_algo)
                        c.execute("SELECT 1 FROM files_name_size WHERE name=? AND size=? AND hash=? LIMIT 1",
                                  (file, size, source_hash))
                        if c.fetchone():
                            rel_path = os.path.relpath(source_path, source_folder)
                            dest_path = os.path.join(destination, rel_path)
                            duplicates.append((source_path, dest_path))
                            if progress_callback:
                                progress_callback(msg=f"Found duplicate: {source_path}")
                else:  # content
                    source_hash = compute_file_hash(source_path, hash_algo)
                    c.execute("SELECT 1 FROM files_content WHERE size=? AND hash=? LIMIT 1",
                              (size, source_hash))
                    if c.fetchone():
                        rel_path = os.path.relpath(source_path, source_folder)
                        dest_path = os.path.join(destination, rel_path)
                        duplicates.append((source_path, dest_path))
                        if progress_callback:
                            progress_callback(msg=f"Found duplicate: {source_path}")
            except (OSError, PermissionError) as e:
                if progress_callback:
                    progress_callback(msg=f"Warning: Cannot process {source_path}: {e}")
    return duplicates

def move_duplicates(duplicates, dry_run, progress_callback=None, cancel_flag=None):
    total = len(duplicates)
    for i, (src, dst) in enumerate(duplicates, 1):
        if cancel_flag and cancel_flag():
            return
        if progress_callback:
            progress_callback(i, total)
        dst_dir = os.path.dirname(dst)
        if not dry_run:
            os.makedirs(dst_dir, exist_ok=True)
            try:
                shutil.move(src, dst)
                if progress_callback:
                    progress_callback(msg=f"Moved: {src} -> {dst}")
            except Exception as e:
                if progress_callback:
                    progress_callback(msg=f"Error moving {src}: {e}")
        else:
            if progress_callback:
                progress_callback(msg=f"[DRY RUN] Would move: {src} -> {dst}")

# ----------------------------------------------------------------------
# Worker thread that runs the duplicate detection/moving
# ----------------------------------------------------------------------
class DuplicateWorker(threading.Thread):
    def __init__(self, queue, source, reference, destination, method,
                 use_hash, hash_algo, dry_run, use_db):
        super().__init__()
        self.queue = queue
        self.source = source
        self.reference = reference
        self.destination = destination
        self.method = method
        self.use_hash = use_hash
        self.hash_algo = hash_algo
        self.dry_run = dry_run
        self.use_db = use_db
        self._cancel = False

    def cancel(self):
        self._cancel = True

    def progress_callback(self, current=None, total=None, msg=None):
        """Send progress updates to the GUI via queue."""
        if msg is not None:
            self.queue.put(('msg', msg))
        if current is not None and total is not None:
            self.queue.put(('progress', current, total))

    def run(self):
        try:
            # Phase 1: Build reference index
            self.progress_callback(msg="Building reference index...")
            if self.use_db:
                conn = build_index_db(
                    self.reference, self.use_db, self.method, self.use_hash, self.hash_algo,
                    progress_callback=self.progress_callback, cancel_flag=lambda: self._cancel
                )
                if conn is None:  # cancelled
                    self.queue.put(('finished',))
                    return
            else:
                index = build_index_memory(
                    self.reference, self.method, self.use_hash, self.hash_algo,
                    progress_callback=self.progress_callback, cancel_flag=lambda: self._cancel
                )
                if index is None:
                    self.queue.put(('finished',))
                    return

            # Phase 2: Scan source for duplicates
            self.progress_callback(msg="Scanning source folder for duplicates...")
            if self.use_db:
                duplicates = find_duplicates_db(
                    self.source, conn, self.method, self.use_hash, self.hash_algo, self.destination,
                    progress_callback=self.progress_callback, cancel_flag=lambda: self._cancel
                )
                conn.close()
            else:
                duplicates = find_duplicates_memory(
                    self.source, index, self.method, self.use_hash, self.hash_algo, self.destination,
                    progress_callback=self.progress_callback, cancel_flag=lambda: self._cancel
                )
            if duplicates is None:
                self.queue.put(('finished',))
                return

            self.progress_callback(msg=f"Found {len(duplicates)} duplicate files.")
            if not duplicates:
                self.queue.put(('finished',))
                return

            # Phase 3: Move duplicates
            self.progress_callback(msg="Moving duplicates...")
            move_duplicates(
                duplicates, self.dry_run,
                progress_callback=self.progress_callback, cancel_flag=lambda: self._cancel
            )
            self.progress_callback(msg="Done!")
            self.queue.put(('finished',))
        except Exception as e:
            self.queue.put(('error', str(e)))
            self.queue.put(('finished',))

# ----------------------------------------------------------------------
# Main GUI Application (Default Theme)
# ----------------------------------------------------------------------
class DuplicateMoverApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Duplicate File Mover")
        self.root.geometry("800x600")
        self.root.minsize(700, 500)

        # Use a modern theme if available (without custom colors)
        style = ttk.Style()
        available_themes = style.theme_names()
        if 'clam' in available_themes:
            style.theme_use('clam')
        elif 'vista' in available_themes:
            style.theme_use('vista')
        elif 'alt' in available_themes:
            style.theme_use('alt')
        # No custom color overrides – use system defaults

        # Main container
        main_frame = ttk.Frame(root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- Folder selection ---
        folder_frame = ttk.LabelFrame(main_frame, text="Folders", padding="5")
        folder_frame.pack(fill=tk.X, pady=5)

        # Source
        src_frame = ttk.Frame(folder_frame)
        src_frame.pack(fill=tk.X, pady=2)
        ttk.Label(src_frame, text="Source:").pack(side=tk.LEFT, padx=5)
        self.src_var = tk.StringVar()
        src_entry = ttk.Entry(src_frame, textvariable=self.src_var)
        src_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(src_frame, text="Browse...", command=lambda: self.browse_folder(self.src_var)).pack(side=tk.RIGHT, padx=5)

        # Reference
        ref_frame = ttk.Frame(folder_frame)
        ref_frame.pack(fill=tk.X, pady=2)
        ttk.Label(ref_frame, text="Reference:").pack(side=tk.LEFT, padx=5)
        self.ref_var = tk.StringVar()
        ref_entry = ttk.Entry(ref_frame, textvariable=self.ref_var)
        ref_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(ref_frame, text="Browse...", command=lambda: self.browse_folder(self.ref_var)).pack(side=tk.RIGHT, padx=5)

        # Destination
        dst_frame = ttk.Frame(folder_frame)
        dst_frame.pack(fill=tk.X, pady=2)
        ttk.Label(dst_frame, text="Destination:").pack(side=tk.LEFT, padx=5)
        self.dst_var = tk.StringVar()
        dst_entry = ttk.Entry(dst_frame, textvariable=self.dst_var)
        dst_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(dst_frame, text="Browse...", command=lambda: self.browse_folder(self.dst_var)).pack(side=tk.RIGHT, padx=5)

        # --- Options ---
        options_frame = ttk.LabelFrame(main_frame, text="Options", padding="5")
        options_frame.pack(fill=tk.X, pady=5)

        # Method
        method_frame = ttk.Frame(options_frame)
        method_frame.pack(fill=tk.X, pady=2)
        ttk.Label(method_frame, text="Detection method:").pack(side=tk.LEFT, padx=5)
        self.method_var = tk.StringVar(value="name-size")
        ttk.Radiobutton(method_frame, text="Name+Size", variable=self.method_var,
                        value="name-size", command=self.update_ui).pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(method_frame, text="Content only", variable=self.method_var,
                        value="content", command=self.update_ui).pack(side=tk.LEFT, padx=5)

        # Hash options
        hash_frame = ttk.Frame(options_frame)
        hash_frame.pack(fill=tk.X, pady=2)
        self.use_hash_var = tk.BooleanVar(value=False)
        self.use_hash_cb = ttk.Checkbutton(hash_frame, text="Verify with hash (name+size mode only)",
                                            variable=self.use_hash_var)
        self.use_hash_cb.pack(side=tk.LEFT, padx=5)
        ttk.Label(hash_frame, text="Algorithm:").pack(side=tk.LEFT, padx=5)
        self.algo_var = tk.StringVar(value="md5")
        algo_combo = ttk.Combobox(hash_frame, textvariable=self.algo_var, values=["md5", "sha1", "sha256"],
                                   state="readonly", width=8)
        algo_combo.pack(side=tk.LEFT)

        # Database option
        db_frame = ttk.Frame(options_frame)
        db_frame.pack(fill=tk.X, pady=2)
        self.use_db_var = tk.BooleanVar(value=False)
        self.use_db_cb = ttk.Checkbutton(db_frame, text="Use SQLite database (for huge folders)",
                                          variable=self.use_db_var, command=self.update_ui)
        self.use_db_cb.pack(side=tk.LEFT, padx=5)
        self.db_var = tk.StringVar()
        self.db_entry = ttk.Entry(db_frame, textvariable=self.db_var, state='disabled')
        self.db_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        self.db_btn = ttk.Button(db_frame, text="Browse...", command=self.browse_db_file, state='disabled')
        self.db_btn.pack(side=tk.RIGHT, padx=5)

        # Dry run
        self.dry_run_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(options_frame, text="Dry run (preview only, no files moved)",
                        variable=self.dry_run_var).pack(anchor=tk.W, padx=5, pady=2)

        # --- Buttons ---
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=10)
        self.start_btn = ttk.Button(btn_frame, text="Start", command=self.start_processing)
        self.start_btn.pack(side=tk.LEFT, padx=5)
        self.cancel_btn = ttk.Button(btn_frame, text="Cancel", command=self.cancel_processing, state='disabled')
        self.cancel_btn.pack(side=tk.LEFT, padx=5)

        # --- Progress bar ---
        self.progress = ttk.Progressbar(main_frame, orient=tk.HORIZONTAL, length=100, mode='determinate')
        self.progress.pack(fill=tk.X, pady=5)

        # --- Log area ---
        log_frame = ttk.LabelFrame(main_frame, text="Log", padding="5")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, font=("Consolas", 10))
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # Queue for thread communication
        self.queue = queue.Queue()
        self.worker = None
        self.check_queue()

    def browse_folder(self, var):
        folder = filedialog.askdirectory()
        if folder:
            var.set(folder)

    def browse_db_file(self):
        file = filedialog.asksaveasfilename(defaultextension=".db",
                                             filetypes=[("SQLite DB", "*.db"), ("All files", "*.*")])
        if file:
            self.db_var.set(file)

    def update_ui(self):
        """Enable/disable widgets based on selections."""
        method = self.method_var.get()
        if method == 'content':
            self.use_hash_cb.config(state='disabled')
        else:
            self.use_hash_cb.config(state='normal')

        if self.use_db_var.get():
            self.db_entry.config(state='normal')
            self.db_btn.config(state='normal')
        else:
            self.db_entry.config(state='disabled')
            self.db_btn.config(state='disabled')

    def log_message(self, msg):
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)

    def start_processing(self):
        # Validate inputs
        src = self.src_var.get().strip()
        ref = self.ref_var.get().strip()
        dst = self.dst_var.get().strip()
        if not src or not ref or not dst:
            self.log_message("Error: Please select all three folders.")
            return
        if not os.path.isdir(src):
            self.log_message("Error: Source folder does not exist.")
            return
        if not os.path.isdir(ref):
            self.log_message("Error: Reference folder does not exist.")
            return
        # Destination will be created if needed

        method = self.method_var.get()
        use_hash = self.use_hash_var.get() if method == 'name-size' else True  # content mode always uses hash
        hash_algo = self.algo_var.get()
        dry_run = self.dry_run_var.get()
        use_db = self.db_var.get().strip() if self.use_db_var.get() else None

        # Disable start, enable cancel
        self.start_btn.config(state='disabled')
        self.cancel_btn.config(state='normal')
        self.progress['value'] = 0
        self.progress['maximum'] = 100  # will be updated by worker
        self.log_text.delete(1.0, tk.END)

        # Start worker thread
        self.worker = DuplicateWorker(
            self.queue, src, ref, dst, method, use_hash, hash_algo, dry_run, use_db
        )
        self.worker.start()

    def cancel_processing(self):
        if self.worker:
            self.worker.cancel()
            self.log_message("Cancelling... (please wait)")

    def check_queue(self):
        """Check for messages from the worker thread and update GUI."""
        try:
            while True:
                msg = self.queue.get_nowait()
                if msg[0] == 'progress':
                    _, current, total = msg
                    self.progress['maximum'] = total
                    self.progress['value'] = current
                elif msg[0] == 'msg':
                    _, text = msg
                    self.log_message(text)
                elif msg[0] == 'error':
                    _, err = msg
                    self.log_message(f"ERROR: {err}")
                elif msg[0] == 'finished':
                    self.start_btn.config(state='normal')
                    self.cancel_btn.config(state='disabled')
                    self.worker = None
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.check_queue)

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
if __name__ == "__main__":
    root = tk.Tk()
    app = DuplicateMoverApp(root)
    root.mainloop()