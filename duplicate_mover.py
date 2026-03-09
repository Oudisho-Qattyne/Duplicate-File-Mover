#!/usr/bin/env python3
"""
Duplicate File Mover – Tkinter GUI with format filter, internal source & reference duplicates
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
# Predefined file format categories (extensions with dot)
# ----------------------------------------------------------------------
FORMAT_CATEGORIES = {
    "Images": ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.svg'],
    "Documents": ['.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt', '.xls', '.xlsx', '.ppt', '.pptx', '.csv'],
    "Audio": ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a'],
    "Video": ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm'],
    "Archives": ['.zip', '.rar', '.7z', '.tar', '.gz', '.bz2'],
    "Code": ['.py', '.js', '.html', '.css', '.cpp', '.java', '.php', '.rb', '.go', '.rs'],
    "Other": ['.exe', '.dll', '.iso', '.img']
}

# ----------------------------------------------------------------------
# Core duplicate detection functions
# ----------------------------------------------------------------------
def compute_file_hash(filepath, algorithm='md5', chunk_size=8192):
    hasher = hashlib.new(algorithm)
    with open(filepath, 'rb') as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()

def matches_format(filename, formats):
    """Return True if filename's extension is in the list of formats."""
    if not formats:
        return True
    ext = os.path.splitext(filename)[1].lower()
    return ext in formats

def build_index_memory(folder, method, use_hash, hash_algo, formats,
                       progress_callback=None, cancel_flag=None):
    if method == 'name-size':
        index = defaultdict(list)
    else:
        index = defaultdict(lambda: defaultdict(list))

    folder = os.path.abspath(folder)
    total_files = 0
    # Count files that match format for progress
    for root, dirs, files in os.walk(folder):
        for file in files:
            if matches_format(file, formats):
                total_files += 1
    processed = 0

    for root, dirs, files in os.walk(folder):
        if cancel_flag and cancel_flag():
            return None
        for file in files:
            if not matches_format(file, formats):
                continue
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

def find_duplicates_memory(source_folder, ref_index, method, use_hash, hash_algo,
                           destination, formats, internal_src=False,
                           progress_callback=None, cancel_flag=None):
    duplicates = set()
    source_folder = os.path.abspath(source_folder)

    # Build source index if internal duplicates requested
    src_index = None
    if internal_src:
        src_index = build_index_memory(source_folder, method, use_hash, hash_algo, formats,
                                       progress_callback=None, cancel_flag=cancel_flag)
        if src_index is None:
            return None

    # Count files for progress
    total_files = 0
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            if matches_format(file, formats):
                total_files += 1
    scanned = 0

    for root, dirs, files in os.walk(source_folder):
        if cancel_flag and cancel_flag():
            return None
        for file in files:
            if not matches_format(file, formats):
                continue
            scanned += 1
            if progress_callback:
                progress_callback(scanned, total_files)
            source_path = os.path.join(root, file)
            try:
                size = os.path.getsize(source_path)
                is_duplicate = False

                # Check against reference index
                if method == 'name-size':
                    key = (file, size)
                    if key in ref_index:
                        candidates = ref_index[key]
                        if not use_hash:
                            is_duplicate = True
                        else:
                            source_hash = compute_file_hash(source_path, hash_algo)
                            for cand_path, cand_hash in candidates:
                                if cand_hash is None:
                                    cand_hash = compute_file_hash(cand_path, hash_algo)
                                if source_hash == cand_hash:
                                    is_duplicate = True
                                    break
                else:  # content
                    if size in ref_index:
                        source_hash = compute_file_hash(source_path, hash_algo)
                        if source_hash in ref_index[size]:
                            is_duplicate = True

                # If not duplicate by reference, check internal source duplicates
                if not is_duplicate and internal_src and src_index:
                    if method == 'name-size':
                        key = (file, size)
                        if key in src_index and len(src_index[key]) > 1:
                            if not use_hash:
                                is_duplicate = True
                            else:
                                source_hash = compute_file_hash(source_path, hash_algo)
                                for cand_path, cand_hash in src_index[key]:
                                    if cand_path == source_path:
                                        continue
                                    if cand_hash is None:
                                        cand_hash = compute_file_hash(cand_path, hash_algo)
                                    if source_hash == cand_hash:
                                        is_duplicate = True
                                        break
                    else:  # content
                        if size in src_index:
                            source_hash = compute_file_hash(source_path, hash_algo)
                            if source_hash in src_index[size] and len(src_index[size][source_hash]) > 1:
                                is_duplicate = True

                if is_duplicate:
                    rel_path = os.path.relpath(source_path, source_folder)
                    dest_path = os.path.join(destination, rel_path)
                    duplicates.add((source_path, dest_path))
                    if progress_callback:
                        progress_callback(msg=f"Found duplicate: {source_path}")

            except (OSError, PermissionError) as e:
                if progress_callback:
                    progress_callback(msg=f"Warning: Cannot process {source_path}: {e}")

    return list(duplicates)

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Tables for reference
    c.execute('''CREATE TABLE IF NOT EXISTS files_name_size
                 (name TEXT, size INTEGER, path TEXT, hash TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_name_size ON files_name_size (name, size)')
    c.execute('''CREATE TABLE IF NOT EXISTS files_content
                 (size INTEGER, hash TEXT, path TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_size_hash ON files_content (size, hash)')
    # Tables for source (internal duplicates)
    c.execute('''CREATE TABLE IF NOT EXISTS source_name_size
                 (name TEXT, size INTEGER, path TEXT, hash TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS src_idx_name_size ON source_name_size (name, size)')
    c.execute('''CREATE TABLE IF NOT EXISTS source_content
                 (size INTEGER, hash TEXT, path TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS src_idx_size_hash ON source_content (size, hash)')
    conn.commit()
    return conn

def build_index_db(folder, db_path, method, use_hash, hash_algo, formats,
                   table_prefix='', progress_callback=None, cancel_flag=None):
    """Build index in SQLite database with optional table prefix."""
    conn = init_db(db_path) if not table_prefix else sqlite3.connect(db_path)
    c = conn.cursor()
    # Clear existing data in the relevant tables
    if method == 'name-size':
        c.execute(f"DELETE FROM {table_prefix}name_size")
    else:
        c.execute(f"DELETE FROM {table_prefix}content")
    conn.commit()

    folder = os.path.abspath(folder)
    batch = []
    batch_size = 1000
    # Count files for progress
    total_files = 0
    for root, dirs, files in os.walk(folder):
        for file in files:
            if matches_format(file, formats):
                total_files += 1
    processed = 0

    for root, dirs, files in os.walk(folder):
        if cancel_flag and cancel_flag():
            conn.close()
            return None
        for file in files:
            if not matches_format(file, formats):
                continue
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
                        c.executemany(f"INSERT INTO {table_prefix}name_size VALUES (?,?,?,?)", batch)
                    else:
                        c.executemany(f"INSERT INTO {table_prefix}content VALUES (?,?,?)", batch)
                    conn.commit()
                    batch.clear()
            except (OSError, PermissionError) as e:
                if progress_callback:
                    progress_callback(msg=f"Warning: {full_path} – {e}")
    if batch:
        if method == 'name-size':
            c.executemany(f"INSERT INTO {table_prefix}name_size VALUES (?,?,?,?)", batch)
        else:
            c.executemany(f"INSERT INTO {table_prefix}content VALUES (?,?,?)", batch)
        conn.commit()
    return conn

def find_duplicates_db(source_folder, conn, method, use_hash, hash_algo,
                       destination, formats, internal_src=False,
                       progress_callback=None, cancel_flag=None):
    duplicates = set()
    source_folder = os.path.abspath(source_folder)
    c = conn.cursor()

    # If internal duplicates requested, build source index in the same DB (different tables)
    if internal_src:
        src_conn = build_index_db(source_folder, os.path.abspath(conn.execute("PRAGMA database").fetchone()[0]),
                                  method, use_hash, hash_algo, formats,
                                  table_prefix='source_', progress_callback=None, cancel_flag=cancel_flag)
        if src_conn is None:
            return None
    else:
        src_conn = None

    # Count files in source for progress
    total_files = 0
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            if matches_format(file, formats):
                total_files += 1
    scanned = 0

    for root, dirs, files in os.walk(source_folder):
        if cancel_flag and cancel_flag():
            return None
        for file in files:
            if not matches_format(file, formats):
                continue
            scanned += 1
            if progress_callback:
                progress_callback(scanned, total_files)
            source_path = os.path.join(root, file)
            try:
                size = os.path.getsize(source_path)
                is_duplicate = False

                # Check reference tables
                if method == 'name-size':
                    if not use_hash:
                        c.execute("SELECT 1 FROM files_name_size WHERE name=? AND size=? LIMIT 1",
                                  (file, size))
                        if c.fetchone():
                            is_duplicate = True
                    else:
                        source_hash = compute_file_hash(source_path, hash_algo)
                        c.execute("SELECT 1 FROM files_name_size WHERE name=? AND size=? AND hash=? LIMIT 1",
                                  (file, size, source_hash))
                        if c.fetchone():
                            is_duplicate = True
                else:  # content
                    source_hash = compute_file_hash(source_path, hash_algo)
                    c.execute("SELECT 1 FROM files_content WHERE size=? AND hash=? LIMIT 1",
                              (size, source_hash))
                    if c.fetchone():
                        is_duplicate = True

                # If not duplicate by reference, check internal source duplicates
                if not is_duplicate and internal_src and src_conn:
                    src_c = src_conn.cursor()
                    if method == 'name-size':
                        if not use_hash:
                            src_c.execute("SELECT COUNT(*) FROM source_name_size WHERE name=? AND size=?",
                                          (file, size))
                            count = src_c.fetchone()[0]
                            if count > 1:
                                is_duplicate = True
                        else:
                            source_hash = compute_file_hash(source_path, hash_algo)
                            src_c.execute("SELECT COUNT(*) FROM source_name_size WHERE name=? AND size=? AND hash=?",
                                          (file, size, source_hash))
                            count = src_c.fetchone()[0]
                            if count > 1:
                                is_duplicate = True
                    else:  # content
                        source_hash = compute_file_hash(source_path, hash_algo)
                        src_c.execute("SELECT COUNT(*) FROM source_content WHERE size=? AND hash=?",
                                      (size, source_hash))
                        count = src_c.fetchone()[0]
                        if count > 1:
                            is_duplicate = True

                if is_duplicate:
                    rel_path = os.path.relpath(source_path, source_folder)
                    dest_path = os.path.join(destination, rel_path)
                    duplicates.add((source_path, dest_path))
                    if progress_callback:
                        progress_callback(msg=f"Found duplicate: {source_path}")

            except (OSError, PermissionError) as e:
                if progress_callback:
                    progress_callback(msg=f"Warning: Cannot process {source_path}: {e}")

    if src_conn:
        src_conn.close()
    return list(duplicates)

def find_internal_duplicates_memory(index, method, use_hash, hash_algo, folder_path,
                                     progress_callback=None, cancel_flag=None):
    """Log internal duplicates found in an in‑memory index (no moving)."""
    count = 0
    if method == 'name-size':
        for key, paths_hashes in index.items():
            if cancel_flag and cancel_flag():
                return
            if len(paths_hashes) > 1:
                if not use_hash:
                    # All files with same name+size are duplicates
                    paths = [p for p, _ in paths_hashes]
                    count += 1
                    if progress_callback:
                        progress_callback(msg=f"Internal duplicate in {os.path.basename(folder_path)}: {paths[0]} and {paths[1]}")
                else:
                    # Group by hash
                    hash_groups = defaultdict(list)
                    for path, h in paths_hashes:
                        if h is None:
                            h = compute_file_hash(path, hash_algo)
                        hash_groups[h].append(path)
                    for h, paths in hash_groups.items():
                        if len(paths) > 1:
                            count += 1
                            if progress_callback:
                                progress_callback(msg=f"Internal duplicate in {os.path.basename(folder_path)}: {paths[0]} and {paths[1]}")
    else:  # content mode: index is size -> {hash: [paths]}
        for size, hash_dict in index.items():
            if cancel_flag and cancel_flag():
                return
            for h, paths in hash_dict.items():
                if len(paths) > 1:
                    count += 1
                    if progress_callback:
                        progress_callback(msg=f"Internal duplicate in {os.path.basename(folder_path)}: {paths[0]} and {paths[1]}")
    if count > 0 and progress_callback:
        progress_callback(msg=f"Found {count} internal duplicate sets in {os.path.basename(folder_path)}.")

def find_internal_duplicates_db(conn, method, use_hash, hash_algo, folder_path, table_prefix,
                                 progress_callback=None, cancel_flag=None):
    """Log internal duplicates from SQLite tables."""
    c = conn.cursor()
    count = 0
    if method == 'name-size':
        # First, find groups with same name+size and more than one file
        if not use_hash:
            c.execute(f"SELECT name, size, COUNT(*) FROM {table_prefix}name_size GROUP BY name, size HAVING COUNT(*) > 1")
            groups = c.fetchall()
            for name, size, cnt in groups:
                if cancel_flag and cancel_flag():
                    return
                c.execute(f"SELECT path FROM {table_prefix}name_size WHERE name=? AND size=? LIMIT 2", (name, size))
                paths = [row[0] for row in c.fetchall()]
                count += 1
                if progress_callback:
                    progress_callback(msg=f"Internal duplicate in {os.path.basename(folder_path)}: {paths[0]} and {paths[1]}")
        else:
            # Need to group by name, size, hash
            c.execute(f"SELECT name, size, hash, COUNT(*) FROM {table_prefix}name_size GROUP BY name, size, hash HAVING COUNT(*) > 1")
            groups = c.fetchall()
            for name, size, h, cnt in groups:
                if cancel_flag and cancel_flag():
                    return
                c.execute(f"SELECT path FROM {table_prefix}name_size WHERE name=? AND size=? AND hash=? LIMIT 2", (name, size, h))
                paths = [row[0] for row in c.fetchall()]
                count += 1
                if progress_callback:
                    progress_callback(msg=f"Internal duplicate in {os.path.basename(folder_path)}: {paths[0]} and {paths[1]}")
    else:  # content
        c.execute(f"SELECT size, hash, COUNT(*) FROM {table_prefix}content GROUP BY size, hash HAVING COUNT(*) > 1")
        groups = c.fetchall()
        for size, h, cnt in groups:
            if cancel_flag and cancel_flag():
                return
            c.execute(f"SELECT path FROM {table_prefix}content WHERE size=? AND hash=? LIMIT 2", (size, h))
            paths = [row[0] for row in c.fetchall()]
            count += 1
            if progress_callback:
                progress_callback(msg=f"Internal duplicate in {os.path.basename(folder_path)}: {paths[0]} and {paths[1]}")
    if count > 0 and progress_callback:
        progress_callback(msg=f"Found {count} internal duplicate sets in {os.path.basename(folder_path)}.")

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
# Worker thread
# ----------------------------------------------------------------------
class DuplicateWorker(threading.Thread):
    def __init__(self, queue, source, reference, destination, method,
                 use_hash, hash_algo, dry_run, use_db, formats,
                 internal_src, internal_ref):
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
        self.formats = formats
        self.internal_src = internal_src
        self.internal_ref = internal_ref
        self._cancel = False

    def cancel(self):
        self._cancel = True

    def progress_callback(self, current=None, total=None, msg=None):
        if msg is not None:
            self.queue.put(('msg', msg))
        if current is not None and total is not None:
            self.queue.put(('progress', current, total))

    def run(self):
        try:
            # Phase 1: Build reference index
            self.progress_callback(msg="Building reference index...")
            if self.use_db:
                ref_conn = build_index_db(
                    self.reference, self.use_db, self.method, self.use_hash, self.hash_algo, self.formats,
                    table_prefix='files_', progress_callback=self.progress_callback, cancel_flag=lambda: self._cancel
                )
                if ref_conn is None:
                    self.queue.put(('finished',))
                    return
                # Optionally find internal duplicates in reference
                if self.internal_ref:
                    self.progress_callback(msg="Checking for internal duplicates in reference folder...")
                    find_internal_duplicates_db(ref_conn, self.method, self.use_hash, self.hash_algo,
                                                 self.reference, 'files_',
                                                 progress_callback=self.progress_callback, cancel_flag=lambda: self._cancel)
            else:
                ref_index = build_index_memory(
                    self.reference, self.method, self.use_hash, self.hash_algo, self.formats,
                    progress_callback=self.progress_callback, cancel_flag=lambda: self._cancel
                )
                if ref_index is None:
                    self.queue.put(('finished',))
                    return
                if self.internal_ref:
                    self.progress_callback(msg="Checking for internal duplicates in reference folder...")
                    find_internal_duplicates_memory(ref_index, self.method, self.use_hash, self.hash_algo,
                                                     self.reference,
                                                     progress_callback=self.progress_callback, cancel_flag=lambda: self._cancel)

            # Phase 2: Scan source for duplicates to move
            self.progress_callback(msg="Scanning source folder for duplicates...")
            if self.use_db:
                duplicates = find_duplicates_db(
                    self.source, ref_conn, self.method, self.use_hash, self.hash_algo, self.destination,
                    self.formats, self.internal_src,
                    progress_callback=self.progress_callback, cancel_flag=lambda: self._cancel
                )
                ref_conn.close()
            else:
                duplicates = find_duplicates_memory(
                    self.source, ref_index, self.method, self.use_hash, self.hash_algo, self.destination,
                    self.formats, self.internal_src,
                    progress_callback=self.progress_callback, cancel_flag=lambda: self._cancel
                )
            if duplicates is None:
                self.queue.put(('finished',))
                return

            self.progress_callback(msg=f"Found {len(duplicates)} duplicate files to move.")
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
# GUI Application
# ----------------------------------------------------------------------
class DuplicateMoverApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Duplicate File Mover")
        self.root.geometry("850x750")
        self.root.minsize(750, 650)

        style = ttk.Style()
        available_themes = style.theme_names()
        if 'clam' in available_themes:
            style.theme_use('clam')
        elif 'vista' in available_themes:
            style.theme_use('vista')
        elif 'alt' in available_themes:
            style.theme_use('alt')

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

        # Internal duplicates checkboxes
        internal_frame = ttk.Frame(options_frame)
        internal_frame.pack(fill=tk.X, pady=2)
        self.internal_src_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(internal_frame, text="Find internal duplicates in source folder (files to move)",
                        variable=self.internal_src_var).pack(side=tk.LEFT, padx=5)
        self.internal_ref_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(internal_frame, text="Find internal duplicates in reference folder (log only, no move)",
                        variable=self.internal_ref_var).pack(side=tk.LEFT, padx=5)

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

        # --- File type selection (categories) ---
        type_frame = ttk.LabelFrame(main_frame, text="File Types to Process", padding="5")
        type_frame.pack(fill=tk.X, pady=5)

        # Container for checkboxes
        cb_container = ttk.Frame(type_frame)
        cb_container.pack(fill=tk.X, pady=2)

        self.category_vars = {}
        categories = list(FORMAT_CATEGORIES.keys())
        # Arrange in 3 columns
        for i, cat in enumerate(categories):
            var = tk.BooleanVar(value=False)
            self.category_vars[cat] = var
            cb = ttk.Checkbutton(cb_container, text=cat, variable=var)
            row = i // 3
            col = i % 3
            cb.grid(row=row, column=col, sticky='w', padx=10, pady=2)

        # "Other extensions" entry (for custom types)
        other_frame = ttk.Frame(type_frame)
        other_frame.pack(fill=tk.X, pady=5)
        ttk.Label(other_frame, text="Other extensions (comma separated, e.g. .dat,.log):").pack(side=tk.LEFT, padx=5)
        self.other_formats_var = tk.StringVar()
        other_entry = ttk.Entry(other_frame, textvariable=self.other_formats_var)
        other_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        # Select/Clear all buttons
        btn_sel_frame = ttk.Frame(type_frame)
        btn_sel_frame.pack(fill=tk.X, pady=2)
        ttk.Button(btn_sel_frame, text="Select All", command=self.select_all_types).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_sel_frame, text="Clear All", command=self.clear_all_types).pack(side=tk.LEFT, padx=5)

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

    def select_all_types(self):
        for var in self.category_vars.values():
            var.set(True)

    def clear_all_types(self):
        for var in self.category_vars.values():
            var.set(False)

    def log_message(self, msg):
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)

    def get_selected_formats(self):
        """Return list of selected extensions (from categories + custom)."""
        formats = []
        # Add extensions from checked categories
        for cat, var in self.category_vars.items():
            if var.get():
                formats.extend(FORMAT_CATEGORIES[cat])
        # Add custom extensions
        other = self.other_formats_var.get().strip()
        if other:
            parts = [p.strip().lower() for p in other.split(',') if p.strip()]
            for p in parts:
                if not p.startswith('.'):
                    p = '.' + p
                formats.append(p)
        return formats

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

        method = self.method_var.get()
        use_hash = self.use_hash_var.get() if method == 'name-size' else True
        hash_algo = self.algo_var.get()
        dry_run = self.dry_run_var.get()
        use_db = self.db_var.get().strip() if self.use_db_var.get() else None

        formats = self.get_selected_formats()
        internal_src = self.internal_src_var.get()
        internal_ref = self.internal_ref_var.get()

        self.start_btn.config(state='disabled')
        self.cancel_btn.config(state='normal')
        self.progress['value'] = 0
        self.progress['maximum'] = 100
        self.log_text.delete(1.0, tk.END)

        self.worker = DuplicateWorker(
            self.queue, src, ref, dst, method, use_hash, hash_algo, dry_run, use_db,
            formats, internal_src, internal_ref
        )
        self.worker.start()

    def cancel_processing(self):
        if self.worker:
            self.worker.cancel()
            self.log_message("Cancelling... (please wait)")

    def check_queue(self):
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