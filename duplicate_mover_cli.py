#!/usr/bin/env python3
"""
Duplicate File Mover – with optional SQLite backend

Two modes:
  name-size : identify duplicates by name + size (optionally verify with hash)
  content   : identify duplicates by content only (always uses hash)

Files found to be duplicates in the source folder are moved to a destination,
preserving relative paths. For very large reference folders, use --db to store
the index on disk (SQLite) instead of in memory.
"""

import os
import shutil
import hashlib
import argparse
import sqlite3
from collections import defaultdict

# ----------------------------------------------------------------------
# Hashing utilities
# ----------------------------------------------------------------------
def compute_file_hash(filepath, algorithm='md5', chunk_size=8192):
    """Compute hash of a file, reading in chunks."""
    hasher = hashlib.new(algorithm)
    with open(filepath, 'rb') as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()

# ----------------------------------------------------------------------
# In‑memory index (original, fast for moderate sizes)
# ----------------------------------------------------------------------
def build_index_memory(ref_folder, method, use_hash=False, hash_algo='md5'):
    """Build in‑memory index (dictionaries)."""
    if method == 'name-size':
        index = defaultdict(list)          # (name, size) -> list of (path, hash)
    else:  # content
        index = defaultdict(lambda: defaultdict(list))  # size -> {hash: [path, ...]}

    ref_folder = os.path.abspath(ref_folder)
    for root, dirs, files in os.walk(ref_folder):
        for file in files:
            full_path = os.path.join(root, file)
            try:
                size = os.path.getsize(full_path)
                if method == 'name-size':
                    key = (file, size)
                    h = compute_file_hash(full_path, hash_algo) if use_hash else None
                    index[key].append((full_path, h))
                else:  # content
                    file_hash = compute_file_hash(full_path, hash_algo)
                    index[size][file_hash].append(full_path)
            except (OSError, PermissionError) as e:
                print(f"Warning: Cannot access {full_path}: {e}")
    return index

def find_duplicates_memory(source_folder, index, method, use_hash=False, hash_algo='md5'):
    """Find duplicates using in‑memory index."""
    duplicates = []
    source_folder = os.path.abspath(source_folder)
    for root, dirs, files in os.walk(source_folder):
        for file in files:
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
                        dest_path = os.path.join(args.destination, rel_path)
                        duplicates.append((source_path, dest_path))
                        continue
                    source_hash = compute_file_hash(source_path, hash_algo)
                    for cand_path, cand_hash in candidates:
                        if cand_hash is None:
                            cand_hash = compute_file_hash(cand_path, hash_algo)
                        if source_hash == cand_hash:
                            rel_path = os.path.relpath(source_path, source_folder)
                            dest_path = os.path.join(args.destination, rel_path)
                            duplicates.append((source_path, dest_path))
                            break
                else:  # content
                    if size not in index:
                        continue
                    source_hash = compute_file_hash(source_path, hash_algo)
                    if source_hash in index[size]:
                        rel_path = os.path.relpath(source_path, source_folder)
                        dest_path = os.path.join(args.destination, rel_path)
                        duplicates.append((source_path, dest_path))
            except (OSError, PermissionError) as e:
                print(f"Warning: Cannot process {source_path}: {e}")
    return duplicates

# ----------------------------------------------------------------------
# SQLite‑based index (scales to huge collections)
# ----------------------------------------------------------------------
def init_db(db_path):
    """Create database and tables if they don't exist."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Table for name-size mode (hash may be NULL)
    c.execute('''CREATE TABLE IF NOT EXISTS files_name_size
                 (name TEXT, size INTEGER, path TEXT, hash TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_name_size ON files_name_size (name, size)')
    # Table for content mode (size + hash)
    c.execute('''CREATE TABLE IF NOT EXISTS files_content
                 (size INTEGER, hash TEXT, path TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_size_hash ON files_content (size, hash)')
    conn.commit()
    return conn

def build_index_db(ref_folder, db_path, method, use_hash=False, hash_algo='md5'):
    """Build index in SQLite database."""
    conn = init_db(db_path)
    c = conn.cursor()
    # Clear existing data (fresh index)
    c.execute("DELETE FROM files_name_size")
    c.execute("DELETE FROM files_content")
    conn.commit()

    ref_folder = os.path.abspath(ref_folder)
    batch = []
    batch_size = 1000
    total = 0

    for root, dirs, files in os.walk(ref_folder):
        for file in files:
            full_path = os.path.join(root, file)
            try:
                size = os.path.getsize(full_path)
                if method == 'name-size':
                    h = compute_file_hash(full_path, hash_algo) if use_hash else None
                    batch.append((file, size, full_path, h))
                else:  # content
                    h = compute_file_hash(full_path, hash_algo)
                    batch.append((size, h, full_path))
                total += 1
                if len(batch) >= batch_size:
                    if method == 'name-size':
                        c.executemany("INSERT INTO files_name_size VALUES (?,?,?,?)", batch)
                    else:
                        c.executemany("INSERT INTO files_content VALUES (?,?,?)", batch)
                    conn.commit()
                    batch.clear()
                    print(f"Indexed {total} files...", end='\r')
            except (OSError, PermissionError) as e:
                print(f"Warning: Cannot access {full_path}: {e}")

    # Insert remaining
    if batch:
        if method == 'name-size':
            c.executemany("INSERT INTO files_name_size VALUES (?,?,?,?)", batch)
        else:
            c.executemany("INSERT INTO files_content VALUES (?,?,?)", batch)
        conn.commit()
    print(f"Indexed {total} files.                           ")
    return conn

def find_duplicates_db(source_folder, conn, method, use_hash=False, hash_algo='md5'):
    """Find duplicates using SQLite index."""
    duplicates = []
    source_folder = os.path.abspath(source_folder)
    c = conn.cursor()

    for root, dirs, files in os.walk(source_folder):
        for file in files:
            source_path = os.path.join(root, file)
            try:
                size = os.path.getsize(source_path)
                if method == 'name-size':
                    if not use_hash:
                        # Simple name+size check
                        c.execute("SELECT 1 FROM files_name_size WHERE name=? AND size=? LIMIT 1",
                                  (file, size))
                        if c.fetchone():
                            rel_path = os.path.relpath(source_path, source_folder)
                            dest_path = os.path.join(args.destination, rel_path)
                            duplicates.append((source_path, dest_path))
                    else:
                        # Need hash comparison
                        source_hash = compute_file_hash(source_path, hash_algo)
                        c.execute("SELECT 1 FROM files_name_size WHERE name=? AND size=? AND hash=? LIMIT 1",
                                  (file, size, source_hash))
                        if c.fetchone():
                            rel_path = os.path.relpath(source_path, source_folder)
                            dest_path = os.path.join(args.destination, rel_path)
                            duplicates.append((source_path, dest_path))
                else:  # content
                    source_hash = compute_file_hash(source_path, hash_algo)
                    c.execute("SELECT 1 FROM files_content WHERE size=? AND hash=? LIMIT 1",
                              (size, source_hash))
                    if c.fetchone():
                        rel_path = os.path.relpath(source_path, source_folder)
                        dest_path = os.path.join(args.destination, rel_path)
                        duplicates.append((source_path, dest_path))
            except (OSError, PermissionError) as e:
                print(f"Warning: Cannot process {source_path}: {e}")
    return duplicates

# ----------------------------------------------------------------------
# Moving files
# ----------------------------------------------------------------------
def move_duplicates(duplicates, dry_run=False):
    """Move files, creating destination directories as needed."""
    for src, dst in duplicates:
        dst_dir = os.path.dirname(dst)
        if not dry_run:
            os.makedirs(dst_dir, exist_ok=True)
            try:
                shutil.move(src, dst)
                print(f"Moved: {src} -> {dst}")
            except Exception as e:
                print(f"Error moving {src}: {e}")
        else:
            print(f"[DRY RUN] Would move: {src} -> {dst}")

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Find duplicate files between two folders and move them from source to destination."
    )
    parser.add_argument('source', help='Folder from which duplicate files will be moved')
    parser.add_argument('reference', help='Reference folder to compare against')
    parser.add_argument('destination', help='Folder where duplicates will be moved (preserving subpath)')
    parser.add_argument('--method', default='name-size', choices=['name-size', 'content'],
                        help="Detection method: 'name-size' (default) uses name+size; 'content' uses file hash only")
    parser.add_argument('--use-hash', action='store_true',
                        help='[name-size mode only] Verify duplicates by comparing file hash')
    parser.add_argument('--hash-algo', default='md5', choices=['md5', 'sha1', 'sha256'],
                        help='Hash algorithm to use (default: md5)')
    parser.add_argument('--db', metavar='DB_PATH',
                        help='Use SQLite database at DB_PATH for indexing (scales to huge folders). '
                             'If omitted, uses in‑memory dictionaries.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Only list files that would be moved, do not actually move')

    global args
    args = parser.parse_args()

    # Validate folders
    for folder in [args.source, args.reference]:
        if not os.path.isdir(folder):
            print(f"Error: Folder does not exist: {folder}")
            return 1

    os.makedirs(args.destination, exist_ok=True)

    # Build index
    if args.db:
        print(f"Building reference index in SQLite database: {args.db}")
        conn = build_index_db(args.reference, args.db, args.method,
                              use_hash=args.use_hash, hash_algo=args.hash_algo)
    else:
        print("Building reference index in memory...")
        index = build_index_memory(args.reference, args.method,
                                   use_hash=args.use_hash, hash_algo=args.hash_algo)
        conn = None  # not used

    # Find duplicates
    print("Scanning source folder for duplicates...")
    if args.db:
        duplicates = find_duplicates_db(args.source, conn, args.method,
                                        use_hash=args.use_hash, hash_algo=args.hash_algo)
    else:
        duplicates = find_duplicates_memory(args.source, index, args.method,
                                            use_hash=args.use_hash, hash_algo=args.hash_algo)

    print(f"Found {len(duplicates)} duplicate files in source.")

    if not duplicates:
        print("No duplicates to move.")
        if args.db:
            conn.close()
        return 0

    print("Moving duplicates...")
    move_duplicates(duplicates, dry_run=args.dry_run)

    if args.db:
        conn.close()
    return 0

if __name__ == '__main__':
    exit(main())