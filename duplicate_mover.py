#!/usr/bin/env python3
"""
Duplicate File Mover – Two Modes

Mode 1 (name-size): Identify duplicates by file name + size (optionally verify with hash).
Mode 2 (content):   Identify duplicates by file content only (always uses hash).

Files found to be duplicates in the source folder are moved to a destination folder,
preserving the relative directory structure.
"""

import os
import shutil
import hashlib
import argparse
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
# Index building (depends on mode)
# ----------------------------------------------------------------------
def build_reference_index(ref_folder, method, use_hash=False, hash_algo='md5'):
    """
    Build an index of files in the reference folder.
    Returns a dictionary where keys depend on the method:
      - name-size: (filename, size) -> list of (path, hash or None)
      - content:   size -> dict of hash -> list of paths
    """
    index = {}
    ref_folder = os.path.abspath(ref_folder)

    if method == 'name-size':
        idx = defaultdict(list)          # (name, size) -> list of (path, hash)
    else:  # content mode
        idx = defaultdict(lambda: defaultdict(list))  # size -> {hash: [path, ...]}

    for root, dirs, files in os.walk(ref_folder):
        for file in files:
            full_path = os.path.join(root, file)
            try:
                size = os.path.getsize(full_path)
                if method == 'name-size':
                    key = (file, size)
                    h = compute_file_hash(full_path, hash_algo) if use_hash else None
                    idx[key].append((full_path, h))
                else:  # content mode
                    file_hash = compute_file_hash(full_path, hash_algo)
                    idx[size][file_hash].append(full_path)
            except (OSError, PermissionError) as e:
                print(f"Warning: Cannot access {full_path}: {e}")

    return idx

def find_duplicates(source_folder, ref_index, method, use_hash=False, hash_algo='md5'):
    """
    Walk source folder and check each file against the reference index.
    Returns a list of (source_path, dest_path) for files that are duplicates.
    """
    duplicates = []
    source_folder = os.path.abspath(source_folder)

    for root, dirs, files in os.walk(source_folder):
        for file in files:
            source_path = os.path.join(root, file)
            try:
                size = os.path.getsize(source_path)

                if method == 'name-size':
                    key = (file, size)
                    if key not in ref_index:
                        continue
                    candidates = ref_index[key]
                    if not use_hash:
                        # Name+size match is enough
                        rel_path = os.path.relpath(source_path, source_folder)
                        dest_path = os.path.join(args.destination, rel_path)
                        duplicates.append((source_path, dest_path))
                        continue
                    # Verify with hash
                    source_hash = compute_file_hash(source_path, hash_algo)
                    for cand_path, cand_hash in candidates:
                        if cand_hash is None:
                            cand_hash = compute_file_hash(cand_path, hash_algo)
                        if source_hash == cand_hash:
                            rel_path = os.path.relpath(source_path, source_folder)
                            dest_path = os.path.join(args.destination, rel_path)
                            duplicates.append((source_path, dest_path))
                            break

                else:  # content mode
                    # Check if size exists in index
                    if size not in ref_index:
                        continue
                    # Compute source hash
                    source_hash = compute_file_hash(source_path, hash_algo)
                    # Check if hash exists for this size
                    if source_hash in ref_index[size]:
                        # Duplicate found (any file with same size and hash)
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
                        help='[name-size mode only] Verify duplicates by comparing file hash (slower but safer)')
    parser.add_argument('--hash-algo', default='md5', choices=['md5', 'sha1', 'sha256'],
                        help='Hash algorithm to use (default: md5)')
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

    print(f"Building reference index using method '{args.method}'...")
    ref_index = build_reference_index(args.reference, args.method,
                                      use_hash=args.use_hash, hash_algo=args.hash_algo)
    # Count indexed files (roughly)
    if args.method == 'name-size':
        count = sum(len(v) for v in ref_index.values())
    else:
        count = sum(len(v) for size_dict in ref_index.values() for v in size_dict.values())
    print(f"Indexed {count} files in reference.")

    print("Scanning source folder for duplicates...")
    duplicates = find_duplicates(args.source, ref_index, args.method,
                                 use_hash=args.use_hash, hash_algo=args.hash_algo)
    print(f"Found {len(duplicates)} duplicate files in source.")

    if not duplicates:
        print("No duplicates to move.")
        return 0

    print("Moving duplicates...")
    move_duplicates(duplicates, dry_run=args.dry_run)

    return 0

if __name__ == '__main__':
    exit(main())