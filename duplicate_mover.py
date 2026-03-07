#!/usr/bin/env python3
"""
Duplicate File Mover

Find duplicate files between two folders (including subfolders) and move them
from a source folder to a destination folder while preserving the relative path.
Duplicates are identified by file name + size, and optionally by a cryptographic
hash (MD5, SHA‑1, etc.) to confirm identical content.

Usage:
    python duplicate_mover.py /path/to/source /path/to/reference /path/to/destination [--use-hash] [--hash-algo md5]
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
    """
    Compute the hash of a file using the specified algorithm.
    Reads the file in chunks to handle large files efficiently.
    """
    hasher = hashlib.new(algorithm)
    with open(filepath, 'rb') as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()

# ----------------------------------------------------------------------
# Scanning functions
# ----------------------------------------------------------------------
def build_reference_index(ref_folder, use_hash=False, hash_algo='md5'):
    """
    Walk through the reference folder and build an index of files.
    Key: (file_name, file_size)
    Value: list of tuples (full_path, hash) – hash is computed only if use_hash is True.
    """
    index = defaultdict(list)
    ref_folder = os.path.abspath(ref_folder)

    for root, dirs, files in os.walk(ref_folder):
        for file in files:
            full_path = os.path.join(root, file)
            try:
                size = os.path.getsize(full_path)
                key = (file, size)

                if use_hash:
                    file_hash = compute_file_hash(full_path, hash_algo)
                else:
                    file_hash = None

                index[key].append((full_path, file_hash))
            except (OSError, PermissionError) as e:
                print(f"Warning: Cannot access {full_path}: {e}")

    return index

def find_duplicates(source_folder, ref_index, use_hash=False, hash_algo='md5'):
    """
    Walk through the source folder and check each file against the reference index.
    Returns a list of tuples (source_path, dest_path) for files that are duplicates.
    """
    duplicates = []
    source_folder = os.path.abspath(source_folder)

    for root, dirs, files in os.walk(source_folder):
        for file in files:
            source_path = os.path.join(root, file)
            try:
                size = os.path.getsize(source_path)
                key = (file, size)

                # If key not in index, no possible duplicate by name+size
                if key not in ref_index:
                    continue

                # Candidate files in reference with same name and size
                candidates = ref_index[key]

                # If hash verification is disabled, first candidate is enough
                if not use_hash:
                    # Found a duplicate (by name+size)
                    rel_path = os.path.relpath(source_path, source_folder)
                    dest_path = os.path.join(args.destination, rel_path)
                    duplicates.append((source_path, dest_path))
                    continue

                # Hash verification enabled: compute source file hash once
                source_hash = compute_file_hash(source_path, hash_algo)

                # Check against all candidates until a match is found
                for cand_path, cand_hash in candidates:
                    # If candidate hash was not precomputed, compute it now
                    if cand_hash is None:
                        cand_hash = compute_file_hash(cand_path, hash_algo)
                        # Update the stored hash for future use (optional, but index is reused)
                        # Not updating because index is a local structure; fine to recompute once per candidate per source file.

                    if source_hash == cand_hash:
                        rel_path = os.path.relpath(source_path, source_folder)
                        dest_path = os.path.join(args.destination, rel_path)
                        duplicates.append((source_path, dest_path))
                        break   # Stop after first matching candidate

            except (OSError, PermissionError) as e:
                print(f"Warning: Cannot process {source_path}: {e}")

    return duplicates

# ----------------------------------------------------------------------
# Moving files
# ----------------------------------------------------------------------
def move_duplicates(duplicates, dry_run=False):
    """
    Move files from source to destination, creating parent directories as needed.
    If dry_run is True, only print what would be done.
    """
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
    parser.add_argument('--use-hash', action='store_true',
                        help='Verify duplicates by comparing file hash (slower but safer)')
    parser.add_argument('--hash-algo', default='md5', choices=['md5', 'sha1', 'sha256'],
                        help='Hash algorithm to use (default: md5)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Only list files that would be moved, do not actually move')

    global args   # for use in functions
    args = parser.parse_args()

    # Validate folders
    for folder in [args.source, args.reference]:
        print(folder)
        if not os.path.isdir(folder):
            print(f"Error: Folder does not exist: {folder}")
            return 1

    # Create destination folder if it doesn't exist (will also be created during move)
    os.makedirs(args.destination, exist_ok=True)

    print("Building reference index...")
    ref_index = build_reference_index(args.reference, use_hash=args.use_hash, hash_algo=args.hash_algo)
    print(f"Indexed {sum(len(v) for v in ref_index.values())} files in reference.")

    print("Scanning source folder for duplicates...")
    duplicates = find_duplicates(args.source, ref_index, use_hash=args.use_hash, hash_algo=args.hash_algo)
    print(f"Found {len(duplicates)} duplicate files in source.")

    if not duplicates:
        print("No duplicates to move.")
        return 0

    print("Moving duplicates...")
    move_duplicates(duplicates, dry_run=args.dry_run)

    return 0

if __name__ == '__main__':
    exit(main())