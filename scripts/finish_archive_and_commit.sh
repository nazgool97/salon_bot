#!/usr/bin/env bash
set -euo pipefail

# Moves migration files from `bot/migrations/versions/` into
# `bot/migrations/versions/archive/` except the squashed baseline.
# Run this from the repository root and review the staged changes
# before pushing.

ARCHIVE_DIR="bot/migrations/versions/archive"
KEEP_FILE="20251203_squash_initial.py"

mkdir -p "$ARCHIVE_DIR"

echo "Moving migration files into $ARCHIVE_DIR (keeping $KEEP_FILE)"
shopt -s nullglob
for f in bot/migrations/versions/*.py; do
  base=$(basename "$f")
  if [[ "$base" == "$KEEP_FILE" ]]; then
    echo "Keeping $base"
    continue
  fi
  echo "Moving $base -> archive/"
  git mv "$f" "$ARCHIVE_DIR/"
done

echo "Staging any remaining non-py files (if present)"
git add bot/migrations/versions/

echo "Commit with message: Archive old migrations, keep squashed baseline"
git commit -m "Archive old migrations; keep squashed baseline"

echo "Done. Inspect with: git status -- porcelain; git show --name-only HEAD"
