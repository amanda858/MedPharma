#!/bin/bash
cd /workspaces/MedPharma
git log --oneline -8 | cat
echo "=== git status short ==="
git status --short | cat
