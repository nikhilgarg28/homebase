#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
target="$repo_root/third_party/sql-logic-test"
source_url="https://github.com/hydromatic/sql-logic-test"
checkout="0a809c530457bf0e56d637ef19fcaabd2964fd67"

if [ -d "$target/.git" ]; then
  git -C "$target" fetch --depth 1 origin "$checkout"
else
  mkdir -p "$(dirname -- "$target")"
  git clone --depth 1 "$source_url" "$target"
fi

git -C "$target" checkout "$checkout"

printf 'SQL Logic Test corpus is ready at %s\n' "$target"
git -C "$target" log -1 --format='commit %H%ncommitter-date %cI%nsubject %s'
