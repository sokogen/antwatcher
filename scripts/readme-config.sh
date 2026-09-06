#!/bin/sh
# Regenerates the configuration reference in docs/configuration.md from
# antwatcher.example.yml. The block between the config-reference markers is
# replaced verbatim; the archtest package asserts the two stay in sync.
set -eu

cd "$(dirname "$0")/.."
doc=docs/configuration.md
example=antwatcher.example.yml
tmp=$(mktemp)

awk -v example="$example" '
  /<!-- config-reference:begin/ {
    print
    print "```yaml"
    while ((getline line < example) > 0) print line
    close(example)
    print "```"
    skip = 1
    next
  }
  /<!-- config-reference:end/ { skip = 0 }
  !skip { print }
' "$doc" > "$tmp"

mv "$tmp" "$doc"
echo "$doc: configuration reference regenerated from $example"
