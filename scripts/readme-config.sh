#!/bin/sh
# Regenerates the configuration reference in docs/configuration.md from
# antwatcher.example.yml. The block between the config-reference markers is
# replaced verbatim; the archtest package asserts the two stay in sync.
set -eu

cd "$(dirname "$0")/.."
doc=docs/configuration.md
example=antwatcher.example.yml
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

awk -v example="$example" '
  /<!-- config-reference:begin/ {
    print
    print "```yaml"
    # getline returns -1 on a read error and 0 at end of file. Without the
    # check an unreadable example would silently produce an empty fence and
    # the script would still report success.
    while ((rc = (getline line < example)) > 0) print line
    if (rc < 0) {
      print "cannot read " example > "/dev/stderr"
      exit 1
    }
    close(example)
    print "```"
    skip = 1
    next
  }
  /<!-- config-reference:end/ { skip = 0 }
  !skip { print }
' "$doc" > "$tmp"

# cat, not mv: mktemp creates the file 0600 and mv would carry that mode onto
# the target, which git does not track and nothing would notice.
cat "$tmp" > "$doc"
echo "$doc: configuration reference regenerated from $example"
