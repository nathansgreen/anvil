#!/bin/bash

[ -d bench ] || exit 1
PATCH="bench/$1.patch"
[ -f "$PATCH" ] || exit 2
patch -p1 < "$PATCH" || exit $?
git diff > "$PATCH.new"
if cmp -s "$PATCH.new" "$PATCH"
then
	echo "Patch unchanged."
	rm "$PATCH.new"
else
	echo "Updating patch."
	ln -f "$PATCH" "$PATCH.old" && mv "$PATCH.new" "$PATCH"
fi
patch -p1 -R < "$PATCH"
