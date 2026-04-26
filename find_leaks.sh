#!/bin/bash
echo "Looking for maps that never have a delete call..."
for dir in services/*; do
    if [ -d "$dir" ]; then
        # Find all files in the directory
        files=$(find "$dir" -name "*.go" -not -name "*_test.go")
        if [ -n "$files" ]; then
            # Check if map is used
            if grep -q -E "map\[[a-zA-Z0-9]+\]" $files; then
                # Check if delete is missing
                if ! grep -q "delete(" $files; then
                    echo "$dir uses map but never calls delete()"
                fi
            fi
        fi
    fi
done
