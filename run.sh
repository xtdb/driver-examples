#!/bin/bash

# Iterate over all non-hidden subdirectories
for dir in */; do
  # Check if run.sh exists and is executable in the subdirectory
  if [[ -f "${dir}run.sh" && -x "${dir}run.sh" ]]; then
    echo "Executing ${dir}run.sh..."
    (cd "$dir" && ./run.sh)
  else
    echo "No executable run.sh found in $dir, skipping."
  fi
done

