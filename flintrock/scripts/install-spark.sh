#!/bin/bash

set -e

url="$1"

echo "Installing Spark..."
echo "  from: ${url}"

file="$(basename ${url})"

# S3 is generally reliable, but sometimes when launching really large
# clusters it can hiccup on us, in which case we'll need to retry the
# download.
set +e
tries=1
while true; do
    curl --remote-name "${url}"
    curl_ret=$?

    if ((curl_ret == 0)); then
        break
    elif ((tries >= 3)); then
        exit 1
    else
        tries=$((tries + 1))
        sleep 1
    fi
done
set -e

gzip -t "$file"

mkdir "spark"
# strip-components puts the files in the root of spark/
tar xzf "$file" -C "spark" --strip-components=1
rm "$file"
