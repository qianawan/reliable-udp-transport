#!/usr/bin/env sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
build_dir=$(mktemp -d /tmp/gudp-build.XXXXXX)
trap 'rm -rf "$build_dir"' EXIT HUP INT TERM

java_home=$(printenv JAVA_HOME || true)
if [ -n "$java_home" ]; then
  java_cmd="$java_home/bin/java"
  javac_cmd="$java_home/bin/javac"
elif [ -x /opt/homebrew/opt/openjdk@17/bin/javac ]; then
  java_cmd=/opt/homebrew/opt/openjdk@17/bin/java
  javac_cmd=/opt/homebrew/opt/openjdk@17/bin/javac
else
  java_cmd=java
  javac_cmd=javac
fi

find "$project_root/src/main/java" "$project_root/src/test/java" "$project_root/examples" \
  -name '*.java' -print > "$build_dir/sources.txt"

"$javac_cmd" --release 8 -Xlint:all -d "$build_dir/classes" @"$build_dir/sources.txt"
"$java_cmd" -cp "$build_dir/classes" GUDPIntegrationTest
