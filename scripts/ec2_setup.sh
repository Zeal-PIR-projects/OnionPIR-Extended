#!/usr/bin/env bash
# Idempotent build prerequisites for the OnionPIR server/client on Amazon
# Linux (2023): compiler toolchain, CMake, and Intel HEXL. Go and protoc are
# expected to be installed already (the shuffle/Zealot experiments need them).
#
# Prints "HEXL_DIR=<dir containing HEXLConfig.cmake>" as its last line.
# Override the install prefix with HEXL_PREFIX (default /opt/hexl).
set -euo pipefail

HEXL_PREFIX=${HEXL_PREFIX:-/opt/hexl}
HEXL_TAG=${HEXL_TAG:-v1.2.6}

missing=()
command -v cmake >/dev/null 2>&1 || missing+=(cmake)
command -v g++   >/dev/null 2>&1 || missing+=(gcc-c++)
command -v make  >/dev/null 2>&1 || missing+=(make)
command -v git   >/dev/null 2>&1 || missing+=(git)
if ((${#missing[@]})); then
  echo "Installing ${missing[*]}" >&2
  if command -v dnf >/dev/null 2>&1; then dnf -y install "${missing[@]}" >&2
  else yum -y install "${missing[@]}" >&2; fi
fi

find_hexl() { find "$HEXL_PREFIX" -name HEXLConfig.cmake 2>/dev/null | head -n 1; }
config=$(find_hexl || true)
if [ -z "$config" ]; then
  echo "Building Intel HEXL $HEXL_TAG into $HEXL_PREFIX" >&2
  src=$(mktemp -d)
  trap 'rm -rf "$src"' EXIT
  git clone --quiet --depth 1 --branch "$HEXL_TAG" https://github.com/intel/hexl.git "$src" >&2
  cmake -S "$src" -B "$src/build" -DCMAKE_BUILD_TYPE=Release \
        -DHEXL_BENCHMARK=OFF -DHEXL_TESTING=OFF -DHEXL_EXPERIMENTAL=OFF \
        -DCMAKE_INSTALL_PREFIX="$HEXL_PREFIX" >&2
  cmake --build "$src/build" -j"$(nproc)" >&2
  cmake --install "$src/build" >&2
  config=$(find_hexl)
fi
[ -n "$config" ] || { echo "HEXL install failed" >&2; exit 1; }
echo "HEXL_DIR=$(dirname "$config")"
