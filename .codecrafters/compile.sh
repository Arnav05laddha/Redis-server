#!/bin/sh
set -e
cmake -B build -S .
cmake --build build
