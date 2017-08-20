#!/bin/bash
set -e

sleep 5 # Wait until Redis wakes up
./target/release/moon20x
