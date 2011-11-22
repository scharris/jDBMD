#!/bin/sh

mkdir outgoing_patches 2> /dev/null
rm -rf outgoing_patches/*

git format-patch -M origin/master -o outgoing_patches/

