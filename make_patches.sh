#!/bin/sh
rm -rf outgoing_patches/*
git format-patch -M origin/master -o outgoing_patches/

