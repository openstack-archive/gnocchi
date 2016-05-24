#!/bin/bash
set -e

export GNOCCHI_INDEXER_TESTING=True
gnocchi-upgrade --skip-storage --create-legacy-resource-types
$*
