#!/bin/bash
set -ex

set -o allexport
source .env

mkdir -p runtime

# Check if the substitution file path is provided as a command-line argument
if [ $# -eq 2 ]; then
    substitution_file="$2"
    echo "Using external substitution $substitution_file"
    source $substitution_file
fi

set +o allexport

files_matching_criteria="$1"
if [ -z "$files_matching_criteria" ]; then
  # If $files_matching_criteria is empty, assign it all files matching the criteria
  files_matching_criteria=$(find . -type f -name "*.yaml")
else
  echo "Variable \$1 is not empty: $1"
fi

for stack in $files_matching_criteria
do
    stack="${stack%.yaml}"
    stack="${stack#./}"
    # Altrimenti, effettua la sostituzione delle variabili di ambiente
    envsubst < "${stack}.yaml" > "runtime/${stack}-subs.yaml"
    # Deploya lo stack
    docker stack deploy -c "runtime/${stack}-subs.yaml" "${ENVIRONMENTNAME}-${stack}"
done
