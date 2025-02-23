#!/bin/bash

# This script will integrate data from production to dev

# Paramètres
REMOTE_HOST="192.168.104.11"
REMOTE_USER="root"
DATABASE="tme_prod"
TABLE="msg"
LOCAL_CLICKHOUSE="clickhouse-client"
REMOTE_CLICKHOUSE="clickhouse-client -h $REMOTE_HOST"

# Récupérer la structure de la table depuis le serveur distant
echo "Récupération de la structure de la table $DATABASE.$TABLE depuis $REMOTE_HOST..."
TABLE_SCHEMA=$($REMOTE_CLICKHOUSE --query="SHOW CREATE TABLE $DATABASE.$TABLE" --format=TabSeparatedRaw)

echo $TABLE_SCHEMA

if [[ -z "$TABLE_SCHEMA" ]]; then
    echo "Échec : Impossible de récupérer la structure de la table."
        exit 1
        fi

        # Créer la base de données localement si elle n'existe pas
        $LOCAL_CLICKHOUSE --query="DROP DATABASE IF EXISTS $DATABASE"

        echo "Vérification et création de la base de données $DATABASE..."
        $LOCAL_CLICKHOUSE --query="CREATE DATABASE IF NOT EXISTS $DATABASE"

        #Créer la table localement
        echo "Création de la table $DATABASE.$TABLE..."
        echo "$TABLE_SCHEMA" | $LOCAL_CLICKHOUSE --multiquery

        scp $REMOTE_USER@$REMOTE_HOST:/tmp/latest_export.bz2 /tmp
        # Transférer et importer les données en streaming via SSH
        echo "Transfert et importation des données depuis $REMOTE_HOST..."
        cat /tmp/latest_export.bz2 | bzip2 -dc | $LOCAL_CLICKHOUSE --query="INSERT INTO $DATABASE.$TABLE FORMAT Native"

        if [[ $? -eq 0 ]]; then
            echo "Importation terminée avec succès !"
            rm /tmp/latest_export.bz2 
            else
                echo "Échec de l'importation."
                    exit 1
                    fi

