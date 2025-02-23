#!/bin/bash

# Ce script extrait la db localement, 
# la compresse a mort ( j'ai pas le format qui va bien sur mon backend )
# et garde seulement 15 jours d' export de db


# Obtenir la date et l'heure actuelles dans le format souhaité
timestamp=$(date +"%Y%m%d_%H%M%S")

# Construire le nom du fichier avec le timestamp
outfile="/tmp/export_${timestamp}.db"

#Exécuter la requête SQL avec le nom de fichier datetime + compression
clickhouse-client -h 192.168.104.11 --query="SELECT * FROM tme_prod.msg INTO OUTFILE '${outfile}' FORMAT Native"
bzip2 -9 ${outfile}

# cleanup garde que 15 jours
find /tmp/export_*.bz2 -type f -mtime +15 -exec rm {} \;

rm  /tmp/latest_export.bz2
ln -s ${outfile}.bz2 /tmp/latest_export.bz2
