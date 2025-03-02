#!/usr/bin/env python3
# coding=utf-8

from flask import Flask, request, jsonify, Response
from clickhouse_driver import Client
from datetime import datetime, timedelta, date
from collections import defaultdict
import time
import os
import yaml
import json
import logging

app = Flask(__name__)
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(THIS_DIR, "./gn_config.yaml")) as f:
    gn_config = yaml.safe_load(f)

# Configuration de la connexion
# clickhouse_host = 'localhost'
clickhouse_host = gn_config.get('clickhouse_host')
clickhouse_port = gn_config.get('clickhouse_port')
app_port = gn_config.get('app_port')
database_name = gn_config.get('database_name')
table_name = gn_config.get('table_name')
logger = logging.getLogger(__name__)

# Liste des colonnes valides pour éviter les injections SQL
valid_fields = ['id', 'chat_id', 'chat_name', 'username', 'sender_chat_id',
                'title', 'date', 'insert_date', 'document_present', 'document_name',
                'document_type', 'document_size', 'msg_fwd', 'msg_fwd_username',
                'msg_fwd_title', 'msg_fwd_id',
                'text', 'lang', 'urls', 'hashtags']


def convert_record(record):
    """Convertit les champs date et datetime pour chaque record, merci Json"""
    record[6] = datetime.fromisoformat(record[6])  # Conversion de 'date' (7e champ) au format datetime
    record[7] = datetime.fromisoformat(record[7])  # Conversion de 'insert_date' (8e champ) au format datetime
    return record


def serialize_datetime(obj):
    """Convertit les objets datetime en chaînes de caractères."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type non sérialisable")


def valid_integer(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


# Route pour les requêtes de recherche
@app.route('/search', methods=['GET'])
def search():

    start_time = time.time()
    # Connect to clickhouse 
    client = Client(host=clickhouse_host, port=clickhouse_port)

    field = request.args.get('field')
    value = request.args.get('value')
    method = request.args.get('method')  # IS or LIKE
    count = request.args.get('count') # Max responses...

    # Si count non existant ou non valide, count = 100
    try:
        count = int(count) if count else 1001
        if count > 1001:
            return jsonify({'error': 'Count exceeds limits'}), 400
    except ValueError:
        return jsonify({'error': 'Invalid Count'}), 400

    # Valider les paramètres 'field' et 'value'
    if not field or not value:
        return jsonify({'error': 'Missing field or value parameter'}), 400

    if field not in valid_fields:
        return jsonify({'error': 'Invalid field parameter'}), 400

    if not method:
        method = "is"

    numerical = False
    if field == "chat_id":
        svalue = "%(value)i"
        value = int(value)
        numerical = True
    else:
        svalue = "%(value)s"

    # Construire la requête SQL
    # POssibliité avec final avant le where pour eviter les doubles
    if method.lower() == "like":
        query = f"SELECT * FROM {database_name}.{table_name}  WHERE {field} LIKE {svalue} order by date desc limit {count}"
    elif method.lower() == "ilike":
        query = f"SELECT * FROM {database_name}.{table_name}   WHERE positionCaseInsensitiveUTF8({field}, {svalue}) >0  order by date desc limit {count}"
    else:
        query = f"SELECT * FROM {database_name}.{table_name}  WHERE {field} = {svalue} order by date desc limit {count}"
    try:
        # Exécuter la requête
        if method.lower() == "like" and not numerical:
            result = client.execute(query, {'value': f'%{value}%'})
        elif not numerical:
            result = client.execute(query, {'value': f'{value}'})
        else:
            result = client.execute(query, {'value': value})
        # Préparer les résultats
        column_names = valid_fields
        results_dict = [dict(zip(column_names, row)) for row in result]
        len_result = len(result)

        if len_result >= count:
            has_more = "True"
            results_dict = results_dict[:-1]  # A faire avec offset un jour
        else:
            has_more = "False"

        end_time = time.time()

        # Convertir en float
        timing = float(end_time - start_time)
        timing = f"{timing:.5f}"
        del client
        results = { 'has_more': has_more, 'results': results_dict , 'timing': timing} # _ , "lenght": len_result }
        return jsonify(results)
    except Exception as e:
        print(f"error: {e}, \n {query}")
        del client
        return jsonify({'error': str(e)}), 500


# Route pour avoir un message
@app.route('/get_msg', methods=['GET'])
def get_msg():
    # Connect to clickhouse 
    client = Client(host=clickhouse_host, port=clickhouse_port)

    msg_id = request.args.get('msg_id')
    chat_id = request.args.get('channel_id')
    if valid_integer(msg_id) and valid_integer(chat_id):
        query = f"SELECT * FROM {database_name}.{table_name} WHERE msg_id = {msg_id} and chat_id = {chat_id} LIMIT 1"
        result = client.execute(query, {})
        del client

        # Préparer les résultats
        column_names = valid_fields
        results_dict = [dict(zip(column_names, row)) for row in result]
        return jsonify(results_dict)
    else:
        return jsonify({})


# Routes pour les stats
@app.route('/get_stats_chan', methods=['GET'])
def get_stats_chan():
    # Connect to clickhouse 
    client = Client(host=clickhouse_host, port=clickhouse_port)

    chat_name = request.args.get('chan_name')
    fresult={}
    query = f"SELECT chat_id from {database_name}.{table_name} where chat_name = '{chat_name}' limit 1"
    result = client.execute(query, {})

    # On a pas trouvé le Chat name
    if len(result) == 0:
        # Seems stupid, mais ca prouve qu'on a des datas
        if chat_name.startswith("100"):
            chat_id = int(chat_name)
            query = f"SELECT chat_id from {database_name}.{table_name} where chat_id = {chat_id} limit 1"
            result = client.execute(query, {})

    if not result:
        # todo error et on se casse
        fresult['stats']=False
        return jsonify(fresult)

    fresult['stats']=True
    chat_id = int(result[0][0])

    # Get the count of inserted document by last 31 jours
    query = f"SELECT formatDateTime(toDate(toStartOfDay(date)), '%%d/%%m') as actual_date, count(*) as count FROM \
              {database_name}.{table_name}  WHERE date >= toStartOfDay(subtractDays(now(), 31)) and chat_id = {chat_id} \
              GROUP BY toStartOfDay(date) order by toStartOfDay(date);"
    result = client.execute(query, {})

    # Créer une liste de dates sur 31 jours à partir d'aujourd'hui
    today = date.today()  # Obtenir la date actuelle
    date_list = [(today - timedelta(days=i)).strftime("%d/%m") for i in range(31)]

    # Convertir les dates de daily_data en un dictionnaire 
    data_dict = {entry[0]: entry for entry in result}

    # Créer le tableau complet avec les 31 jours, remplis de 0 si data absente
    filled_data = []

    for i in range(31):
        date_obj = date_list[i]  # Obtenir la date actuelle dans la boucle
        date_display = date_list[i]  # Format 'jj/mm'

        if date_obj in data_dict:
            #Si la date est présente dans les données existantes, on l'ajoute telle quelle
            filled_data.append(data_dict[date_obj])
        else:
            # Si la date est absente, on l'ajoute avec la valeur 0
            filled_data.append((date_display, 0))

    fresult['daily'] = filled_data


    # Get the count of inserted document by last 24h
    query = f"SELECT toStartOfHour(date) as actual_hour, formatDateTime(toStartOfHour(date), '%%H:00') as hour_formatted, count(*) as count from\
             {database_name}.{table_name}  WHERE date >= subtractHours(now(), 24) and chat_id = {chat_id} GROUP BY actual_hour ORDER BY actual_hour DESC;"
    query = f"SELECT formatDateTime(toStartOfHour(date), '%%H:00') as hour_formatted, count(*) as count FROM\
             {database_name}.{table_name} WHERE date >= subtractHours(now(), 24) AND chat_id = {chat_id} GROUP BY toStartOfHour(date)\
             ORDER BY toStartOfHour(date) DESC;"

    result = client.execute(query, {})

    # 1. Créer une liste des dernières 24 heures à partir de l'heure actuelle
    now = datetime.now().replace(minute=0, second=0, microsecond=0)  # Obtenir la date et heure actuelle
    # hours_list = [(now - timedelta(hours=i)) for i in range(24)]  # Créer une liste des dernières 24 heures
    hours_format_display = [(now - timedelta(hours=i)).strftime("%H:00") for i in range(24)]  # Format "hh.00"

    # 2. Convertir les heures de daily_data en un dictionnaire pour accès rapide
    # data_dict = {entry[0].replace(tzinfo=None): entry for entry in result}
    data_dict = { entry[0]: entry for entry in result }


    # 3. Créer le tableau complet avec les 24 heures
    filled_data = []

    for i in range(24):
        date_obj = hours_format_display[i]


        if date_obj in data_dict:
            # Si l'heure est présente dans les données existantes, on l'ajoute telle quelle
            filled_data.append(data_dict[date_obj])
        else:
            # Si l'heure est absente, on l'ajoute avec la valeur 0
            filled_data.append((date_obj, 0))

    fresult['hourly'] = filled_data

    # Get the count of inserted document by all months
    query = f"SELECT toStartOfMonth(date) as month, formatDateTime(toStartOfMonth(date), '%%Y/%%m') as month_formatted, count(*) as count FROM \
              {database_name}.{table_name} WHERE date >= subtractMonths(now(), 24) and chat_id = {chat_id} \
              GROUP BY month ORDER BY month DESC"
    result = client.execute(query, {})
    fresult['monthly'] = result


    del client
    return jsonify(fresult)



# Routes pour les stats
@app.route('/get_stats', methods=['GET'])
def get_stats():

    # Connect to clickhouse 
    client = Client(host=clickhouse_host, port=clickhouse_port)


    fresult={}

    # Get the count of inserted document by last 31 jours
    query = f"SELECT toDate(insert_date) as actual_date, formatDateTime(toDate(insert_date), '%%d/%%m') as day_formatted, count(*) as count FROM \
              {database_name}.{table_name} WHERE insert_date >= toStartOfDay(subtractDays(now(), 31)) GROUP BY actual_date  ORDER BY actual_date DESC;"
    result = client.execute(query, {})
    fresult['cdaily'] = result

    # Get the count of inserted document by last 24h
    query = f"SELECT toStartOfHour(insert_date) as actual_hour, formatDateTime(toStartOfHour(insert_date), '%%H:00') as hour_formatted, count(*) as count FROM {database_name}.{table_name}  WHERE insert_date >= subtractHours(now(), 24) GROUP BY actual_hour ORDER BY actual_hour DESC;"
    result = client.execute(query, {})
    fresult['chourly'] = result

    # Get the count of inserted document by 24 months
    query = f"SELECT toStartOfMonth(insert_date) as month,     formatDateTime(toStartOfMonth(insert_date), '%%Y/%%m') as month_formatted, count(*) as count FROM {database_name}.{table_name} WHERE insert_date >= subtractMonths(now(), 24) GROUP BY month ORDER BY month DESC limit 24"
    result = client.execute(query, {})
    fresult['cmonthly'] = result


    # Get the count of document in db by publish day on last 31 days
    query = f"SELECT toStartOfMonth(insert_date) as month, formatDateTime(toDate(insert_date), '%%y/%%m'), count(*) as count FROM {database_name}.{table_name} WHERE insert_date >= subtractMonths(now(), 24) GROUP BY month ORDER BY month ASC"
    query = f"SELECT toDate(date) as actual_date, formatDateTime(toDate(date), '%%d/%%m') as day_formatted, count(*) as count FROM \
            {database_name}.{table_name} WHERE date >= toStartOfDay(subtractDays(now(), 31)) GROUP BY actual_date  ORDER BY actual_date DESC;"
    result = client.execute(query, {})
    fresult['daily'] = result

    # Get the count of document in db by publish day on last 24h
    query = f"SELECT toStartOfHour(date) as actual_hour, formatDateTime(toStartOfHour(date), '%%H:00') as hour_formatted, count(*) as count FROM {database_name}.{table_name}  WHERE date >= subtractHours(now(), 24) GROUP BY actual_hour ORDER BY actual_hour DESC;"
    result = client.execute(query, {})
    fresult['hourly'] = result

    # Get the count of document in db by publish day on last 24 month
    query = f"SELECT toStartOfMonth(date) as month, formatDateTime(toStartOfMonth(date), '%%Y/%%m') as month_formatted, count(*) as count FROM {database_name}.{table_name} WHERE insert_date >= subtractMonths(now(), 24) GROUP BY month ORDER BY month DESC limit 24"
    result = client.execute(query, {})
    fresult['monthly'] = result

    # Get the number of differnet charts
    query =f"SELECT countDistinct(chat_id) as distinct_chat_id_count FROM {database_name}.{table_name}"
    result = client.execute(query, {})
    fresult['chats'] = result[0]

    # get the total nubmer on messages collecteds
    query =f"SELECT count(msg_id) as total_collected_messages FROM {database_name}.{table_name}"
    result = client.execute(query, {})
    fresult['msgs'] = result[0]

    # get the top 50 chatty chans
    query =f"SELECT      chat_id, chat_name, COUNT(msg_id) AS msg_count FROM {database_name}.{table_name} GROUP BY chat_id, chat_name ORDER BY msg_count DESC LIMIT 50; "
    result = client.execute(query, {})
    fresult['top50'] = result

    # Get stats about the db and compressions
    query =f"SELECT name,  formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio FROM system.columns WHERE table = 'msg' GROUP BY name"
    result = client.execute(query, {})
    fresult['stats'] = result

    del client
    return jsonify(fresult)


# Route pour avoir un message
@app.route('/user_brief', methods=['GET'])
def user_brief():

    # Connect to clickhouse 
    client = Client(host=clickhouse_host, port=clickhouse_port)

    user_id = request.args.get('user_id')
    s_max = 500
    has_more=False
    json_result = {}
    if valid_integer(user_id):
        query = f"SELECT count(*) FROM {database_name}.{table_name} WHERE sender_chat_id = {user_id}"
        json_result["msg_count"] = client.execute(query, {})[0][0]
        if json_result["msg_count"] > 0: 
            query = f"SELECT chat_id, chat_name FROM {database_name}.{table_name} WHERE sender_chat_id = {user_id} group by chat_id, chat_name"
            json_result["user_chats"] = client.execute(query, {})
            query = f"SELECT date FROM {database_name}.{table_name} WHERE sender_chat_id == {user_id} order by date desc limit 1"
            json_result["last_msg"] = client.execute(query, {})[0][0]
            query = f"SELECT date FROM {database_name}.{table_name}  WHERE sender_chat_id == {user_id} order by date asc limit 1"
            json_result["first_msg"] = client.execute(query, {})[0][0]
            query = f"SELECT * FROM {database_name}.{table_name}  WHERE sender_chat_id == {user_id} order by date desc limit {s_max+1}"
            result = client.execute(query, {})
            has_more=False
            if len(result) >= s_max+1:
                has_more=True
            json_result["last_msgs"] = client.execute(query, {})[:-1]
        else:
            json_result["user_chats"] = None
            json_result["last_msg"] = None
            json_result["first_msg"] = None
            json_result["last_msgs"] = None
        json_result["has_more"] = has_more
        # Préparer les résulats
        del client
        return jsonify(json_result)
    else:
        del client
        return jsonify({})

@app.route('/stats_msg', methods=['GET'])
def stats_msg():
    # Connect to clickhouse 
    client = Client(host=clickhouse_host, port=clickhouse_port)

    query = f"select count(msg_id), chat_id ,chat_name from tme_prod.msg  where date > toDateTime('2024-09-04 00:00:00') and date < toDateTime('2024-09-04 23:59:59')  group by chat_id,chat_name order by count(msg_id) desc limit 25"
    result = client.execute(query, {})

    del client
    return jsonify(result)

# Route pour les last messages
@app.route('/index', methods=['GET'])
def index():
    # Connect to clickhouse 
    client = Client(host=clickhouse_host, port=clickhouse_port)

    query = f"select date, chat_id, msg_id, chat_name from {database_name}.{table_name} final where date > now() - INTERVAL 1 HOUR order by date desc"
    # order by date desc limit 500"
    result = client.execute(query, {})

    del client
    return jsonify(result)


# Route pour les last messages
@app.route('/count', methods=['GET'])
def count():
    # Connect to clickhouse 
    client = Client(host=clickhouse_host, port=clickhouse_port)

    query = f"select count(chat_name) from {database_name}.{table_name}"
    result = client.execute(query, {})

    del client
    return jsonify({"count": result[0][0]})

# Route pour les last messages, 
# donne les messages collectés dans la clickhouse
# 2 param, 
#   since = timestamp du debut.
#   for = nombre de minutes a fournir.
@app.route('/last', methods=['GET'])
def last():
    # Connect to clickhouse 
    client = Client(host=clickhouse_host, port=clickhouse_port)


    #  wget "http://localhost:6000/last?since=1724681600&for=15" -O -  | jq .
    if request.args.get('since'):
        since = request.args.get('since') # Get Unix TimeStamp
    else:
        since = int(round(time.time() * 1000)) # Sinon c'est NOW

    if request.args.get('for'):
        tfor = request.args.get('for') # minutes to fetch
    else:
        tfor = 5 # Si pas précisé c'est 5

    # LIMITS and default
    if not valid_integer(since):   # Si bad integer = Now
        since = int(round(time.time() * 1000))
    else:
        since = int(since)  
    if not valid_integer(tfor):
        tfor = 5
    else:
        tfor = int(tfor)
    tfor = ( tfor * 60 ) + since # convert to millisec

    # on demande que la date du post.. pas l'insert date, pbm de charge
    query = f"select * from {database_name}.{table_name} where insert_date>=toDateTime({since}) and insert_date<=toDateTime({tfor}) AND ((document_present = 1) OR (text != '')) limit 2000000"

    page_size = 50000  # Taille des records (chunk)
    del client

    def generate():
    
        client = Client(host=clickhouse_host, port=clickhouse_port)

        messages = 0
        offset = 0
        while True:
            # Requête SQL avec pagination et limite qui ne choppe pas les texte vide
            query = f"""
            SELECT * 
            FROM {database_name}.{table_name} 
            WHERE insert_date >= toDateTime({since}) 
              AND insert_date <= toDateTime({tfor}) 
              AND ((document_present = 1) OR (text != '')) 
            LIMIT {page_size} OFFSET {offset}
            """

            # Exécuter la sql
            result = client.execute(query, {})
            print(f"SQL page fetched, offset: {offset}")  # Kindoff debug

            # Si aucun résultat n'est retourné, arrêter
            if not result:
                break

            # Préparer les résultats
            column_names = valid_fields
            results_dict = [dict(zip(column_names, row)) for row in result]
            out_dict = []

            for msg in results_dict:
                messages += 1
                # Convertir les objets datetime en compatible json
                msg['insert_date'] = serialize_datetime(msg.get('insert_date'))
                msg['date'] = serialize_datetime(msg.get('date'))

                htext = f"On {msg.get('date')} on Telegram\n"
                htext += f"The following data were collected in the channel {msg.get('chat_name')}/{msg.get('chat_id')} with message id {msg.get('id')}\n"
                htext += f"User Username {msg.get('username')} / {msg.get('sender_chat_id')}\n"
                htext += f"Subject: {msg.get('title')}\n"
                htext += msg.get('text') + "\n"
                if msg.get('msg_fwd') == 1:
                    htext += f"It was a forward from the channel {msg.get('msg_fwd_username')}/{msg.get('msg_fwd_id')}\n"
                if msg.get('document_present') == 1:
                    htext += f"The document {msg.get('document_name')}/{msg.get('document_type')} with a size of {msg.get('document_size')} bytes was attached to this messages.\n"
                htext += f"\nThis message was acquired on {msg.get('insert_date')}\n"

                out_dict.append({
                    "date": msg.get('insert_date'),
                    'text': htext,
                    'channel_id': msg.get('chat_id'),
                    'channel_name': msg.get('chat_name'),
                    'msg_id': msg.get('id')
                })

            # Convertir en JSON et envoyer un chunk
            yield json.dumps({'results': out_dict, 'length': len(out_dict)}, default=serialize_datetime) + "\n"

            # Incrémenter l'offset pour la page suivante
            offset += page_size

        print(f"Send Messages {messages}")
        del client
    return Response(generate(), content_type='application/json')



# Collect messages to integrate into the database.
@app.route('/insert_records', methods=['POST'])
def insert_records():
    data = request.get_json()

    if not data:
        return jsonify({"error": "Invalid or missing JSON data"}), 400

    records = json.loads(data).get('records')
    
    if not records:
        return jsonify({"error": "No records found in the JSON data"}), 400

    # Conversion des champs datetime pour chaque record
    records = [convert_record(record) for record in records]

    # Connect to clickhouse 
    client = Client(host=clickhouse_host, port=clickhouse_port)

    try:
        client.execute(f'INSERT INTO {database_name}.{table_name} VALUES', records)
        logger.info(f"Inserted {len(records)} records into ClickHouse")
        return jsonify({"status": "success", "inserted_records": len(records)}), 200
    except Exception as e:
        logger.error(f"Failed to insert records: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        del client


@app.route('/graph', methods=['GET'])
def get_graph():
    chat_id = request.args.get('chat_id')

    clickhouse_client = Client(host=clickhouse_host, port=clickhouse_port)
    if not chat_id:
        return jsonify({'error': 'chat_id is required'}), 400

    try:
        # Requête pour récupérer les nouds (utilisateurs du chat_id)
        nodes_query = f"""
            SELECT DISTINCT
                sender_chat_id AS id,
                username AS label,
                chat_name
            FROM tme_prod.msg
            WHERE chat_id = {chat_id}
            AND msg_fwd_id != 0 
            AND sender_chat_id != {chat_id}
            limit 100
        """

        # Exécution des requêtes
        nodes_result = clickhouse_client.execute(nodes_query)
        # edges_result = clickhouse_client.execute(edges_query)

        # Dictionnaire pour assurer l'unicité des nœuds
        nodes_map = {}

        chat_name = ""
        for row in nodes_result:
            user_id = row[0]
            username = row[1].replace("None", "") # suite a une non gestion historique, certain champ usernane sont none
            chat_name = row[2]

            if user_id not in nodes_map:
                nodes_map[user_id] = {'id': user_id, 'labels': set()}
            nodes_map[user_id]['labels'].add(username)  # Utilisation d'un set pour stocker les labels

        # Liste des nœuds et des arêtes
        nodes = [{'id': 1, 'label': f"{chat_id}\n{chat_name}", 'shape': "box", 'color':'purple'}]  # Ajouter le channel principal

        edges = []
        seen_edges = set()

        for row in nodes_result:
            from_id = 1  # Assumons que le noeud source est toujours 1
            to_id = row[0]  # id du nœud cible
            edge_label = ""  # Label pour l'arête

            edge = (from_id, to_id)  # Crée un tuple représentant l'arête

            # Vérifie si l'arête n'a pas encore été ajoutée
            if edge not in seen_edges:
                edges.append({'from': from_id, 'to': to_id, 'label': edge_label})
                seen_edges.add(edge)  # Ajoute l'arête à l'ensemble pour éviter les doublons


        # Ajouter les utilisateurs comme nœuds
        for user_id, user_data in nodes_map.items():
            nodes.append({'id': user_data['id'], 'label': f'{user_data["id"]}\n'+'\n'.join(user_data['labels'])})

        # Vérifier si chaque utilisateur est aussi dans d'autres chats
        for user_id in nodes_map.keys():
            other_chats_query = f"""
                SELECT DISTINCT chat_id, chat_name
                FROM tme_prod.msg
                WHERE sender_chat_id = {user_id}
                AND chat_id != {chat_id} 
            """
            other_chats_result = clickhouse_client.execute(other_chats_query)

            for other_chat in other_chats_result:
                other_chat_id = other_chat[0]
                other_chat_node_id = f'{other_chat_id}'  # ID unique pour éviter les conflits

                # Ajouter le nouveau chat en rouge s'il n'existe pas déjà
                if not any(n['id'] == other_chat_node_id for n in nodes):
                    nodes.append({'id': other_chat_node_id, 'label': f'{other_chat_id}\n{other_chat[1]}', 'color': 'red', 'shape':'box'})

                # Vérifie si user_id est différent de other_chat_node_id avant d'ajouter l'arête
                if str(user_id) != other_chat_node_id:
                    edges.append({'from': user_id, 'to': other_chat_node_id})


        # Retourner les données au format JSON
        return jsonify({'nodes': nodes, 'edges': edges})

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        del clickhouse_client 

@app.route('/user_talk/<int:user>', methods=['GET'])
def user_talk(user):
    # Requête ClickHouse pour récupérer les données pour un user donné

    client = Client(host=clickhouse_host, port=clickhouse_port)
    query = f"""
    SELECT
        toDate(date) AS day,
        chat_id,
        chat_name,
        COUNT(*) AS count
    FROM tme_prod.msg
    WHERE sender_chat_id = {user}
    GROUP BY day, chat_id, chat_name
    ORDER BY day
    """
    
    # Exécuter la requête ClickHouse et obtenir le résultat
    result = client.execute(query)
    
    # Organiser les données dans un dictionnaire pour le traitement
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    
    # Remplir le dictionnaire avec les données
    for row in result:
        day, chat_id, chat_name, count = row
        data[day][chat_id]['name'] = chat_name
        data[day][chat_id]['count'] += count  # Incrémenter le count pour chaque chat_id et jour
    
    # Transformer le dictionnaire en une liste de dictionnaires pour le format JSON
    formatted_data = []
    for day, chats in data.items():
        for chat_id, chat_info in chats.items():
            formatted_data.append({
                'day': str(day),  # Convertir la date en chaîne pour le JSON
                'chat_id': chat_id,
                'chat_name': chat_info['name'],
                'count': chat_info['count']
            })
    del(client) 
    return jsonify(formatted_data)

@app.route('/user_dailytalk/<int:user_id>')
def user_dailytalk(user_id):
    client = Client(host=clickhouse_host, port=clickhouse_port)
    # Requête pour récupérer les données depuis ClickHouse
    query = f"""
        SELECT 
            toStartOfHour(date) AS hour, 
            toDayOfWeek(date) AS day_of_week,
            count(*) AS count
        FROM 
            tme_prod.msg
        WHERE 
            sender_chat_id = {user_id}
        GROUP BY 
            hour, day_of_week
        ORDER BY 
            day_of_week, hour
    """
    query = f"""
    SELECT
        toHour(date) AS hour,                  -- Extraire l'heure
        toDayOfWeek(date) AS day_of_week,      -- Extraire le jour de la semaine (1 = lundi, 2 = mardi, ...)
        count(*) AS message_count              -- Compter le nombre de messages
    FROM
        tme_prod.msg  -- Nom de la table
    WHERE
        sender_chat_id = {user_id}             -- Filtrer pour l'utilisateur avec l'ID spécifique (sender_chat_id)
    GROUP BY
        day_of_week,                           -- Regrouper par jour de la semaine
        hour                                   -- Regrouper par heure
    ORDER BY
        day_of_week ASC,                       -- Trier par jour de la semaine (lundi = 1, dimanche = 7)
        hour ASC                               -- Trier par heure (0 à 23)
    """


    # Exécuter la requête
    result = client.execute(query)
    
    # Créer des dictionnaires pour les données de la heatmap
    heatmap_data = {
        "Monday": [0] * 24,
        "Tuesday": [0] * 24,
        "Wednesday": [0] * 24,
        "Thursday": [0] * 24,
        "Friday": [0] * 24,
        "Saturday": [0] * 24,
        "Sunday": [0] * 24
    }

    # Mapper les numéros de jours de la semaine (1 = Monday, 7 = Sunday) aux noms de jours
    jours_map = {
        1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 
        4: 'Thursday', 5: 'Friday', 6: 'Saturday', 7: 'Sunday'
    }

    # remplir les données de la heatmap
    for row in result:
        hour = row[0]  # Heure de la date (0-23)
        day_of_week = jours_map[row[1]]  # Jour de la semaine
        count = row[2]  # Nombre de messages

        # Ajouter les messages dans le bon jour et heure
        heatmap_data[day_of_week][hour] = count

    del(client)
    # Convertir le dictionnaire en JSON
    return jsonify(heatmap_data)

@app.route('/user_details/<int:user_id>')
def user_details(user_id):
    client = Client(host=clickhouse_host, port=clickhouse_port)
    # Requête pour récupérer les données depuis ClickHouse
    if not valid_integer(user_id):
        return jsonify({'results': False})

    query = f" select date from  tme_prod.msg where sender_chat_id = {user_id} order by date asc limit 1;" 
    data = client.execute(query)
    date_in = data[0][0].strftime("%d/%m/%Y")

    query = f" select date from  tme_prod.msg where sender_chat_id = {user_id} order by date desc limit 1;" 
    data = client.execute(query)
    date_out = data[0][0].strftime("%d/%m/%Y")

    resume = f"Account {user_id} is active since {date_in} to {date_out}"

    query = f"SELECT distinct(chat_id,chat_name, username ) FROM tme_prod.msg where sender_chat_id == {user_id}"

    pseudos = []
    data = client.execute(query)
    for line in data:
        username = line[0][2].replace("None", "") # suite a une non gestion historique, certain champ usernane sont none
        if username == " ":
            pseudos.append(("Unknown pseudo",line[0][0],line[0][1]))
        else:
            pseudos.append((username,line[0][0],line[0][1]))

    return jsonify({'resume': resume, 'pseudos': pseudos, 'results': True})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=app_port)
