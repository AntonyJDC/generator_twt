import getopt
import os
import argparse
import sys
import bz2
import json
import networkx as nx
import time
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from datetime import datetime
import shutil

def parse_args(argv):
    parser = argparse.ArgumentParser(description="Argumentos para generador.py", add_help=False)
    parser.add_argument("-d", "--directory", default="data", help="Ruta al directorio de datos")
    parser.add_argument("-fi", "--fecha-inicial", help="Fecha inicial")
    parser.add_argument("-ff", "--fecha-final", help="Fecha final")
    parser.add_argument("-h", "--hashtags", help="Lista de hashtags")
    parser.add_argument("-grt", action="store_true", help="Crear grafo de retweets")
    parser.add_argument("-jrt", action="store_true", help="Crear JSON de retweets")
    parser.add_argument("-gm", action="store_true", help="Crear grafo de menciones")
    parser.add_argument("-jm", action="store_true", help="Crear JSON de menciones")
    parser.add_argument("-gcrt", action="store_true", help="Crear grafo de corretweets")
    parser.add_argument("-jcrt", action="store_true", help="Crear JSON de corretweets")
    args = parser.parse_args(argv)
    args = vars(args)
    if "directory" not in args:
       args["directory"] = "data"
    return args

def obtener_id(tweet):
    return tweet['id_str'] if 'retweeted_status' in tweet else str(tweet['id'])

def validar_fecha(tweet, fi, ff):
    if 'created_at' in tweet:
        tweet_date_str = tweet['created_at']
        
        # Convertir la fecha del tweet al formato deseado
        tweet_date = datetime.strptime(tweet_date_str, "%a %b %d %H:%M:%S +0000 %Y").date()
            
        if fi is not None:
            # Convertir la fecha inicial al formato deseado
            fi = datetime.strptime(fi, "%d-%m-%y").date()
            if tweet_date < fi:
                return False

        if ff is not None:
            # Convertir la fecha final al formato deseado
            ff = datetime.strptime(ff, "%d-%m-%y").date()
            if tweet_date > ff:
                return False

        # Si no se generó ninguna excepción, la fecha se validó correctamente
        return True

    return True

def procesar_tweets(tweet, retweets_info, mentions_info, tweet_type, hashtags_set=None, fi=None, ff=None):
    if fi is not None or ff is not None:
        if not validar_fecha(tweet, fi, ff):
            return
    if 'user' in tweet:
        author_username = tweet['user']['screen_name']
        tweet_id = obtener_id(tweet)

    if 'entities' in tweet and 'hashtags' in tweet['entities']:
        tweet_hashtags = {tag['text'].lower() for tag in tweet['entities']['hashtags']}
        if hashtags_set and not tweet_hashtags.intersection(hashtags_set):
            return

    if tweet_type == 'retweet':
        retweets_info.setdefault(author_username, {"tweets": {}})
        retweets_info[author_username]["tweets"].setdefault(tweet_id, {"retweetedBy": []})
        
        if 'retweeted_status' in tweet and 'user' in tweet['retweeted_status']:
            retweet_author_username = tweet['retweeted_status']['user']['screen_name']
            retweeted_tweet_id = obtener_id(tweet['retweeted_status'])

            retweets_info.setdefault(retweet_author_username, {"tweets": {}})
            retweets_info[retweet_author_username]["tweets"].setdefault(retweeted_tweet_id, {"retweetedBy": []})
            retweets_info[retweet_author_username]["tweets"][retweeted_tweet_id]["retweetedBy"].append(author_username)

            original_tweet = tweet['retweeted_status']
            procesar_menciones(original_tweet, mentions_info)
    else:
        if 'user' in tweet:
            procesar_menciones(tweet, mentions_info)

def procesar_menciones(tweet, mentions_info):
    if 'entities' in tweet and 'user_mentions' in tweet['entities'] and tweet['entities']['user_mentions']:
        mentioned_usernames = set(mention['screen_name'] for mention in tweet['entities']['user_mentions'])
        for mentioned_username in mentioned_usernames:
            mentions_info.setdefault(mentioned_username, {"mentions": []})
            mentions_info[mentioned_username]["mentions"].append({"mentionBy": tweet['user']['screen_name'], "tweets": [obtener_id(tweet)]})

def decompress_and_create_json_files(directory, hashtags_file=None, fi=None, ff=None):
    retweets_info = {}
    mentions_info = {}
    hashtags_set = set()
    if hashtags_file:
        with open(hashtags_file, 'r') as hashtags_file:
            hashtags_set = {line.strip().lower() for line in hashtags_file}

    base_path = Path(directory)
    file_paths = base_path.rglob('*.json.bz2')

    for file_path in file_paths:
        json_file_path = file_path.with_suffix('.json')

        with bz2.BZ2File(file_path, 'rb') as source, open(json_file_path, 'wb') as target:
            target.write(source.read())
            #print("hice esto")
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            for line in json_file:
                tweet = json.loads(line)
                tweet_type = 'retweet' if 'retweeted_status' in tweet else 'original'
                procesar_tweets(tweet, retweets_info, mentions_info, tweet_type, hashtags_set, fi, ff)

    return retweets_info, mentions_info

def json_retweets(retweets_info, arg):
    retweets_json = {"retweets": []}

    for author, author_info in retweets_info.items():
        total_retweets = sum(len(tweet_info["retweetedBy"]) for tweet_info in author_info["tweets"].values())
        
        if total_retweets > 0:
            author_data = {"username": author, "receivedRetweets": total_retweets, "tweets": {}}

            for tweet_id, tweet_info in author_info["tweets"].items():
                retweeted_by = tweet_info["retweetedBy"]
                tweet_data = {"retweetedBy": retweeted_by}
                author_data["tweets"]["tweetId: {}".format(tweet_id)] = tweet_data

            retweets_json["retweets"].append(author_data)

    # Ordenar el JSON por número total de retweets al usuario (de mayor a menor)
    retweets_json["retweets"] = sorted(retweets_json["retweets"], key=lambda x: x["receivedRetweets"], reverse=True)
    if arg==True:
        with open("rt.json", "w", encoding="utf-8") as json_file:
            json.dump(retweets_json, json_file, ensure_ascii=False, indent=2)

    return retweets_json

def json_menciones(mentions_info, arg):
    mentions_json = {"mentions": []}

    for username, user_info in mentions_info.items():
        total_mentions = sum(len(mention_info['tweets']) for mention_info in user_info['mentions'])
        user_data = {"username": username, "receivedMentions": total_mentions, "mentions": []}

        for mention_info in user_info["mentions"]:
            mention_data = {"mentionBy": mention_info["mentionBy"], "tweets": mention_info["tweets"]}
            user_data["mentions"].append(mention_data)

        mentions_json["mentions"].append(user_data)

    # Ordenar el JSON por número total de menciones al usuario (de mayor a menor)
    mentions_json["mentions"] = sorted(mentions_json["mentions"], key=lambda x: x["receivedMentions"], reverse=True)
    if arg==True:
        with open("mención.json", "w", encoding="utf-8") as json_file:
            json.dump(mentions_json, json_file, ensure_ascii=False, indent=2)

    return mentions_json


def grafo_retweets(retweets_json):
    G = nx.Graph()

    for author_data in retweets_json["retweets"]:
        author = author_data["username"]
        
        for tweet_id, tweet_data in author_data["tweets"].items():
            G.add_node(author)
            for retweeted_by in tweet_data["retweetedBy"]:
                G.add_node(retweeted_by)
                G.add_edge(author, retweeted_by)

            # Conectar al autor con todos los que retuitearon ese tweet
            G.add_edges_from([(author, retweeted_by) for retweeted_by in tweet_data["retweetedBy"]])

    nx.write_gexf(G, "rt.gexf")


def grafo_menciones(mentions_json):
    G = nx.Graph()

    for user_data in mentions_json["mentions"]:
        username = user_data["username"]
        
        for mention_data in user_data["mentions"]:
            mention_by = mention_data["mentionBy"]
            G.add_node(username)
            G.add_node(mention_by)
            G.add_edge(username, mention_by)

    nx.write_gexf(G, "mención.gexf")


def json_corretweets(retweets_info, arg):
    corrtweets_dict = defaultdict(set)


    for author, author_info in retweets_info.items():
        for tweet_info in author_info["tweets"].values():
            retweeted_by = tweet_info["retweetedBy"]

            if retweeted_by:
                retweeters = set(retweeted_by)
                corrtweets_dict[author].update(retweeters)

    corrtweets_list = []
    for author1, author2 in combinations(corrtweets_dict, 2):
        common_retweeters = corrtweets_dict[author1] & corrtweets_dict[author2]
        if common_retweeters:
            coretweet_data = {
                'authors': {'u1': author1, 'u2': author2},
                'totalCoretweets': len(common_retweeters),
                'retweeters': list(common_retweeters)
            }
            corrtweets_list.append(coretweet_data)
    
    corrtweets_list = sorted(corrtweets_list, key=lambda x: x['totalCoretweets'], reverse=True)

    corrtweets_json = {'coretweets': corrtweets_list}
    if arg==True:
        with open('corrtw.json', 'w', encoding='utf-8') as json_file:
            json.dump(corrtweets_json, json_file, ensure_ascii=False, indent=2)


    return corrtweets_json



def grafo_corretweets(corrtweets_info):
    G = nx.Graph()

    for corrtweet_info in corrtweets_info["coretweets"]:
        author1 = corrtweet_info["authors"]["u1"]
        author2 = corrtweet_info["authors"]["u2"]
        total_corretweets = corrtweet_info["totalCoretweets"]

        # Agregar nodos y aristas al grafo
        G.add_node(author1)
        G.add_node(author2)
        G.add_edge(author1, author2, weight=total_corretweets)

    nx.write_gexf(G, "corrtw.gexf")


if __name__ == "__main__":
    tiempo_inicial = time.time()

    args = parse_args(sys.argv[1:])

    # Desempaquetar los argumentos
    directory = args.get("directory", "data")
    fecha_inicial = args.get("fecha-inicial")
    fecha_final = args.get("fecha-final")
    hashtags_file = args.get("hashtags")

    retweets_info, mentions_info = decompress_and_create_json_files(directory, hashtags_file, fecha_inicial, fecha_final)

    if args.get("grt") or args.get("jrt"):
        rt_json = json_retweets(retweets_info, args.get("jrt"))
        if args.get("grt"):
            grafo_retweets(rt_json)

    if args.get("gm") or args.get("jm"):
        mentions_json = json_menciones(mentions_info, args.get("jm"))
        if args.get("gm"):
            grafo_menciones(mentions_json)

    if args.get("gcrt") or args.get("jcrt"):
        corrtweets_json = json_corretweets(retweets_info, args.get("jcrt"))
        if args.get("gcrt"):
            grafo_corretweets(corrtweets_json)

    tiempo_final = time.time()
    tiempo_total = tiempo_final - tiempo_inicial
    print(f"Tiempo total de ejecución: {tiempo_total} segundos.")
