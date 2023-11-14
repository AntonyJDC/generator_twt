import os
import timeit
import json
import shutil
import bz2
import getopt
import networkx as nx
from collections import defaultdict
import sys
from datetime import datetime

def gen_graph_retweets(tweets):
    G = nx.DiGraph()
    for tweet in tweets:
        if 'retweeted_status' in tweet:      
            retweeted_user = tweet['retweeted_status']['user']['screen_name']
            retweeting_user = tweet['user']['screen_name']

            G.add_node(retweeting_user)
            G.add_node(retweeted_user)

            G.add_edge(retweeting_user, retweeted_user)

    return G

def create_json_retweets(tweets):
    retweets_info = defaultdict(lambda: {'receivedRetweets': 0, 'tweets': defaultdict(list)})

    for tweet in tweets:
        if 'retweeted_status' in tweet:
            retweeted_user = tweet['retweeted_status']['user']['screen_name']
            retweeting_user = tweet['user']['screen_name']
            tweet_id = tweet['retweeted_status']['id_str']

            retweets_info[retweeted_user]['tweets'][tweet_id].append(retweeting_user)
            retweets_info[retweeted_user]['receivedRetweets'] += 1

    sorted_retweets = sorted(retweets_info.items(), key=lambda x: x[1]['receivedRetweets'], reverse=True)

    json_structure = {'retweets': []}
    for user, data in sorted_retweets:
        tweets_data = [{'tweetID: ' + tweet_id: {'retweetedBy': retweeters} for tweet_id, retweeters in data['tweets'].items()}]
        json_structure['retweets'].append({
            'username': user,
            'receivedRetweets': data['receivedRetweets'],
            'tweets': tweets_data
        })

    return json_structure


def create_graph_mentions(tweets):
    G = nx.DiGraph()
    for tweet in tweets:
        if 'entities' in tweet and 'user_mentions' in tweet['entities']:
            tweeting_user = tweet['user']['id']
            G.add_node(tweeting_user)

            for mentioned_user in tweet['entities']['user_mentions']:
                mentioned_userid = mentioned_user['id']
                G.add_node(mentioned_userid)
                G.add_edge(tweeting_user, mentioned_userid)

    return G

def crear_json_menciones(tweets):
    mentions_info = defaultdict(lambda: {'mentionBy': defaultdict(int), 'tweets': []})

    for tweet in tweets:
        if 'entities' in tweet and 'user_mentions' in tweet['entities']:
            tweet_id = tweet['id_str']
            mentioning = tweet['user']['screen_name']

            for mentioned_user in tweet['entities']['user_mentions']:
                mentioned_username = mentioned_user['screen_name']
                mentions_info[mentioned_username]['mentionBy'][mentioning] += 1
                mentions_info[mentioned_username]['tweets'].append(tweet_id)

    final_structure = {'mentions': []}
    for username, data in mentions_info.items():
        mention_list = []
        for mentioning_user, count in data['mentionBy'].items():
            mention_list.append({'mentionBy': mentioning_user, 'tweets': list(set(data['tweets']))})

        final_structure['mentions'].append({
            'username': username,
            'receivedMentions': sum(data['mentionBy'].values()),
            'mentions': mention_list
        })

    final_structure['mentions'].sort(key=lambda x: x['receivedMentions'], reverse=True)

    return final_structure

def create_graph_corretweets(tweets):
    G = nx.Graph()

    retweets = defaultdict(set)

    for tweet in tweets:
        if 'retweeted_status' in tweet:
            retweeting_user = tweet['user']['screen_name']
            retweeted_user = tweet['retweeted_status']['user']['screen_name']

            retweets[retweeting_user].add(retweeted_user)

    for retweeting_user, authors in retweets.items():
        for user1 in authors:
            for user2 in authors:
                if user1 != user2:
                    G.add_edge(user1, user2)

    return G

def create_json_corretweets(tweets):
    coretweets = defaultdict(set)
    for tweet in tweets:
        if 'retweeted_status' in tweet:
            retweeted_user = tweet['retweeted_status']['user']['screen_name']
            retweeting_user = tweet['user']['screen_name']
            coretweets[retweeted_user].add(retweeting_user)

    co_retweets = defaultdict(lambda: {'retweeters': set(), 'totalCoretweets': 0})
    users = list(coretweets.keys())
    for i in range(len(users)):
        for j in range(i + 1, len(users)):
            user1, user2 = users[i], users[j]
            co_retweeters = coretweets[user1].intersection(coretweets[user2])
            if co_retweeters:
                co_retweets[(user1, user2)]['retweeters'] = co_retweeters
                co_retweets[(user1, user2)]['totalCoretweets'] = len(co_retweeters)

    json_structure = {'coretweets': []}
    for users, data in co_retweets.items():
        json_structure['coretweets'].append({
            'authors': {'u1': users[0], 'u2': users[1]},
            'totalCoretweets': data['totalCoretweets'],
            'retweeters': list(data['retweeters'])
        })

    return json_structure

def unzip_tweets(directory, start_date_str, end_date_str, output_base_directory):
    start_date = datetime.strptime(start_date_str, "%d-%m-%y")
    end_date = datetime.strptime(end_date_str, "%d-%m-%y")

    tweets=[]

    output_directory = os.path.join(output_base_directory)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    for year in os.listdir(directory):
        year_path = os.path.join(directory, year)
        if os.path.isdir(year_path):
            for month in os.listdir(year_path):
                month_path = os.path.join(year_path, month)
                if os.path.isdir(month_path):
                    for day in os.listdir(month_path):
                        day_path = os.path.join(month_path, day)
                        if os.path.isdir(day_path):
                            for hour in os.listdir(day_path):
                                hour_path = os.path.join(day_path, hour)
                                if os.path.isdir(hour_path):
                                    current_date = datetime(year=int(year), month=int(month), day=int(day))
                                    if start_date <= current_date <= end_date:
                                        for file in os.listdir(hour_path):
                                            if file.endswith('.bz2'):
                                                file_path = os.path.join(hour_path, file)

                                                with bz2.BZ2File(file_path, 'rb') as f:
                                                    for line in f:
                                                        tweet = json.loads(line)
                                                        tweets.append(tweet)
    grafo_retweets=gen_graph_retweets(tweets)
    nx.write_gexf(grafo_retweets, 'rt.gexf')
    retweets_json = create_json_retweets(tweets)
    with open('rt.json', 'w') as file:
        json.dump(retweets_json, file, indent=4)
    grafo_menciones = create_graph_mentions(tweets)
    nx.write_gexf(grafo_menciones, 'mencion.gexf')
    menciones_json = crear_json_menciones(tweets)
    with open('mencion.json', 'w') as file:
        json.dump(menciones_json, file, indent=4)
        grafo_co_retweet = create_graph_corretweets(tweets)
    nx.write_gexf(grafo_co_retweet, 'corrtw.gexf')
    co_retweet_json = create_json_corretweets(tweets)
    with open('corrtw.json', 'w') as file:
        json.dump(co_retweet_json, file, indent=4)

def main(argv):
    directory = 'data'
    start_date = None
    end_date = None
    hashtag_file = None
    
    try:
        opts,args=getopt.getopt(argv,"d:fi:ffh:h")
    except getopt.GetoptError as err:
        print(err)
        print('Uso: generador.py -d <path> -fi <fecha inicial> -ff <fecha final> -h <archivo de hashtags>')
        sys.exit(2)
    
    print(opts)

    for opt, arg in opts:
        print(opt)
        if opt == "-d":
            directory = arg
        elif opt == "-fi":
            start_date = arg
        elif opt == "-ff":
            end_date = arg
        elif opt == "-h":
            hashtag_file = arg

    unzip_tweets(directory,start_date,end_date,hashtag_file,"output")
    
    try:
        if os.path.exists("output"):
            shutil.rmtree("output")
    except Exception as e:
        print(f"Error al eliminar la carpeta: {e}")


if __name__ == "__main__":
   start_time = timeit.default_timer()
   main(sys.argv[1:])
   end_time = timeit.default_timer()
   print(f"Total execution time: {end_time - start_time} seconds")
