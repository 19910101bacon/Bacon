import requests
import json
from datetime import datetime, time, timedelta
import pandas as pd
from bs4 import BeautifulSoup
from pandas.io.gbq import *

#################################
#user_ID = '975502379179828_1736125623117496'
#Access_token = 'EAAdF3NjELPkBAIDdA9xqdB4mkRUWGW7RoZBem7ZC8uh6OaiC7khNvhot8ZAsSzPr5mp7OZAvKb1OtorR2AJvjJWRacwBVWV3YA2NVz4nAwUZCj55YAzBPFJ9B7oIG3gLX4LnmziqvRlQjVlGk7SdUek2MN1eNRI2Rkqh0k6AuEzaEcB5qWbwYcO7AC35Mxk0ZD'

#################################

def post_new_table(article_id_s, Access_token):
    ## Result Form
    col = ['all_time', 'year', 'month', 'day', 'date', 'time', 'user_id', 'post_id', 'message', 'count', 'tokenization','magnitude', 'score']
    Result = pd.DataFrame(columns = col)

    for i in range(len(article_id_s)):
        article_id_now = article_id_s[i]
        comment = '?fields=created_time,id,message,link,type,shares,message_tags'
        link = '&access_token='
        url = 'https://graph.facebook.com/v2.11/' + article_id_now +comment + link + Access_token

        response = requests.get(url)
        html = json.loads(response.text)
        All_Time = html['created_time']
        Year = html['created_time'][0:4]
        Month = html['created_time'][5:7]
        Day = html['created_time'][8:10]
        Date = html['created_time'][0:10]
        Time = html['created_time'][11:16]
        User_ID = html['id'].split('_')[0]
        Post_ID = html['id']
        Message = html['message']
        Count = str(html['shares']['count'])
        a = requests.get('https://us-central1-project-ai-187007.cloudfunctions.net/api/language/analyzeSyntax?text=' + Message)
        b = requests.get('https://us-central1-project-ai-187007.cloudfunctions.net/api/language/analyzeSentiment?text=' + Message)
        Tokenization = a.text[16:-2]
        Magnitude = float(b.json()[0]['documentSentiment']['magnitude'])
        Score = float(b.json()[0]['documentSentiment']['score'])
        # col = ['all_time', 'year', 'month', 'day', 'date', 'time', 'user_id', 'post_id', 'message', 'count', 'tokenization','magnitude', 'score']
        # a = pd.DataFrame([[All_Time, Year, Month, Day, Date, Time, User_ID, Post_ID, Message, Count, Tokenization, Magnitude, Score]], columns = col)
        Result.loc[i] = [All_Time, Year, Month, Day, Date, Time, User_ID, Post_ID, Message, Count, Tokenization, Magnitude, Score]
        #print(i)
    Project = 'project-ai-187007'
    #print(Result.dtypes)
    try:
        to_gbq(Result,'fb_data.Post_New', Project, if_exists = 'append', verbose=True)
        print('post_new table update finish')
    except:
        pass
        print('unfinish')
