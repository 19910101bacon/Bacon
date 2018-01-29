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

def post_keywords_table(article_id_s, Access_token):
    ## Result Form
    col = ['user_id', 'keyword', 'type', 'salience']
    Result = pd.DataFrame(columns = col)
    index = 0
    for i in range(len(article_id_s)):
        article_id_now = article_id_s[i]
        comment = '?fields=id,message'
        link = '&access_token='
        url = 'https://graph.facebook.com/v2.11/' + article_id_now +comment + link + Access_token

        response = requests.get(url)
        html = json.loads(response.text)
        Post_ID = html['id']
        Message = html['message']

        a = requests.get('https://us-central1-project-ai-187007.cloudfunctions.net/api/language/analyzeEntities?text=' + Message)
        focus = a.json()[0]['entities']
        try:
            for j in range(len(focus)):
                tem = focus[j]
                Result.loc[index] = [Post_ID, tem['name'], tem['type'], tem['salience']]
                index += 1
        except:
            pass


    Project = 'project-ai-187007'
    #print(Result.dtypes)
    try:
        to_gbq(Result,'fb_data.Post_Keywords', Project, if_exists = 'append', verbose=True)
        print('Post_Keywords table update finish')
    except:
        pass
        print('unfinish')
