import requests
import json
from datetime import datetime, time, timedelta
import pandas as pd

#################################
# ID = '975502379179828'
# Access_token = 'EAACEdEose0cBAHpz6KZCk4xVv2bXibxnp15ZAR3w7oF2WpFXuWqImgiROArrOV9k5ojbQQLohBZCnAPTVZA5GL7CNJ3seDFsFysKS2SZAQ9YTsPsA6etzLAouQgAw9fzeM2uRusFQCV9kV1fhnyW4qSZC829jTFsmGxZAuZBnUTyfllJysCvR2LiTm3qCPZAs1lxhd1cuFgXGSAZDZD'
# time = 90
#################################

# step1 select the time within the article
def Article_ID(user_ID, Access_token, time):
    comment = '?fields=posts.limit(90){created_time, id}'
    link = '&access_token='
    url = 'https://graph.facebook.com/v2.11/' + user_ID +comment + link + Access_token

    response = requests.get(url)
    html = json.loads(response.text)
    Data_tem1 = html['posts']['data']

    now = datetime.now().date()
    before = now - timedelta(days=time)

    ID_select = []
    for item in Data_tem1:
        c = item['created_time'][0:10]
        d = pd.to_datetime(c).date()
        if d > before:
            ID_select.append(item['id'])
    return ID_select
