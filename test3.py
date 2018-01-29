import requests
import json
from datetime import datetime, time, timedelta
import pandas as pd
from bs4 import BeautifulSoup
from pandas.io.gbq import *
from google.cloud import bigquery


#################################
#user_ID = '975502379179828_1736125623117496'
#Access_token = 'EAAdF3NjELPkBAIDdA9xqdB4mkRUWGW7RoZBem7ZC8uh6OaiC7khNvhot8ZAsSzPr5mp7OZAvKb1OtorR2AJvjJWRacwBVWV3YA2NVz4nAwUZCj55YAzBPFJ9B7oIG3gLX4LnmziqvRlQjVlGk7SdUek2MN1eNRI2Rkqh0k6AuEzaEcB5qWbwYcO7AC35Mxk0ZD'

#################################
def BQ(Project, Query):
    ## connecting with BigQuery on GCP, and quering the data you want
    ## input : GCP project name ,e.g. 'project-ai-187007'
    ##        QUERY = (
    ##        'SELECT  message, tokenString FROM `fb_data.post_new` ')
    client = bigquery.Client(project = Project)
    query_job = client.query(Query)
    TIMEOUT = 30
    iterator = query_job.result(timeout = TIMEOUT)
    return iterator

Project = 'project-ai-187007'

QUERY2 = ('SELECT A.* FROM fb_data.clustering_result_new as A')

result_iterator = BQ(Project, QUERY2)
column_headers = [field.name for field in result_iterator.schema]
rows = [row.values() for row in iter(result_iterator)]

Final_Table = pd.DataFrame(rows, columns=column_headers)

Each_Group_Count = Final_Table.groupby('group').size().to_frame()

a = Final_Table.join(Each_Group_Count, on = ['group'])
a['credibility'] = a['credibility']/a[0]
a = a.drop(0, axis = 1)
to_gbq(a,'fb_data.Clustering_Result_New', Project, if_exists = 'replace', verbose=True)

#print(a)
quit()
print(Final_Table)

for i in range(len(Final_Table)):
    divide = Each_Group_Count[int(Final_Table['group'][i])]
    a = Final_Table['credibility'][i]/divide
    Final_Table.at['credibility',i] = a
    print(i)

print(Final_Table)


to_gbq(Final_Table,'fb_data.Clustering_Result_New', Project, if_exists = 'replace', verbose=True)








quit()

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
