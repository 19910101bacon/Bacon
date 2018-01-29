from google.cloud import bigquery
from PreProcess.pre_pro_CoMatx import *
import pandas as pd
import numpy as np
from gensim.models import word2vec
import logging
from sklearn import cluster
from scipy.spatial import distance_matrix
from pandas.io.gbq import *


logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)





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

def W2V_array(W2V_input, size, MinCount):
    ## using gensim module and get Word2Vec models
    ## 'size' is word embedding dimmension
    ## transform to array type
    model = word2vec.Word2Vec(W2V_input, size, MinCount)

    vector = [list(model.wv[token]) for token in list(model.wv.vocab.keys())]
    Word2vec = pd.DataFrame(np.array(vector)).T    ## 吃array
    Word2vec.columns = list(model.wv.vocab.keys())
    return Word2vec

def QL(iterator):
    ## after quering, transform result to list (if just quering one column on Bigquery)
    rows = [row.values() for row in iter(iterator)]
    column_headers = [field.name for field in iterator.schema]
    Data_tem = pd.DataFrame(rows, columns=column_headers)
    tem_Result = Data_tem[column_headers].values
    Result = [''.join(item) for item in tem_Result]
    return(list(set(Result)))



if __name__ == '__main__':
    ## quering data
    Project = 'project-ai-187007'

    QUERY2 = ('SELECT T1.ID , T1.key , C.group , C.credibility \
    From fb_data.Clustering_Result_New as C LEFT JOIN(\
        SELECT A.user_id as ID, B.keyword as key \
        From fb_data.Post_New as A LEFT JOIN fb_data.Post_Keywords as B \
        On A.post_id = B.user_id) as T1 \
    On T1.key = C.Word'
    )

    result_iterator = BQ(Project, QUERY2)
    column_headers = [field.name for field in result_iterator.schema]
    rows = [row.values() for row in iter(result_iterator)]

    Final_Table = pd.DataFrame(rows, columns=column_headers)



    #Final_Table.to_csv('Final_Table.csv', index = False)

    Ten_people = ['136845026417486','1509106909358665','60397536269','354487984641189','102954753119805','192348605466','169987619690567','296552120461731','46251501064','193480161484']
    Ten_people_name = ['柯文哲','徐熙娣','葛仲珊','謝和弦','炎亞綸','安心亞','曹西平','沈玉琳','蔡英文','林俊傑']
    col_u = ['Name', 'ID', 'Group_0', 'Group_1', 'Group_2', 'Group_3', 'Group_4', 'Group_5', 'Group_6', 'Group_7', 'Group_8', 'Group_9']
    Update_Table = pd.DataFrame(columns = col_u)
    for item in range(len(Ten_people_name)):
        search_name = Ten_people[item]

        try:
            Target_Final_Table_tem = Final_Table[Final_Table.ID == search_name]
        except:
            print('There are no this ID in Database')
            quit()

        tem1 = Target_Final_Table_tem.groupby('group')['credibility'].agg(['sum','count'])
        Target_Final_Table = pd.DataFrame([0,1,2,3,4,5,6,7,8,9], columns = ['group'])  ## let final table hbe all 'group'
        Target_Final_Table = Target_Final_Table.join(tem1, on = ['group']).fillna(0) ## let final table hbe all 'group'


        try:
            Target_Final_Table['percentage1'] = Target_Final_Table['sum']/sum(Target_Final_Table['sum'])
        except:
            Target_Final_Table['percentage1'] = 0
        try:
            Target_Final_Table['percentage2'] = Target_Final_Table['count']/sum(Target_Final_Table['count'])
        except:
            Target_Final_Table['percentage2'] = 0



        a = [Ten_people_name[item], Ten_people[item]] +  Target_Final_Table['percentage1'].values.tolist()

        Update_Table.loc[item] = a
    print(Update_Table)



to_gbq(Update_Table,'fb_data.Ten_Celebrities', Project, if_exists = 'replace', verbose=True)



## BQ -> Post_Article -> Word2Vec
# Word2Vec -> W2V_word
## GCP -> NLP_keyword
## ___________________
## >> Common_keyword
