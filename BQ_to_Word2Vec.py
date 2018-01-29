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

def Kmeans_Credibility(Common_keyword, kmeans_model):
    ## Common_keyword => col : keyword ; row : vector(dimension)
    ## kmeans_model : pleause use sklearn cluster, such as
    ## group_num = 6
    ## Common_keyword = pd.read_csv('Clustering_Keyword.csv')
    ## clf = cluster.KMeans(init='k-means++', n_clusters=group_num, random_state=42)
    ## kmeans = clf.fit(Common_keyword.T)

    Clustering_Table = pd.DataFrame({'Word' : list(Common_keyword.T.index),
    'group' : kmeans.labels_.tolist(),
    'credibility' : 0})

    a = Common_keyword.T.values
    b = distance_matrix(a,a)
    Distance_Matrix = pd.DataFrame(b, index = list(Common_keyword.T.index) , columns = list(Common_keyword.T.index))
    result = []
    for i in range(len(Clustering_Table)):
         word_tem = Clustering_Table['Word'][i]
         word_vector = Common_keyword[word_tem]
         group = Clustering_Table['group'][i]
         center_vector = kmeans.cluster_centers_[group]
         word_others_vector = list(Distance_Matrix[word_tem])

         fraction = distance_matrix([word_vector], [center_vector])[0][0]
         numerator = np.mean(word_others_vector)
         credibility = 1 - fraction/numerator
         result.append(credibility)
    Clustering_Table['credibility'] = result
    return Clustering_Table


if __name__ == '__main__':
    ## quering data
    Project = 'project-ai-187007'
    QUERY = (
            'SELECT  message, tokenization FROM `fb_data.Post_New` ')
    iterator = BQ(Project, QUERY)

    ## pre processing
    column_headers = [field.name for field in iterator.schema]
    rows = [row.values() for row in iter(iterator)]

    Post_Article = pd.DataFrame(rows, columns=column_headers)

    # Pandas -> gensim Word2Vec's input

    W2V_input = [''.join(item).split() for item in Post_Article[['tokenization']].values.tolist() ]
    size = 250
    Word2vec = W2V_array(W2V_input, size, 3)

    # Keyword on GCP_NLP
    QUERY1 = ('SELECT keyword FROM `fb_data.Post_Keywords`')
    keyword_iterator = BQ(Project, QUERY1)
    NLP_keyword = QL(keyword_iterator)

    ## common keyword , for clustering
    W2V_word = list(Word2vec.columns.values)
    common_keyword = list(set(NLP_keyword) & set(W2V_word))
    Common_keyword = Word2vec[common_keyword]
    #Common_keyword.to_csv('Clustering_Keyword.csv', index = False)

    ## clustering by kmeans and calculating weight
    group_num = 10
    clf = cluster.KMeans(init='k-means++', n_clusters=group_num, random_state=42)
    kmeans = clf.fit(Common_keyword.T)
    Clustering_Result = Kmeans_Credibility(Common_keyword, kmeans)

    ## upload DataFrame to BigQuery
    try:
        to_gbq(Clustering_Result,'fb_data.clustering_result_new', Project, if_exists = 'replace')
    except:
        print('Could not create the table because it already exists')

    QUERY2 = ('SELECT T1.ID , T1.key , C.group , C.credibility \
    From fb_data.clustering_result_new as C LEFT JOIN(\
        SELECT A.user_id as ID, B.keyword as key \
        From fb_data.post_new as A LEFT JOIN fb_data.post_keywords as B \
        On A.post_id = B.post_id) as T1 \
    On T1.key = C.Word'
    )

    result_iterator = BQ(Project, QUERY2)
    column_headers = [field.name for field in result_iterator.schema]
    rows = [row.values() for row in iter(result_iterator)]

    Final_Table = pd.DataFrame(rows, columns=column_headers)
    #Final_Table.to_csv('Final_Table.csv', index = False)

    search_name = '114604505257582'

    try:
        Target_Final_Table = Final_Table[Final_Table.ID == search_name]
    except:
        print('There are no this ID in Database')
        quit()


    Result = Target_Final_Table.groupby('group')['credibility'].agg(['sum','count'])
    Result['percentage1'] = Result['sum']/sum(Result['sum'])
    Result['percentage2'] = Result['count']/sum(Result['count'])
    print(Result)


## BQ -> Post_Article -> Word2Vec
# Word2Vec -> W2V_word
## GCP -> NLP_keyword
## ___________________
## >> Common_keyword
