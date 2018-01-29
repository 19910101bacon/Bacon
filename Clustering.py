import pandas as pd
import numpy as np
from sklearn import cluster
from scipy.spatial import distance_matrix
from google.cloud import bigquery
from pandas.io.gbq import *


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

    #def naming (Clustering_Table):
        ## designed for CLustering_Table
    #    for

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

if __name__ == '__main__':
    #!/usr/bin/python
    # -*- coding: utf-8 -*-

    Final_Table = pd.read_csv('Final_Table.csv')
    search_name = 114604505257582

    try:
        Target_Final_Table = Final_Table[Final_Table.ID == search_name]
    except:
        print('There are no this ID in Database')
        quit()
    Result = Target_Final_Table.groupby('group')['credibility'].agg(['sum','count'])
    Result['percentage1'] = Result['sum']/sum(Result['sum'])
    Result['percentage2'] = Result['count']/sum(Result['count'])

    print(Result)


    quit()
    QUERY2 = ('SELECT T1.User_ID, T1.Keyword, C.group, C.credibility\
    From fb_data.clustering_result_new as C LEFT JOIN(\
        SELECT A.user_id as User_ID, B.keyword as Keyword \
        From `fb_data.post_new` as A LEFT JOIN `fb_data.post_keywords` as B \
        On A.post_id = B.post_id) as T1 \
    On T1.Keyword = C.Word'
    )
    Project = 'project-ai-187007'
    keyword_iterator = BQ(Project, QUERY2)


    quit()




    group_num = 5
    Common_keyword = pd.read_csv('Clustering_Keyword.csv', encoding='utf-8')

    clf = cluster.KMeans(init='k-means++', n_clusters=group_num, random_state=42)
    kmeans = clf.fit(Common_keyword.T)
    #print(kmeans.inertia_)
    a = Kmeans_Credibility(Common_keyword, kmeans)
    #print(a[a.group == 1])


    Project = 'project-ai-187007'
    client = bigquery.Client(project = Project)

    to_gbq(a,'fb_data.clustering_result_new', Project)



    quit()









    dataset = client.dataset('fb_data')
    table_ref = dataset.table('clustering_result')
    #SCHEMA = [
    #    bigquery.SchemaField('Word', 'STRING', mode='required'),
    #    bigquery.SchemaField('credibility', 'FLOAT', mode='required'),
    #    bigquery.SchemaField('group', 'INTEGER', mode='required')
    #]

    #table = bigquery.Table(table_ref, schema=SCHEMA)
    #table = client.create_table(table)      # build new table

    job = client.load_table_from_file(
        a, table_ref)  #
    quit()

    table = client.get_table(table)
    table = client.update_table(
        table,
        ['schema', 123, 'description']
    )
    #errors = client.insert_rows(table, rows_to_insert)  # API request




    quit()
    table_ref = dataset.table(TABLE_ID)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = 'CSV'
    job_config.skip_leading_rows = 1
    job = client.load_table_from_file(
        csv_file, table_ref, job_config=job_config)  # API request
    job.result()

quit()












Clustering_Table = pd.DataFrame({'Word' : list(Common_keyword.T.index),
'group' : kmeans.labels_.tolist(),
'credibility' : 0})

a = Common_keyword.T.values
b = distance_matrix(a,a)
Distance_Matrix = pd.DataFrame(b, index = list(Common_keyword.T.index) , columns = list(Common_keyword.T.index))

for i in range(len(Clustering_Table)):
     word_tem = Clustering_Table['Word'][i]
     word_vector = Common_keyword[word_tem]
     group = Clustering_Table['group'][i]
     center_vector = kmeans.cluster_centers_[group]
     word_others_vector = list(Distance_Matrix[word_tem])

     fraction = distance_matrix([word_vector], [center_vector])[0][0]
     numerator = np.mean(word_others_vector)
     credibility = 1 - fraction/numerator
     print(credibility)



#print(len(Clustering_Table))
quit()


quit()

Index = kmeans.labels_.tolist()
Row = list(Common_keyword.T.index)

dd = {'word':Row, 'cluster':Index}
df = pd.DataFrame.from_dict(dd)
#for i in range(group_num):
#    print(df[df.cluster == i]['word'].values)

#############
for g in range(group_num):
    index1 = [i for i,j in enumerate(Index) if j == g]
    print([j for i,j in enumerate(Row) if i in index1])
