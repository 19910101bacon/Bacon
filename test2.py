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

    #QUERY2 = ('SELECT user_id\
    #rom fb_data.Post_Keywords')


    QUERY2 = ('SELECT A.user_id as ID, B.keyword as key \
    From fb_data.Post_New as A LEFT JOIN fb_data.Post_Keywords as B \
    On A.post_id = B.user_id')

    result_iterator = BQ(Project, QUERY2)
    column_headers = [field.name for field in result_iterator.schema]
    rows = [row.values() for row in iter(result_iterator)]

    Final_Table = pd.DataFrame(rows, columns=column_headers)

    #Final_Table.to_csv('Final_Table.csv', index = False)
    print(Final_Table)
    quit()

    search_name = '60397536269'

    try:
        Target_Final_Table = Final_Table[Final_Table.ID == search_name]
    except:
        print('There are no this ID in Database')
        quit()


    Result = Target_Final_Table.groupby('group')['credibility'].agg(['sum','count'])
    Result['percentage1'] = Result['sum']/sum(Result['sum'])
    Result['percentage2'] = Result['count']/sum(Result['count'])
    print(Result)


    """
    col = ['Name', 'ID', 'Group_0', 'Group_1', 'Group_2', 'Group_3', 'Group_4', 'Group_5', 'Group_6', 'Group_7', 'Group_8', 'Group_9']
    D = pd.DataFrame([['柯文哲', '136845026417486', '0.150871329672953','0.0284077345428503','0.107424206254476','0.0845070422535211','0.16591071854858','0.0305562186679398','0.0224397230842683','0.129863929338744','0.110766292671282','0.169252804965386'],
                  ['徐熙娣', '1509106909358660',  '0.150527137836783','0.0431472081218274','0.0324092151503319','0.158531823506443','0.131784459195627','0.132955876610699','0.106794221007419','0.0304568527918782','0.113627489262007','0.0997657165169856'],
                  ['葛仲珊', '60397536269', '0.079312257348863','0.0134960251432797','0.135330005546312','0.11924570160843','0.118875947494916','0.0863375855056387','0.133481234978739','0.126640783878721','0.0290256979108893','0.158254760584212'],
                  ['謝和弦', '354487984641189', '0.0183752417794971','0.103675048355899','0.0413926499032882','0.1678916827853','0.105029013539652','0.0676982591876209','0.112379110251451','0.0864603481624758','0.176208897485493','0.120889748549323'],
                  ['炎亞綸', '102954753119805', '0.103209459459459','0.0396959459459459','0.0552364864864865','0.0910472972972973','0.0891891891891892','0.0886824324324324','0.0785472972972973','0.157263513513514','0.160135135135135','0.136993243243243'],
                  ['安心亞', '192348605466', '0.15072202166065','0.00487364620938628','0.156498194945848','0.137003610108303','0.0826714801444043','0.0164259927797834','0.138628158844765','0.12129963898917','0.0736462093862816','0.118231046931408'],
                  ['曹西平', '169987619690567', '0.00363699341877381','0.127987530308279','0.162279182542432','0.124004156563907','0.13751298926221','0.163318323519224','0.015413924489089','0.169553169379979','0.00710079667474887','0.0891929338413578'],
                  ['沈玉琳', '296552120461731', '0.22311766060327','0.0973981119042137','0.0221045360349988','0.135620538798066','0.0138153350218743','0.0693069306930693','0.216900759843426','0.0538798065853097','0.10223347916187','0.0656228413539028'],
                  ['蔡英文', '46251501064', '0.0710221285563751','0.0101159114857745','0.208851422550053','0.148366701791359','0.0472075869336143','0.131717597471022','0.0453108535300316','0.130031612223393','0.087881981032666','0.119494204425711'],
                  ['林俊傑', '193480161484', '0.149844293826708','0.134090492764243','0.0483605055871039','0.15973621542407','0.0450631983879831','0.0778530866459058','0.0569701410514746','0.128045429565855','0.0261952738596813','0.173841362886976']], columns = col)

    to_gbq(D,'fb_data.Ten_Celebrities', Project, if_exists = 'replace')
    print(D)
    quit()
    """
## BQ -> Post_Article -> Word2Vec
# Word2Vec -> W2V_word
## GCP -> NLP_keyword
## ___________________
## >> Common_keyword
