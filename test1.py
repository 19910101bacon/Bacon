from google.cloud import bigquery
from PreProcess.pre_pro_CoMatx import *
client = bigquery.Client(project='project-ai-187007')
QUERY = (
        'SELECT  message, tokenString FROM `fb_data.post_new` ')
query_job = client.query(QUERY)

TIMEOUT = 30
iterator = query_job.result(timeout = TIMEOUT)


#col_name = []
#for item in iterator.schema :
#    col_name.append(item.name)

#for item in iterator :
#    for order in range(len(col_name)) :
#        print(list(item)[order])


import pandas as pd
import numpy as np

column_headers = [field.name for field in iterator.schema]
rows = [row.values() for row in iter(iterator)]

Post_Article = pd.DataFrame(rows, columns=column_headers)
#print( pandas.DataFrame(rows, columns=column_headers) )
#print(iter(iterator))

## stage1 : 將 '\n', ‘\t’, ' ', '#' 拿掉
#(1).
#for item in Post_Article['tokenization']:
#    print(item.replace('TB',' '))


#print(Post_Article[['tokenization']])
## 刪除標點符號與表情符號、空白，並整合起來
result = []
i = 0
for sent in Post_Article[['tokenString']].values.tolist():
    Article_Class = PPC(sent = sent[0])
    result.append(Article_Class.combination())

sent = ''.join(result)

#fh = open('article.txt', 'w')
#fh.write(sent)
#fh.close()

####################

a = word2vec.Word2Vec(sent, size=10)
print(a)

####################


"""
window_size = 2
vocabulary={}
data=[]
row=[]
col=[]

#print(result[0].split())

from scipy.sparse import coo_matrix
for sent in result:
    for pos, token in enumerate(sent.split()):
        i = vocabulary.setdefault(token,len(vocabulary))
        start=max(0,pos-window_size)
        end=min(len(sent.split()),pos+window_size+1)
        for pos2 in range(start,end):
            if pos2==pos:
                continue
            j=vocabulary.setdefault(sent.split()[pos2],len(vocabulary))
            data.append(1.); row.append(i); col.append(j);
    cooccurrence_matrix=coo_matrix((data,(row,col)))



#print(result[0].split())
print(vocabulary)
print(cooccurrence_matrix.toarray() )

import numpy as np

#print( cooccurrence_matrix.toarray().tolist() )

"""
