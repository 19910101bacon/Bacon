from gensim.models import word2vec
import pandas as pd
import numpy as np
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
sentences = word2vec.Text8Corpus("article.txt")
model = word2vec.Word2Vec(sentences, size=10)


print(list(model.wv.vocab.keys()))
print(list(model.wv['聯名']))



vector = [list(model.wv[token]) for token in list(model.wv.vocab.keys())]
Word2vec = pd.DataFrame(np.array(vector)).T    ## 吃array
Word2vec.columns = list(model.wv.vocab.keys())
print(Word2vec.columns.values)
