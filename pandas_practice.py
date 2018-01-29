from sklearn import datasets
import pandas as pd
import numpy as np

iris = datasets.load_iris()
x = pd.DataFrame(iris['data'], columns=iris['feature_names'])
y = pd.DataFrame(iris['target'], columns=['target_names'])
data = pd.concat([x,y], axis=1)



#print(data.head(3))
#print(type(iris))
#print(type(iris.data))
#print(type(iris['target']))
#print(iris['target'])
#print(iris['data'])
#print(iris['feature_names'])
#print(data.head(5))
#print(iris)
#print(data.head(5))
#print(iris['target_names'])

print(type(range(5)))
#print(data[5,7,10])
#print(data[[5:10]])  XX
