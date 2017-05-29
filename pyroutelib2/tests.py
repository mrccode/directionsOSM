import pandas as pd
import numpy as np
from multiprocessing import Pool
from sklearn.datasets import load_iris

num_partitions = 10 #number of partitions to split dataframe
num_cores = 4 #number of cores on your machine

data = load_iris()
iris = pd.DataFrame(data.data, columns=data.feature_names)
print(iris.head())

def parallelize_dataframe(df, func):
    df_split = np.array_split(df, num_partitions)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def multiply_columns(data):
    data['something'] = data['sepal width (cm)'].apply(lambda x: x**x)
    return data


iris = parallelize_dataframe(iris, multiply_columns)

print(iris.head())