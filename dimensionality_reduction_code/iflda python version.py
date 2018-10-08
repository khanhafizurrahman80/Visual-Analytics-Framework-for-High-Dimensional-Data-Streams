#from __future__ import print_function
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder

np.set_printoptions(precision=4)
df = pd.read_csv(filepath_or_buffer='/Users/khanhafizurrahman/Desktop/Thesis/code/Thesis_Implementation/UploadFiles/stock_dataset_numeric.csv') # dataframe : dataset-> Iris dataset
df1 = df
null_columns = df1.columns[df1.isnull().any()]
df1 = df1.drop(null_columns, axis=1)
Y = pd.DataFrame(df1['class'])
y = np.ravel(Y.values)
enc = LabelEncoder()
label_encoder = enc.fit(y)
y = label_encoder.transform(y) + 1
feature_label_data = np.ravel(y)
feature_label_data = feature_label_data.T
feature_data = df1.drop(['class','defaultHeader'], axis=1) # all columns are of float64, numpy ndarray
feature_data = feature_data.T

"""print('feature data shape::', feature_data.shape, '\n feature data first five sample::', feature_data[:,0:4], 
      '\n feature label data shape::', feature_label_data.shape, '\n feature label data first five sample::', feature_label_data[:,0:4])"""

def getMean(X, XLabel):
    print X.shape, XLabel.shape
    XLabel_ = XLabel.reshape(-1,1)
    CLabel = np.unique(XLabel_)
    mean_vectors = np.zeros((X.shape[1],len(CLabel)))
    k = 0
    for i in(CLabel):
        loc=(XLabel_ == i).nonzero()[1] # at [0] gives value, [1] gives position
        mean_value = np.mean(X[:,loc],axis=1).reshape(-1,1) # axis 1 because it is equivalent in matlab mean(X,2)
                                                            # reshape to make it column vector
        mean_vectors[:,k] = mean_value.flatten()  # flatten need to make it one dimensional array
        k = k +1
    return mean_vectors,CLabel

def FLDA_Cholesky(feature_data,feature_label_data):
    C, CLabel = getMean(feature_data, feature_label_data)
    L = np.dot(C.T,C)
    R = np.linalg.cholesky(L).T # T to make equivalent to matlab code
    R_Inv = np.linalg.inv(R)
    Q = np.dot(C,R_Inv)
    G = np.dot(Q,R_Inv.T)
    return G,Q,C

def isSquare(m):
    return all (len (row) == len (m) for row in m)

G,Q,C = FLDA_Cholesky(feature_data, feature_label_data)
print G

#print('G:: \n', G, '\n Q:: \n', Q, '\n C:: \n', C)

# started from here the mathematical calculation which are not in the algorithm description

reduced_data = np.dot(feature_data.T, G)
#print reduced_data


