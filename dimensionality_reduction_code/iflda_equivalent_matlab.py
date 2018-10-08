#from __future__ import print_function
import pandas as pd
import numpy as np


np.set_printoptions(precision=4)
read_df = pd.read_csv(filepath_or_buffer='/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/orl_dataset/merged_csv/s1.csv', header=None) # dataframe : dataset-> Iris dataset
"""
feature_data = read_df[[0,1,2,3]].values # numpy ndarray
print feature_data.shape
feature_data = feature_data.T
feature_label_data = read_df[[4]].values # numpy ndarray
print feature_label_data.shape
feature_label_data = feature_label_data.T
"""

"""the following part is for using dynamic part """

feature_data = read_df.iloc[:,0:-1].values # all columns are of float64, numpy ndarray
feature_data = feature_data.T
print 'feature_data'
print feature_data.shape

feature_label_data = read_df.iloc[:,-1].values.reshape(-1,1)
feature_label_data = feature_label_data.T
print 'feature_label_data'
print feature_label_data.shape # numpy array


"""dynamic part end""" 
"""print('feature data shape::', feature_data.shape, '\n feature data first five sample::', feature_data[:,0:4], 
      '\n feature label data shape::', feature_label_data.shape, '\n feature label data first five sample::', feature_label_data[:,0:4])"""

def getMean(X, XLabel):
    CLabel = np.unique(XLabel)
    mean_vectors = np.zeros((X.shape[0],len(CLabel)))
    k = 0
    for i in(CLabel):
        loc=(XLabel == i).nonzero()[1] # at [0] gives value, [1] gives position
        mean_value = np.mean(X[:,loc],axis=1).reshape(-1,1) # axis 1 because it is equivalent in matlab mean(X,2)
                                                            # reshape to make it column vector
        mean_vectors[:,k] = mean_value.flatten()  # flatten need to make it one dimensional array
        k = k +1
    print 'mean_vectors'
    print mean_vectors.shape
    print 'CLabel'
    print CLabel.shape
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
print 'G'
print G.shape

output = np.dot(feature_data.T,G)
print 'output'
print output.shape

