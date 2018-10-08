import pandas as pd
from sklearn.preprocessing import LabelEncoder
from matplotlib import pyplot as plt
import numpy as np
import math

df = pd.read_csv(filepath_or_buffer='/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/activitiy_recognition_csv/merged.csv', header=None)
df.dropna(how="all", inplace=True)
header = df.iloc[0] # consider that header is always present
df = df[1:] # consider that header is always present
X =df.iloc[:,0:-1].values
y = df.iloc[:,-1].values
enc = LabelEncoder()
label_encoder = enc.fit(y)
y = label_encoder.transform(y) + 1
y.astype(int)
y_unique = np.unique(y)
np.set_printoptions(precision=4)
mean_vectors = []

for cl in range(1,(y_unique.shape[0] + 1)):
    mean_vectors.append(np.mean(X[y==cl], axis=0))
    print('Mean Vector class %s: %s\n' %(cl, mean_vectors[cl-1]))

overall_mean = np.mean(X, axis=0)
print ('Overall mean:: ', overall_mean)
mean_vectors_shape = mean_vectors[0].shape[0] # mean_vectors shape (4,)
S_W = np.zeros((mean_vectors_shape,mean_vectors_shape))
for cl,mv in zip(range(1,(y_unique.shape[0] + 1)), mean_vectors):
    print 'inside within cluster'
    class_sc_mat = np.zeros((mean_vectors_shape,mean_vectors_shape)) # scatter matrix for every class
    for row in X[y == cl]:
        print row
        row, mv = row.reshape(-1,1), mv.reshape(-1,1) # make column vectors
        class_sc_mat += (row-mv).dot((row-mv).T)
    print 'outside row'
    S_W += class_sc_mat # sum class scatter matrices
print('within-class Scatter Matrix:\n', S_W)



S_B = np.zeros((mean_vectors_shape,mean_vectors_shape))
for i,mean_vec in enumerate(mean_vectors):
    print 'between matrix'
    n = X[y==i+1,:].shape[0]
    mean_vec = mean_vec.reshape(-1,1) # make column vector
    overall_mean = overall_mean.reshape(-1,1) # make column vector
    S_B += n* (mean_vec - overall_mean).dot((mean_vec - overall_mean).T)
    
print('between-class Scatter Matrix:\n', S_B)

eig_vals, eig_vecs = np.linalg.eig(np.linalg.inv(S_W).dot(S_B))
print((np.linalg.inv(S_W).dot(S_B)).shape, eig_vecs.shape ,eig_vals.shape)
for i in range(len(eig_vals)):
    eigvec_sc = eig_vecs[:,i].reshape(-1,1)
    print('\nEigenvector {}: \n{}'.format(i+1, eigvec_sc.real))
    print('Eigenvalue {:}: {:.2e}'.format(i+1, eig_vals[i].real))

# Make a list of (eigenvalue, eigenvector) tuples
eig_pairs = [(np.abs(eig_vals[i]), eig_vecs[:,i]) for i in range(len(eig_vals))]

# Sort the (eigenvalue, eigenvector) tuples from high to low
eig_pairs = sorted(eig_pairs, key=lambda k: k[0], reverse=True)

# Visually confirm that the list is correctly sorted by decreasing eigenvalues
print('Eigenvalues in decreasing order:\n')
for i in eig_pairs:
    print(i[0])

print('Variance explained:\n')
eigv_sum = sum(eig_vals)
W_list = []
for i,j in enumerate(eig_pairs):
    if (j[0]/eigv_sum).real > 0.0001:
        W_list.append(eig_pairs[i] [1].reshape(-1,1).real)
        print('eigenvalue {0:}: {1:.2%}'.format(i+1, (j[0]/eigv_sum).real))

W_arr = np.concatenate(W_list, axis=1)
print 'W_arr::'
print W_arr
# first column of w is eigenvector 1 and sec column of w is eigenvector 2
W = np.hstack((eig_pairs[0] [1].reshape(-1,1),eig_pairs[1][1].reshape(-1,1)))

print('Matrix W:\n', W.real)

X_lda = X.dot(W)
print X_lda.shape
#assert X_lda.shape == (150,2), "The matrix is not 150x2 dimensional."

X_lda_dynamic = X.dot(W_arr)
print X_lda_dynamic.shape
#assert X_lda_dynamic.shape == (150,2), "The matrix is not 150x2 dimensional."