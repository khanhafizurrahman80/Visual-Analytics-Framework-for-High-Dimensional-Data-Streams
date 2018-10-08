from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import time

def lda_function():
    df = pd.read_csv('/Users/khanhafizurrahman/Desktop/Thesis/code/Thesis_Implementation/UploadFiles/merged.csv')
    Y = df.iloc[:,-2].values
    Y = pd.DataFrame(Y)
    y = np.ravel(Y.values)
    enc = LabelEncoder()
    label_encoder = enc.fit(y)
    y = label_encoder.transform(y) + 1
    print y.shape
    X = df.iloc[:,0:-2].values
    X = pd.DataFrame(X)
    print X.shape
    #X = df.drop(['class'], axis=1)
    indices_to_keep = ~X.isin([np.nan, np.inf, -np.inf]).any(1)
    X = X[indices_to_keep].astype(np.float64)
    print X.shape
    sklearn_lda = LDA()
    X_lda_sklearn = sklearn_lda.fit(X,y).transform(X) # numpy array
    output_df = pd.DataFrame(X_lda_sklearn)
    frames = [output_df, pd.DataFrame(y)]
    output_df = pd.concat(frames, axis =1)
    output_df.to_csv('/Users/khanhafizurrahman/Desktop/ThesisFinalCodeBackup/Dataset_I_used/activity_recognition_csv/output_LDA_merged.csv',index=None, header=['ld1','ld2','ld3','ld4','ld5','ld6','ld7','ld8','ld9','ld10','ld11','ld12','ld13','ld14','ld15','ld16','ld17','ld18','class'])
    print output_df.shape

if __name__ == '__main__':
    start_time = time.time()
    lda_function()
    print("--- %s seconds ---" % (time.time() - start_time))

