import pandas as pd
import numpy as np
from sklearn import preprocessing
from sklearn.preprocessing import LabelEncoder
import sympy

data = pd.read_csv('/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/nyse/fundamentals.csv')
print data.shape, data.size

remove_reduced_data = data.dropna()
null_columns = data.columns[data.isnull().any()]
print null_columns
remove_reduced_data = remove_reduced_data.drop(['Unnamed: 0', 'Period Ending'], axis=1)
print remove_reduced_data.shape, remove_reduced_data.size
class_level_dataframe = remove_reduced_data['Ticker Symbol']
remove_reduced_data = remove_reduced_data.drop(['Ticker Symbol'], axis=1)
q,r = np.linalg.qr(remove_reduced_data)
print q.shape
reduced_form, inds = sympy.Matrix(remove_reduced_data.values).rref()
print inds
remove_reduced_data = remove_reduced_data.iloc[:, inds]
Y = class_level_dataframe
y = np.ravel(Y.values)
enc = LabelEncoder()
label_encoder = enc.fit(y)
y = label_encoder.transform(y) + 1
remove_reduced_data['class']= y
remove_reduced_data.to_csv('/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/a/stock_dataset.csv',index = False)