import matplotlib.pyplot as plt
import pandas as pd

""" convert image to csv"""
"""
for i in range(10):
    dir_img = '/Users/khanhafizurrahman/Desktop/Thesis/selectedPaper/dataset/paper used dataset/att_faces/s40/' + str(i+1) + '.pgm'
    dir_csv = '/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/orl_dataset/s40/' + str(i+ 1) + '.csv'
    img = plt.imread(dir_img)
    df = pd.DataFrame(img)
    df.to_csv(dir_csv, index= False) """

""" make one file """

df_list = []
for i in range(10):
    dir_csv = '/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/orl_dataset/s40/' + str(i+ 1) + '.csv'
    read_df = pd.read_csv(dir_csv)
    read_df['class'] = pd.Series(i+1, index=read_df.index)
    df_list.append(read_df)

final_df = pd.concat(df_list)
final_df.to_csv('/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/orl_dataset/merged_csv/s40.csv', index= False)
print final_df.shape
    

