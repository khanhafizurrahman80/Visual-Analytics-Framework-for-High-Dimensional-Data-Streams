import numpy as np
import pandas as pd
import csv
import os
"""
def txttocsv(read_file_path, write_file_path):
    with open(read_file_path, 'r') as in_file:
        stripped = (line.strip() for line in in_file)
        lines = (line.split(",") for line in stripped if line)
        with open(write_file_path, 'w') as out_file:
            writer = csv.writer(out_file)
            writer.writerow(('T_xacc', 'T_yacc', 'T_yacc', 'T_xgyro', 'T_ygyro', 'T_zgyro', 'T_xmag', 'T_ymag', 'T_zmag',
                            'RA_xacc', 'RA_yacc', 'RA_yacc', 'RA_xgyro', 'RA_ygyro', 'RA_zgyro', 'RA_xmag', 'RA_ymag', 'RA_zmag',
                            'LA_xacc', 'LA_yacc', 'LA_yacc', 'LA_xgyro', 'LA_ygyro', 'LA_zgyro', 'LA_xmag', 'LA_ymag', 'LA_zmag',
                            'RL_xacc', 'RL_yacc', 'RL_yacc', 'RL_xgyro', 'RL_ygyro', 'RL_zgyro', 'RL_xmag', 'RL_ymag', 'RL_zmag',
                            'LL_xacc', 'LL_yacc', 'LL_yacc', 'LL_xgyro', 'LL_ygyro', 'LL_zgyro', 'LL_xmag', 'LL_ymag', 'LL_zmag',))
            writer.writerows(lines)

for action_dir in os.listdir("/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/activity_recognition/"):
    if (action_dir != '.DS_Store'):
        print action_dir
        os.chdir('/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/activitiy_recognition_csv/')
        os.makedirs("./" + action_dir + "/")
        for person_dir in os.listdir("/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/activity_recognition/" + action_dir + "/"):
            if (person_dir != '.DS_Store'):
                os.chdir('/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/activitiy_recognition_csv/' + action_dir + "/")
                os.makedirs("./" + person_dir + "/")
            for file_path in os.listdir("/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/activity_recognition/" + action_dir + "/" + person_dir + "/"):
                if (file_path != '.DS_Store'):
                    read_file_path =  "/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/activity_recognition/" + action_dir + "/" + person_dir + "/" + file_path
                    write_file_path = "/Users/khanhafizurrahman/Desktop/ThesisFinalCode/Dataset_I_used/activitiy_recognition_csv/" + action_dir + "/" + person_dir + "/" + file_path.replace(".txt", ".csv")
                    txttocsv(read_file_path, write_file_path)

"""

output_csv_list = []

for action_dir in os.listdir("/Users/khanhafizurrahman/Desktop/ThesisFinalCodeBackup/Dataset_I_used/activity_recognition_csv_drive/"):
    if action_dir != '.DS_Store':
        i = 0
        person_csv_list = []
        for person_dir in os.listdir("/Users/khanhafizurrahman/Desktop/ThesisFinalCodeBackup/Dataset_I_used/activity_recognition_csv_drive/" + action_dir + "/"):
            if person_dir != '.DS_Store':
                pd_csv_list = []
                count = 0
                for file_path in os.listdir("/Users/khanhafizurrahman/Desktop/ThesisFinalCodeBackup/Dataset_I_used/activity_recognition_csv_drive/" + action_dir + "/" + person_dir + "/"):
                    if file_path != '.DS_Store':
                        write_file_path = "/Users/khanhafizurrahman/Desktop/ThesisFinalCodeBackup/Dataset_I_used/activity_recognition_csv_drive/" + action_dir + "/" + person_dir + "/" + file_path
                        if count > 11:
                            break
                        input_df = pd.read_csv(write_file_path)
                        """input_df = input_df.head(25)
                        print input_df"""
                        pd_csv_list.append(pd.DataFrame(input_df.values.reshape(1,-1), index= None))
                        output_df = pd.concat(pd_csv_list)
                        count = count + 1
                      
                if action_dir == 'a01':
                    output_df ['class'] = 'sitting'
                elif action_dir == 'a02':
                    output_df ['class'] = 'standing'
                elif action_dir == 'a03':
                    output_df ['class'] = 'lying on back side'
                elif action_dir == 'a04': 
                    output_df ['class'] = 'lying on right side'
                elif action_dir == 'a05':
                    output_df ['class'] = 'ascending stairs'
                elif action_dir == 'a06':
                    output_df ['class'] = 'descending stairs'
                elif action_dir == 'a07':
                    output_df ['class'] = 'standing in an elevator still'
                elif action_dir == 'a08':
                    output_df ['class'] = 'moving around in an elevator'
                if action_dir == 'a09':
                    output_df ['class'] = 'walking in a parking lot'
                elif action_dir == 'a10':
                    output_df ['class'] = 'walking on a treadmill with a speed of 4 km/h (in flat)'
                elif action_dir == 'a11':
                    output_df ['class'] = 'walking on a treadmill with a speed of 4 km/h (15 deg inclined position)'
                elif action_dir == 'a12':
                    output_df ['class'] = 'running on a treadmill with a speed of 8 km/h'
                elif action_dir == 'a13':
                    output_df ['class'] = 'exercising on a stepper'
                elif action_dir == 'a14':
                    output_df ['class'] = 'exercising on a cross trainer'
                elif action_dir == 'a15':
                    output_df ['class'] = 'cycling on an exercise bike in horizontal positions'
                elif action_dir == 'a16':
                    output_df ['class'] = 'cycling on an exercise bike in vertical positions'
                elif action_dir == 'a17':
                    output_df ['class'] = 'rowing'
                elif action_dir == 'a18':
                    output_df ['class'] = 'jumping'
                elif action_dir == 'a19':
                    output_df ['class'] = 'playing basketball'

                print output_df.shape                
                person_csv_list.append(output_df)

        print len(person_csv_list)
        person_output_df = pd.concat(person_csv_list)
        print person_output_df.shape
        output_csv_list.append(person_output_df)
    
print len(output_csv_list)
for i in range(len(output_csv_list)):
    print i
    output_csv_list[i].shape
final_output_df = pd.concat(output_csv_list)
print final_output_df.shape
final_output_arr = final_output_df.values
print type(final_output_arr)
print final_output_arr.shape
np.random.shuffle(final_output_arr)
print final_output_arr.shape
final_output_df_shuffle = pd.DataFrame(final_output_arr)
print final_output_df_shuffle.shape
final_output_df_shuffle.to_csv('/Users/khanhafizurrahman/Desktop/ThesisFinalCodeBackup/Dataset_I_used/activity_recognition_csv_drive//merged.csv', index = False)




