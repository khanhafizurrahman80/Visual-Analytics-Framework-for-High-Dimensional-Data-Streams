from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
import sys
from pandas import DataFrame, concat
import numpy as np
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
from sklearn.preprocessing import LabelEncoder

a_try = 0
def createSparkSession(appName,master_server):
    spark = SparkSession \
        .builder \
        .appName(appName) \
        .master(master_server) \
        .getOrCreate()

    return spark

# option("startingOffsets", "earliest") option allow to read data from the beginning otherwise we can only view data when we submit after application starts...
def createInitialDataFrame(spark, kafka_bootstrap_server, subscribe_topic):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
        .option("subscribe", subscribe_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    return df

def inputSchema2(fieldNameList, fieldTypeList):
    schema = StructType([
        StructField("feature_values", ArrayType(DoubleType())), \
        StructField("class", StringType()), \
        StructField("defaultHeader", StringType())
    ])
    """schema = StructType([
        StructField("sepal_length", DoubleType()),
        StructField("sepal_width", DoubleType()),
        StructField("petal_length", DoubleType()),
        StructField("petal_width", DoubleType()),
        StructField("class", StringType()),
        StructField("defaultHeader", StringType()),
    ])"""
    """schema = StructType()
    j = 0
    for i in fieldNameList:
        schema = schema.add(fieldNameList[j], fieldTypeList[j])
        j = j + 1"""
    return schema

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

# In Lda, we found the variance using eigen vectors and choose the best among those variances
def outputOfFLDAAlgorithm(X,G):
    eigen_val_of_G, eigen_vec_of_G = np.linalg.eig(G)
    print(eigen_val_of_G)
    Y = np.dot(X,eigen_vec_of_G)
    return Y

def renameCols(df, output_df):
    input_cols = list(df)
    output_cols = list(output_df)
    i = 0
    for x in output_cols:
        output_df.rename(columns={x: input_cols[i]}, inplace=True)
        i = i + 1
    return output_df

def arrangeDatasets(df, output_df):
    input_cols = list(df)
    output_cols = list(output_df)
    remaining_cols = list(set(input_cols) - set(output_cols))
    for x in remaining_cols:
        output_df[x] = 0
    return output_df

def createColumns(column_list_size):
    print column_list_size
    columns = []
    for i in range(column_list_size):
        columns.append('col' + str(i))
    return columns

def create_dataframe(columns, feature_vals):
    print 'create_dataframe'
    print feature_vals.shape
    print len(columns)
    df = DataFrame(columns= columns)
    print 'feature_vals[0]'
    print feature_vals[0]
    print type(feature_vals[0])
    print feature_vals[0].shape
    for i in range(feature_vals.shape[0]):
        df.loc[i] = feature_vals[i]
    return df

def createOutputDataFrame(output_np_arr, class_values, column_names):
    print column_names[0]
    output_df_cols = ["feature_values", "class", "defaultHeader" ]
    output_df = DataFrame(columns= column_names)
    default_val = np.array(output_np_arr.shape[0] * ["defaultValue"])
    output_df[column_names[0]] = output_np_arr.tolist()
    output_df[column_names[1]] = class_values
    output_df[column_names[2]] = default_val
    print 'createOutputDataFrame'
    print output_df.head()
    return output_df

def dimensionality_reduction(inputSchema_output):
    @pandas_udf(inputSchema_output, functionType=PandasUDFType.GROUPED_MAP)
    def traditional_LDA(df):
        df1 = df
        null_columns = df1.columns[df1.isnull().any()]
        print null_columns
        df1 = df1.drop(null_columns, axis=1)
        Y = DataFrame(df.iloc[:,-2].values)
        y = np.ravel(Y.values)
        feature_val_array = df1['feature_values']
        columns = createColumns(feature_val_array.values[0].shape[0])
        feature_dataframe = create_dataframe(columns, feature_val_array.values)
        X = df.drop(['class','defaultHeader'], axis=1)
        sklearn_lda = LDA()
        X_lda_sklearn = sklearn_lda.fit(feature_dataframe,y).transform(feature_dataframe)
        #X_lda_sklearn = sklearn_lda.fit(X,y).transform(X)
        X_lda_sklearn_df = DataFrame(X_lda_sklearn) # pandas.core.frame.DataFrame
        print(list(X_lda_sklearn_df))
        col_names = list(df)
        output_df = DataFrame(columns = col_names)
        """output_df = renameCols(df,X_lda_sklearn_df)
        print(list(output_df))
        output_df = arrangeDatasets(df,output_df) 
        col_names =  list(output_df)"""
        output_df[col_names[0]] = X_lda_sklearn.tolist()
        output_df[col_names[1]] = Y.values
        output_df[col_names[2]] = ''
        return output_df
    return traditional_LDA

def dimensionality_reduction_FLDA(inputSchema_output):
    @pandas_udf(inputSchema_output, functionType=PandasUDFType.GROUPED_MAP)
    def flda(df):
        df1 = df
        null_columns = df1.columns[df1.isnull().any()]
        print 'null_columns'
        print null_columns
        df1 = df1.drop(null_columns, axis=1)
        feature_data = df1.iloc[:,0:-2].values # all columns are of float64, numpy ndarray
        feature_data = feature_data.T

        print 'feature_data'
        print feature_data.shape

        feature_label_data = df1.iloc[:,-2].values.reshape(-1,1)
        feature_label_data = feature_label_data.T

        print 'feature_label_data'
        print feature_label_data.shape

        G, Q, C = FLDA_Cholesky(feature_data, feature_label_data)
        print G.shape
        #output_of_alg = outputOfFLDAAlgorithm(feature_data, G)
        X_flda_alg = np.dot(feature_data.T, G)
        print X_flda_alg.shape
        X_flda_alg_df = DataFrame(X_flda_alg)
        output_df = renameCols(df,X_flda_alg_df)
        output_df = arrangeDatasets(df,output_df) 
        return output_df
    return flda

def writeStream(df3):
    df3.writeStream \
        .format("console") \
        .option("truncate","false") \
        .start() \
        .awaitTermination()

def test_writeStream_to_kafka(testDataFrame, kafka_bootstrap_server, output_topic):
    testDataFrame.printSchema()
    print(output_topic)
    query = testDataFrame \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", output_topic) \
        .start() \
        .awaitTermination()


#def kafkaAnalysisProcess(appName,master_server,kafka_bootstrap_server,subscribe_topic):
def kafkaAnalysisProcess(appName,master_server,kafka_bootstrap_server,subscribe_topic,schema,fieldNameList):
    spark = createSparkSession(appName,master_server)
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/Users/khanhafizurrahman/Desktop/Thesis/code/Thesis_Implementation/checkPoint/test_writeStream_to_kafka")
    df = createInitialDataFrame(spark, kafka_bootstrap_server, subscribe_topic)
    df.printSchema()
    df = df.selectExpr("CAST(value AS STRING)")
    df1 = df.select(from_json(df.value, schema).alias("json"))
    df1.printSchema()
    df2 = df1.select('json.*')
    field_id = fieldNameList[0]
    traditional_LDA = dimensionality_reduction(schema) 
    df3 = df2.groupby("defaultHeader").apply(traditional_LDA)
    print 'df3 schema::'
    df3.printSchema()
    df3_sub = df3.selectExpr("to_json(struct(*)) AS value")
    #df3_sub = df3.selectExpr("CAST("+field_id+" AS STRING) AS key","to_json(col('df3.feature_values')) AS value")
    """flda = dimensionality_reduction_FLDA(schema)
    df4 = df2.groupby("defaultHeader").apply(flda)
    df4_sub = df4.selectExpr("CAST("+field_id+" AS STRING) AS key","to_json(struct(*)) AS value")"""
    return df3_sub # should be df3, df4

if __name__ == '__main__':
    appName = str(sys.argv[1])
    master_server = str(sys.argv[2])
    kafka_bootstrap_server = str(sys.argv[3])
    subscribe_topic = str(sys.argv[4])
    subscribe_output_topic = str(sys.argv[5])
    fieldNameListNameAsString = str(sys.argv[6])
    fieldNames = map(str,fieldNameListNameAsString.strip('[]').split(','))
    fieldNameList = []
    for i in fieldNames:
        fieldNameList.append(i.replace("\"","")) 
    fieldTypeListNameAsString = str(sys.argv[7])
    fieldTypes = map(str,fieldTypeListNameAsString.strip('[]').split(','))
    fieldTypeList = []
    for i in fieldTypes:
        fieldTypeList.append(i.replace("\"",""))
    print len(fieldNameList)
    print len(fieldTypeList)

    schema = inputSchema2(fieldNameList, fieldTypeList)
    #write_to_console_df = kafkaAnalysisProcess(appName,master_server,kafka_bootstrap_server,subscribe_topic)
    write_to_console_df = kafkaAnalysisProcess(appName,master_server,kafka_bootstrap_server,subscribe_topic, schema, fieldNameList)
    columns_of_the_schema = write_to_console_df.columns
    for column in columns_of_the_schema:
        pass
    test_writeStream_to_kafka(testDataFrame = write_to_console_df, kafka_bootstrap_server = kafka_bootstrap_server, output_topic = subscribe_output_topic)
    #writeStream(df3= write_to_console_df)
    #writeStreamtoKafka(df3= write_to_console_df, output_topic= subscribe_output_topic) # value of df3 will be changed to write_to_kafka_df
