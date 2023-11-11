# Import Libraries
#import re
import pandas as pd
import numpy as np
import missingno as msno
pd.set_option('display.width',170, 'display.max_rows',200, 'display.max_columns',900)
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *

# helper methods to deal with dataframe
def display_dataframe_info(df):
    print ("\n\n---------------------")
    print ("DF Info")
    print ("---------------------")
    print ("Dataframe shape:", df.shape, "\n")
    print ("Column Headers:", list(df.columns.values), "\n")
    print (df.dtypes)

def display_dataframe_report(df):
    import re
    
    missing = []
    non_numeric = []
    
    for column in df:
        # find unique values
        values = df[column].unique() 
        uniq_val = "{} has {} unique values".format(column, values.size)
        print(uniq_val)
        
        if (values.size > 10):
            print("Listing up to 12 unique values:")
            print(values[0:12])
            print ("\n---------------------\n")
            
        # find missing values
        if (True in pd.isnull(values)):
            percentage = 100 * pd.isnull(df[column]).sum() / df.shape[0]
            mis_val = "{} is missing in {}, {}%.".format(pd.isnull(df[column]).sum(), column, percentage)
            missing.append(mis_val)

        # find non-numeric values
        for i in range(1, np.prod(values.shape)):
            if (re.match('nan', str(values[i]))):
                break
            if not (re.search('(^\d+\.?\d*$)|(^\d*\.?\d+$)', str(values[i]))):
                non_numeric.append(column)
                break
    print ("\n~~~~~~~~~~~~~~~~~~~~~~\n")
    
    print ("Missing values:")
    for i in range(len(missing)):
        print("\n{}" .format(missing[i]))
        
    print ("\n Non-numeric values:")
    print("\n{}" .format(non_numeric))
        
    print ("\n~~~~~~~~~~~~~~~~~~~~~~\n")
    
    
# Cleaning dataframe method
    
    
def remove_missing_data(df):
    """
        Clean the data within the dataframe.
        
        :param df: dataframe
        :return: the cleaned dataframe
    """
    print("Removing missing data...")
    
    columns_to_drop = []
    
    for column in df:
        values = df[column].unique() 
        
        # search missing values
        
        if (True in pd.isnull(values)):
            percentage_missing = 100 * pd.isnull(df[column]).sum() / df.shape[0]
            
            if (percentage_missing >= 90):
                columns_to_drop.append(column)
    
    # Remove columns missing 90% or more of their values
    df = df.drop(columns=columns_to_drop)
    
    # remove rows where all elements are missing
    df = df.dropna(how='all')
    
    print("Cleaning complete!")
    
    return df

def remove_duplicate_rows(df, cols=[]):
    """
        Remove duplicate data within the dataframe.
        
        :param df: dataframe
        :return: the cleaned dataframe
    """
    print("Removing duplicate rows...")
    
    row_count_before = df.shape[0]
    
    # remove duplicate rows
    
    df = df.drop_duplicates()
    
    print("{} rows removed.".format(row_count_before - df.shape[0]))
    
    return df

def write_to_parquet(df, output_path, table_name):
    """
        Writes the dataframe as parquet file.
        
        :param df: dataframe to write
        :param output_path: output path where to write
        :param table_name: name of the table
    """
    
    file_path = output_path + table_name
    
    print("Writing table {} to {}".format(table_name, file_path))
    
    df.write.mode("overwrite").parquet(file_path)
    
    print("Write to parquet completed")
    
    
def perform_quality_check(input_df, table_name):
    
    """Check data completeness
        :param input_df: spark dataframe to check counts on.
        :param table_name: name of table
    """
    
    record_count = input_df.count()

    if (record_count == 0):
        
        print("Data quality check failed for {} with no records.".format(table_name))
    else:
        print("Data quality check passed for {} with record_count: {} records.".format(table_name, record_count))
        
    return 0
    
    