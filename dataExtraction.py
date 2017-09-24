# -*- coding: utf-8 -*-
"""
Created on Wed Jul  5 17:59:25 2017

@author: Sudath
"""

import pandas as pd
import numpy as np
import datetime as dt

os.chdir('C:/Users/sudat/Downloads/Mayo Clinic project')

patientid = '97810'
recordedDate = '01142017'

featureMeasured = 'breathingrate'
pd.to_datetime(str(recordedDate), format = '%m%d%Y')
fileName = patientid+ '_'+featureMeasured+ '_'+ recordedDate+ '.csv'
filePath = ('C:/Users/sudat/Downloads/Mayo Clinic project/Data/'+patientid+ '/' + fileName)
df = pd.read_csv(filePath)
df[' Measurement Date'] = pd.to_datetime(df[' Measurement Date'], infer_datetime_format=True)
df_resample_breathing = df[[' Measurement Date',' Respiration Rate']]
df_resample_breathing.set_index(pd.to_datetime(df_resample_breathing[' Measurement Date']), inplace=True)
df_resample_breathing = df_resample_breathing.resample('10min', how=np.mean)


featureMeasured = 'weight'
fileName = patientid+ '_'+featureMeasured+ '_'+ recordedDate+ '.csv'
filePath = ('C:/Users/sudat/Downloads/Mayo Clinic project/Data/'+patientid+ '/' + fileName)
df = pd.read_csv(filePath)
df['Observation Date'] = pd.to_datetime(df['Observation Date'], infer_datetime_format=True)
df_resample_weight = df[['Observation Date',' Weight (kg)']]
df_resample_weight.set_index(pd.to_datetime(df_resample_weight['Observation Date']), inplace=True)
df_resample_weight = df_resample_weight.resample('10min', how=np.mean)



featureMeasured = 'bloodpressure'
fileName = patientid+ '_'+featureMeasured+ '_'+ recordedDate+ '.csv'
filePath = ('C:/Users/sudat/Downloads/Mayo Clinic project/Data/'+patientid+ '/' + fileName)
df = pd.read_csv(filePath)
df['Observation Date'] = pd.to_datetime(df['Observation Date'], infer_datetime_format=True)
df_resample_bp = df[['Observation Date',' Systolic', ' Diastolic']]
df_resample_bp.set_index(pd.to_datetime(df_resample_bp['Observation Date']), inplace=True)
df_resample_bp = df_resample_bp.resample('10min', how=np.mean)

df_resample =df_resample_breathing.join(df_resample_weight, how='outer').join(df_resample_bp, how ='outer')

index = pd.date_range(pd.to_datetime(str(recordedDate), format = '%m%d%Y'), periods=144, freq='10T')
df_refrence = pd.DataFrame(index=index)

df_resample = df_resample.join(df_refrence, how='outer')

df_resample = df_resample.ffill().bfill()

df_resample.to_csv('C:/Users/sudat/Downloads/Mayo Clinic project/Data/example.csv')

