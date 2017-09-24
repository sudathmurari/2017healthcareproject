# -*- coding: utf-8 -*-
"""
Created on Wed Jul  5 17:59:25 2017

@author: Sudath
"""

import pandas as pd
import numpy as np
import datetime as dt



patientid = '97810'


recordedDateTup = ('01142017','01152017','01162017','01172017','01182017','01192017','01202017','01212017',
'01222017','01232017','01242017','01252017','01262017','01272017','01282017','01292017','01302017','02012017',
'02022017','02032017','02042017','02052017','02062017','02072017','02082017','02092017','02102017','02112017',
'02122017','02132017','02142017','02152017','02162017','02172017','02182017','02192017','02202017','02212017',
'02222017','02232017','02242017','02252017','02262017','02272017','02282017','03012017','03022017','03032017',
'03042017','03052017','03062017')

#recordedDateTup = ('01262017','01142017')
for i in range(0,len(recordedDateTup)):
    recordedDate = recordedDateTup[i]



    featureMeasured = 'breathingrate'
    pd.to_datetime(str(recordedDate), format = '%m%d%Y')
    fileName = patientid+ '_'+featureMeasured+ '_'+ recordedDate+ '.csv'
    filePath = ('C:/Users/sudat/Downloads/Mayo Clinic project/Data/'+patientid+ '/' + fileName)
    df = pd.read_csv(filePath)
    df[' Measurement Date'] = pd.to_datetime(df[' Measurement Date'], infer_datetime_format=True)
    df_resample_breathing = df[[' Measurement Date',' Respiration Rate']]
    df_resample_breathing.set_index(pd.to_datetime(df_resample_breathing[' Measurement Date']), inplace=True)
    df_resample_breathing = df_resample_breathing.resample('5min', how=np.mean)

    df_resample_breathing = df_resample_breathing[[' Respiration Rate']]

    featureMeasured = 'weight'
    fileName = patientid+ '_'+featureMeasured+ '_'+ recordedDate+ '.csv'
    filePath = ('C:/Users/sudat/Downloads/Mayo Clinic project/Data/'+patientid+ '/' + fileName)
    df = pd.read_csv(filePath)
    df['Observation Date'] = pd.to_datetime(df['Observation Date'], infer_datetime_format=True)
    df_resample_weight = df[['Observation Date',' Weight (kg)']]
    df_resample_weight.set_index(pd.to_datetime(df_resample_weight['Observation Date']), inplace=True)
    df_resample_weight = df_resample_weight.resample('5min', how=np.mean)
    df_resample_weight = df_resample_weight[[' Weight (kg)']]

    featureMeasured = 'bloodpressure'
    fileName = patientid+ '_'+featureMeasured+ '_'+ recordedDate+ '.csv'
    filePath = ('C:/Users/sudat/Downloads/Mayo Clinic project/Data/'+patientid+ '/' + fileName)
    df = pd.read_csv(filePath)
    df['Observation Date'] = pd.to_datetime(df['Observation Date'], infer_datetime_format=True)
    df_resample_bp = df[['Observation Date',' Systolic', ' Diastolic']]
    df_resample_bp.set_index(pd.to_datetime(df_resample_bp['Observation Date']), inplace=True)
    df_resample_bp = df_resample_bp.resample('5min', how=np.mean)
#    df_resample_bp['Index'] = pd.date_range(pd.to_datetime(str(recordedDate), format = '%m%d%Y'), periods=288, freq='5T')
#    df_resample_bp=df_resample_bp.set_index('Index')
    df_resample_bp = df_resample_bp[[' Systolic', ' Diastolic']]

    featureMeasured = 'activitylevel'
    pd.to_datetime(str(recordedDate), format = '%m%d%Y')
    fileName = patientid+ '_'+featureMeasured+ '_'+ recordedDate+ '.csv'
    filePath = ('C:/Users/sudat/Downloads/Mayo Clinic project/Data/'+patientid+ '/' + fileName)
    df = pd.read_csv(filePath)
    df[' Measurement Date'] = pd.to_datetime(df[' Measurement Date'], infer_datetime_format=True)
    df_resample_actbp = df[[' Measurement Date',' Activity',' Body Position']]
    df_resample_actbp.set_index(pd.to_datetime(df_resample_actbp[' Measurement Date']), inplace=True)
    df_resample_actbp = df_resample_actbp.resample('1min', how=np.mean)
    index = pd.date_range(pd.to_datetime(str(recordedDate), format = '%m%d%Y'), periods=1440, freq='1T')
    df_resample_actbp =df_resample_actbp.reindex(index)
    df_resample_actbp = df_resample_actbp[[' Activity',' Body Position']]
    df_resample_actbp = pd.DataFrame(np.reshape(df_resample_actbp.values,(288,10)),
                        columns=['AC1','BP1','AC2','BP2','AC3','BP3','AC4','BP4','AC5','BP5'])
    df_resample_actbp['Index'] = pd.date_range(pd.to_datetime(str(recordedDate), format = '%m%d%Y'), periods=288, freq='5T')
    df_resample_actbp=df_resample_actbp.set_index('Index')

    df_resample =df_resample_breathing.join(df_resample_weight, how='outer').join(df_resample_bp, how ='outer').join(df_resample_actbp, how ='outer')
    index = pd.date_range(pd.to_datetime(str(recordedDate), format = '%m%d%Y'), periods=288, freq='5T')
    df_resample =df_resample.reindex(index)
    df_resample = df_resample[[' Respiration Rate',' Weight (kg)',' Systolic',' Diastolic','AC1','AC2','AC3','AC4','AC5','BP1','BP2','BP3','BP4','BP5']]

    try:

        df_resample[[' Respiration Rate',' Weight (kg)',' Systolic',' Diastolic']] = df_resample[[' Respiration Rate',' Weight (kg)',' Systolic',' Diastolic']].ffill().bfill()
        df_resample[np.isnan(df_resample[['AC1','AC2','AC3','AC4','AC5']])] = 0
        df_resample[np.isnan(df_resample[['BP1','BP2','BP3','BP4','BP5']])] = 5
    except:
        pass

    opPath=('C:/Users/sudat/Downloads/Mayo Clinic project/Data/'+'preProcessed_'+recordedDate+'.csv')
    df_resample.to_csv(opPath, sep=',', encoding='utf-8')
    #df_resample.to_csv('C:/Users/sudat/Downloads/Mayo Clinic project/Data/example.csv')

