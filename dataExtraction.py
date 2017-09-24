# -*- coding: utf-8 -*-
"""
Created on Wed Jul  5 17:59:25 2017

@author: Sudath
"""

import pandas as pd
import numpy as np
import datetime as dt
import os
import glob

patientidtup = ('102243', '97810', '82920', '78850', '70921', '9406', '8829', '8246', '7984', '7983')#'102243',
patientidtup = ( '7984', '8246')#'102243',

for p in range(0,len(patientidtup)):
    patientid = patientidtup[p]
    #patientid = '102243'#102243#97810#82920#78850#70921#9406#8829#8246#7984#7983

    path = 'C:/Users/sudat/Downloads/Mayo Clinic project/Data/'+patientid
    extension = 'csv'
    os.chdir(path)
    result = [i for i in glob.glob('*breathingrate*.{}'.format(extension))]
    #print(result)
    recordedDateTup= tuple([x[-12:-4] for x in result])
    #recordedDateTup = ('01142017','01272017')
    for j in range(0,len(recordedDateTup)):
        recordedDate = recordedDateTup[j]
        print(patientid, recordedDate)
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
        df_resample_actbp2 = df_resample_actbp[[' Measurement Date',' Body Position']]
        df_resample_actbp = df_resample_actbp.resample('1min', how=np.mean)
        index = pd.date_range(pd.to_datetime(str(recordedDate), format = '%m%d%Y'), periods=1440, freq='1T')
        df_resample_actbp =df_resample_actbp.reindex(index)


        df_resample_actbp = df_resample_actbp[[' Activity']]
        df_resample_actbp = pd.DataFrame(np.reshape(df_resample_actbp.values,(288,5)),
                            columns=['AC1','AC2','AC3','AC4','AC5'])
        df_resample_actbp['Index'] = pd.date_range(pd.to_datetime(str(recordedDate), format = '%m%d%Y'), periods=288, freq='5T')
        df_resample_actbp=df_resample_actbp.set_index('Index')

        df_resample_actbp2 = df_resample_actbp2.pivot_table(index=' Measurement Date', columns=' Body Position', aggfunc=len, fill_value=0)
        try:
            df_resample_actbp2 = df_resample_actbp2.resample('5min', how=np.sum)
        except:
            pass
        reqBodyPos = (5, 129, 130, 131)
        df_resample_actbp2 = df_resample_actbp2.reindex(columns=list(reqBodyPos), fill_value=0)
        df_resample_actbp2 = df_resample_actbp2.add_prefix('Body_Pos_')

        df_resample =df_resample_breathing.join(df_resample_weight, how='outer').join(df_resample_bp, how ='outer').join(df_resample_actbp, how ='outer')
        index = pd.date_range(pd.to_datetime(str(recordedDate), format = '%m%d%Y'), periods=288, freq='5T')
        df_resample =df_resample.reindex(index)
        df_resample = df_resample[[' Respiration Rate',' Weight (kg)',' Systolic',' Diastolic','AC1','AC2','AC3','AC4','AC5']]

        try:
            df_resample[[' Respiration Rate',' Weight (kg)',' Systolic',' Diastolic']] = df_resample[[' Respiration Rate',' Weight (kg)',' Systolic',' Diastolic']].ffill().bfill()
            df_resample[np.isnan(df_resample[['AC1','AC2','AC3','AC4','AC5']])] = 0
            df_resample = df_resample.fillna(0)
        except:
            pass

        try:

            df_resample =df_resample.join(df_resample_actbp2, how='outer')

        except:
            pass

        featureMeasured = 'eventmarkers'
        fileName = patientid+ '_'+featureMeasured+ '_'+ recordedDate+ '.csv'
        filePath = ('C:/Users/sudat/Downloads/Mayo Clinic project/Data/'+patientid+ '/' + fileName)
        df = pd.read_csv(filePath)
        df[' Marker Date'] = pd.to_datetime(df[' Marker Date'], infer_datetime_format=True)
        df_resample_EV = df[['Type Id',' Marker Date']]

        df_resample_EV = df_resample_EV.pivot_table(index=' Marker Date', columns='Type Id', aggfunc=len, fill_value=0)

        reqTypeId = (26,29,36,39,56,57,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,79,80,81,82,83,84,85,86,87,88,92,93,128,129)

        cols = [c for c in df_resample_EV.columns if c in reqTypeId]
        df_resample_EV = df_resample_EV[cols]

        df_resample_EV = df_resample_EV.reindex(columns=list(reqTypeId), fill_value=0)
        try:
            df_resample_EV = df_resample_EV.resample('5min', how=np.sum)
        except:
            pass
        df_resample_EV = df_resample_EV.add_prefix('Type_Id_')


        df_resample_EV = df_resample_EV.fillna(0)
    #    df_resample_EV['Index'] = pd.date_range(pd.to_datetime(str(recordedDate), format = '%m%d%Y'), periods=288, freq='5T')
    #    df_resample_EV=df_resample_bp.set_index('Index')
        try:
            df_resample = df_resample.join(df_resample_EV, how='outer')
        except:
            pass

        df_resample = df_resample.fillna(0)
        opPath=('C:/Users/sudat/Downloads/Mayo Clinic project/Data/'+patientid+'/preProcessed_'+recordedDate+'_'+patientid+'.csv')
        df_resample.to_csv(opPath, sep=',', encoding='utf-8')
        #df_resample.to_csv('C:/Users/sudat/Downloads/Mayo Clinic project/Data/example.csv')
