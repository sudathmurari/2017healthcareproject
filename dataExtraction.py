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
import zipfile
from time import sleep
import warnings

# the working directory must have folders named with patient's ID
workingdir = 'C:/Users/sudat/Downloads/Mayo Clinic project/Data/'
warnings.filterwarnings('ignore')

# Print iterations progress
def printProgressBar (iteration, total, prefix = '\tProgress', suffix = 'Complete', decimals = 2, length = 30, fill = '█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    # Print New Line on Complete
    if iteration == total:
        print()

def extractBreathingRate (recordedDate, patientid, workingdir):
    featureMeasured = 'breathingrate'
    pd.to_datetime(str(recordedDate), format = '%m%d%Y')
    fileName = patientid+ '_'+featureMeasured+ '_'+ recordedDate+ '.csv'
    filePath = (workingdir+patientid+ '/' + fileName)
    df = pd.read_csv(filePath)
    df[' Measurement Date'] = pd.to_datetime(df[' Measurement Date'], infer_datetime_format=True)
    df_resample_breathing = df[[' Measurement Date',' Respiration Rate']]
    df_resample_breathing.set_index(pd.to_datetime(df_resample_breathing[' Measurement Date']), inplace=True)
    df_resample_breathing = df_resample_breathing.resample('5min', how=np.mean)
    df_resample_breathing = df_resample_breathing[[' Respiration Rate']]
    return df_resample_breathing

def extractWeight (recordedDate, patientid, workingdir):
    featureMeasured = 'weight'
    fileName = patientid+ '_'+featureMeasured+ '_'+ recordedDate+ '.csv'
    filePath = (workingdir+patientid+ '/' + fileName)
    df = pd.read_csv(filePath)
    df['Observation Date'] = pd.to_datetime(df['Observation Date'], infer_datetime_format=True)
    df_resample_weight = df[['Observation Date',' Weight (kg)']]
    df_resample_weight.set_index(pd.to_datetime(df_resample_weight['Observation Date']), inplace=True)
    df_resample_weight = df_resample_weight.resample('5min', how=np.mean)
    df_resample_weight = df_resample_weight[[' Weight (kg)']]
    return df_resample_weight

def extractBloodPressure (recordedDate, patientid, workingdir):
    featureMeasured = 'bloodpressure'
    fileName = patientid+ '_'+featureMeasured+ '_'+ recordedDate+ '.csv'
    filePath = (workingdir+patientid+ '/' + fileName)
    df = pd.read_csv(filePath)
    df['Observation Date'] = pd.to_datetime(df['Observation Date'], infer_datetime_format=True)
    df_resample_bp = df[['Observation Date',' Systolic', ' Diastolic']]
    df_resample_bp.set_index(pd.to_datetime(df_resample_bp['Observation Date']), inplace=True)
    df_resample_bp = df_resample_bp.resample('5min', how=np.mean)
    df_resample_bp = df_resample_bp[[' Systolic', ' Diastolic']]
    return df_resample_bp

#def extractActBodyPos (recordedDate, patientid, workingdir):


def extractEventMarkers (recordedDate, patientid, workingdir):
    featureMeasured = 'eventmarkers'
    fileName = patientid+ '_'+featureMeasured+ '_'+ recordedDate+ '.csv'
    filePath = (workingdir+patientid+ '/' + fileName)
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
    #df_resample_EV = df_resample_EV.add_prefix('Type_Id_')
    df_resample_EV.columns = ['Bradycardia', 'Tachycardia (extreme) while inactive', 'Bradypnea', 'Tachypnea while inactive', 'R-R pause', 'R-R onset', 'SinBRADY', 'SinTACHY', 'NSR+IVCD', 'SinBrady+IVCD', 'SinTachy+IVCD', 'PJC', 'JuncTACHY', '1degr.AVBlock+NSR', '1degr.AVBlock+SinTACHY', '1degr.AVBlock+SinBRADY', 'Mobitz I', 'Mobitz II', 'AVblock', 'PAC', 'SVTA', 'AFib slow', 'AFib normal', 'AFib rapid', 'PVC', 'VCoup', 'VTrip', 'VBig', 'VTrig', 'IVR', 'VT', 'Slow VT', 'VF', 'Unclassified rhythm', 'SVC', 'NSR', 'Minimum Heart Rate', 'Maximum Heart Rate']
    return df_resample_EV

# list all the patient id, the folder names must be patient id
#patientidtup = ('102243', '97810', '82920', '78850', '70921', '9406', '8829', '8246', '7984', '7983')#'102243',
patientidtup = ('97603','144614','121191','118447','79247','9234','9229','8026','8009') #


# create a place holder zip archive for output files
archveName = workingdir + 'DataExtract_v99.zip'
zf = zipfile.ZipFile(archveName, 'w', zipfile.ZIP_DEFLATED)

# iterate thru each patient
for p in range(0,len(patientidtup)):
    patientid = patientidtup[p]
    #patientid = '102243'#102243#97810#82920#78850#70921#9406#8829#8246#7984#7983
    # path where the patient data is stored, each folder contains 1 patient data
    path = workingdir+patientid
    extension = 'csv'

    # read the files to gather all the unique dates in the patient folder
    os.chdir(path)
    result = [i for i in glob.glob('*breathingrate*.{}'.format(extension))]

    recordedDateTup= tuple([x[-12:-4] for x in result])
    #recordedDateTup = ('03122017','03132017')

    # Initiate a dataframe
    df_total = pd.DataFrame()

    # Print patient progress
    print("Extracting data for patient %s (%d of %d)" %(patientid,p+1,len(patientidtup)))


    # iterate thru each date foa given patient
    for j in range(0,len(recordedDateTup)):
            try:
                recordedDate = recordedDateTup[j]

                # extract breathing rate
                df_resample_breathing = extractBreathingRate(recordedDate, patientid, workingdir)

                # extract weight
                df_resample_weight = extractWeight(recordedDate, patientid, workingdir)

                # extract bloodpressure
                df_resample_bp = extractBloodPressure (recordedDate, patientid, workingdir)

                # extract activity and body position
                featureMeasured = 'activitylevel'
                pd.to_datetime(str(recordedDate), format = '%m%d%Y')
                fileName = patientid+ '_'+featureMeasured+ '_'+ recordedDate+ '.csv'
                filePath = (workingdir+patientid+ '/' + fileName)
                df = pd.read_csv(filePath)
                df[' Measurement Date'] = pd.to_datetime(df[' Measurement Date'], infer_datetime_format=True)
                df_resample_actbp = df[[' Measurement Date',' Activity',' Body Position']]
                df_resample_actbp.set_index(pd.to_datetime(df_resample_actbp[' Measurement Date']), inplace=True)
                df_resample_actbp2 = df_resample_actbp[[' Measurement Date',' Body Position']]
                df_resample_actbp = df_resample_actbp.resample('1min', how=np.max)
                index = pd.date_range(pd.to_datetime(str(recordedDate), format = '%m%d%Y'), periods=1440, freq='1T')
                df_resample_actbp =df_resample_actbp.reindex(index)

                df_resample_actbp = df_resample_actbp[[' Activity',' Measurement Date']]
                bins = [0,10,20,30,40,50,60,70,80,90,100]
                group_names = ['Act0to10', 'Act11to20', 'Act21to30', 'Act31to40','Act41to50', 'Act51to60', 'Act61to70', 'Act71to80', 'Act81to90', 'Act91to100']
                #categories = pd.cut(df_resample_actbp[' Activity'], bins, labels=group_names)
                df_resample_actbp['categories'] = pd.cut(df_resample_actbp[' Activity'], bins, labels=group_names)

                df_resample_actbp = df_resample_actbp.pivot_table(index=' Measurement Date', columns='categories', aggfunc=len, fill_value=0)
                df_resample_actbp.columns=group_names

                index0 = pd.date_range(pd.to_datetime(str(recordedDate), format = '%m%d%Y'), periods=1440, freq='1T')
                df_resample_temp = pd.DataFrame(index= index0)

                df_resample_actbp =df_resample_actbp.join(df_resample_temp, how='outer')
                df_resample_actbp = df_resample_actbp.resample('1min', how=np.max)


                df_resample_actbp2 = df_resample_actbp2.pivot_table(index=' Measurement Date', columns=' Body Position', aggfunc=len, fill_value=0)
                try:
                    df_resample_actbp2 = df_resample_actbp2.resample('5min', how=np.sum)
                except:
                    pass
                reqBodyPos = (5, 129, 130, 131)
                df_resample_actbp2 = df_resample_actbp2.reindex(columns=list(reqBodyPos), fill_value=0)
                df_resample_actbp2.columns=['BodyPosUnknown','Standing','Leaning','Lying']

                # join all data to form a single datadrame
                df_resample =df_resample_breathing.join(df_resample_weight, how='outer').join(df_resample_bp, how ='outer').join(df_resample_actbp, how ='outer')
                index = pd.date_range(pd.to_datetime(str(recordedDate), format = '%m%d%Y'), periods=288, freq='5T')
                df_resample =df_resample.reindex(index)
                df_resample = df_resample[[' Respiration Rate',' Weight (kg)',' Systolic',' Diastolic','Act0to10', 'Act11to20', 'Act21to30', 'Act31to40','Act41to50', 'Act51to60', 'Act61to70', 'Act71to80', 'Act81to90', 'Act91to100']]

                try:
                    #df_resample[[' Respiration Rate',' Weight (kg)',' Systolic',' Diastolic']] = df_resample[[' Respiration Rate',' Weight (kg)',' Systolic',' Diastolic']].ffill().bfill()
                    df_resample[np.isnan(df_resample[['Act0to10', 'Act11to20', 'Act21to30', 'Act31to40','Act41to50', 'Act51to60', 'Act61to70', 'Act71to80', 'Act81to90', 'Act91to100']])] = 0
                    #df_resample = df_resample.fillna(0)
                except:
                    pass

                # Join bodyposition data to rest of the data
                try:

                    df_resample =df_resample.join(df_resample_actbp2, how='outer')

                except:
                    pass


                # extract event markers
                df_resample_EV = extractEventMarkers (recordedDate, patientid, workingdir)

                # Join event markers to rest of the data
                try:
                    df_resample = df_resample.join(df_resample_EV, how='outer')
                except:
                    pass


                df_total = pd.concat([df_total,df_resample])


                # print date progress
                printProgressBar(j + 1, len(recordedDateTup))

            except:
                pass

    # forward fill and backward fill cells when zero for few columns
    df_total[[' Respiration Rate',' Weight (kg)',' Systolic',' Diastolic']] = df_total[[' Respiration Rate',' Weight (kg)',' Systolic',' Diastolic']].ffill().bfill()
    df_total = df_total.fillna(0)

    # remove rows where columns of interest are zeros
    cols_of_interest= ['BodyPosUnknown','Standing','Leaning','Lying']
    df_total = df_total[(df_total[cols_of_interest] != 0).any(axis=1)]

    df_total["Date"] = df_total.index.date

    # write data into csv day-wise
    df_by_date = df_total.groupby("Date")
    for (date, date_df) in df_by_date:
        filename = patientid+'/'+patientid+'_'+str(date)+'.csv'
        #print(filename)
        zf.writestr(filename, date_df.to_csv())


    #create a csv for the patient
    #opFileName= patientid+'.csv'
    #zf.writestr(opFileName, df_total.to_csv())

#close zip file to complete writing to disk
zf.close()

