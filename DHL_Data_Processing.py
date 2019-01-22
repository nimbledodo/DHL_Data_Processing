#!/usr/bin/python
'''
***********************************************************************************
    File Name   :   DHL_Data_Processing.py
    Programmer  :   J. Huh
    Purpose     :   동현물류의 저장된 raw 데이터를 읽어와서 필요한 정보만 저장하여 csv로 저장, 매일 데이터 처리, 스택 효율 처리
    How to use  :
    Arguments:
        argv[1]:
    Input files:

    Limitations:
        -
************************************************************************************
 '''

import os
import pandas as pd


# rawCsv 및 dailyHeader 파일을 받아 최종 csv 파일을 만들어 저장함
def makeFinalCsv(header, raw):
    #
    #   rawCsv 파일 중 dailyHeader에 명시된 데이터만 골라 scale 후 저장
    #   Parameters
    #       header: Header 정보를 담은 DataFrame, SMPC, Name, 변수설명, Scale, Unit, Column로 구성
    #           SMPC : S, M1, M2,.. P1, P2,.., C1, C2..
    #           Name: 변수이름, 변수설명: 설명, Unit: 값 단위
    #           Scale: -면 음수변환을 해야함, +면 음수변환하지 않음, 절대값은 scale, 즉 최종값 = 기록값*abs(scale)
    #           Column: raw 파일 중 해당 정보가 기록된 컬럼번호
    #       raw: raw csv 파일의 내용을 담은 DataFrame
    #
    newDf = pd.DataFrame()    #신규 DataFrame 생성
    newDf['Time'] = raw['Time'] #시간 행은 그대로 복사

    try:
        for i in range(len(header)):
            col = int(header.iloc[i].Column)    # 값이 들어있는 컬럼번호
            scale = float(header.iloc[i].Scale) # 컬럼번호에 해당하는 scale
            name = header.iloc[i].SMPC+'.'+'.'.join(header.iloc[i].Name.split('.')[1:])  # 컬럼번호에 해당하는 Name
            rawValues = list(map(int, raw.iloc[:, col].tolist()))   # 값에 해당하는 rawValue (int로 변경)
            # scale 된 값을 newDf에 추가
            newDf[name] = scaleValues(rawValues, scale)
    except Exception as ex:
        print("Error in makeFinalCsv", ex)
        pass

    return newDf

# list를 입력받아 scale해서 내보냄
def scaleValues(rawValues, scale):
    #
    # parameters
    #   value: scale 전 값 (list)
    #   scale: scale 값
    #       양수면 그냥 곱해서 return
    #       음수면 음수처리 후 곱해서 return
    # return
    #   scale 처리한 값 (list)

    NEG_CONST = 65537  #음수 양수 변환값
    MAX_DECIMAL = 5  # 최대 표현할 소수점

    newValues = rawValues

    try:
        for i in range(len(rawValues)):
            old = rawValues[i]
            if scale >= 0:
                new = old*scale
            if scale < 0: #음수처리를 해야한다는 표시로 scale을 -로 함
                if old > 32768:   #음수인 경우 음수처리 먼저 하고 scale
                    new = (old-NEG_CONST)*abs(scale)
                else:
                    new = old*abs(scale) #양수인 경우는 그냥 scale
            if abs(scale == 1):   # scale이 1 혹은 -1이면 정수이므로 그냥 출력
                new = int(new)
            else:   # 정수가 아니면 자리수 제한
                new = round(new, MAX_DECIMAL)
            newValues[i] = new

    except Exception as ex:
        print("Error in scaleValues", ex)
        pass
    
    return newValues

# Windows 경로 표현방식을 Python에서 읽을 수 있는 방식으로 바꿔줌
def parseDir(oldDir):
    return oldDir.replace('\\', '/')



if __name__ == "__main__":
    # 아래 내용을 주어진 날짜에 대해 반복

    # ftp 접속하여 csv 파일 받아옴

    # csv 파일을 처리하여 매일 데이터 저장으로 만듬
    confFolder = "I:\내 드라이브\Project\[2018] 동현물류\운행기록\conf"
    os.chdir(parseDir(confFolder))
    headerName = "dailyHeader.csv"

    dfHeader = pd.read_csv(headerName, dtype='unicode', index_col=False)


    dataFolder = "C:/Users/Jeehyang/Desktop"
    os.chdir(parseDir(dataFolder))
    rawName = "2019-01-20.csv"
    rawCsv = pd.read_csv(rawName, dtype='unicode', index_col=False)

    newDf = makeFinalCsv(dfHeader, rawCsv)

    newDf.to_csv("test3.csv", mode='w',index=False)

    # csv 파일을 처리하여 요금분석용 데이터만 뽑음

    # 구글드라이브의 요금분석 데이터 업데이트

    # csv 파일을 처리하여 스택 및 시스템 효율 분석

    # 구글드라이브의 스택 효율 시트 업데이트

    pass
