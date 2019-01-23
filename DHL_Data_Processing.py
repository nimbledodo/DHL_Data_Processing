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
import shutil
import ftplib

# 데이터 처리 기본 세팅 설정
def setConfig():
    confs = {}
    cwd = os.getcwd()   # 현재 디렉토리
    # 인자가 없을 경우 기본값
    CONF_INIT = "init.conf" # 변경을 원하는 default variable을 저장하는 파일명
    CONF_DIR = cwd # 기본값은 실행경로과 동일한 경로
    # 인자가 있을 경우 argv[1]은 파일명, argv[2]는 폴더 경로
    try:
        if len(sys.argv)>=2:
            CONF_INIT = sys.argv[1] #CONF_INIT 파일명
        if len(sys.argv)==3:
            CONF_DIR = sys.argv[2] #CONF_INIT 파일 저장 경로
    except:
        pass

    # CONF_DIR\CONF_INIT 파일에 해당 데이터가 없을 경우 default value로 대신함
    confs["HDR_DIR"] = cwd  # 헤더파일이 저장된 폴더
    confs["HDR_FILE"] = "dailyHeader.csv"  # daily 헤더파일명
    confs["MONTHLY_FILE"] = "monthlyHeader.csv" #montly 헤더파일명
    confs["TMPL_FILE"] = "templateDaily.csv"    #daily 저장파일의 헤더가 기록된 파일명
    confs["RAW_DIR"] = cwd  # Raw 데이터가 저장된 폴더
    confs["SAVE_DIR"] = cwd  # 데이터가 저장될 폴더
    confs["SAVE_SKIP"] = 1  # daily 데이터를 skip할 간격
    confs["SAVE_PREFIX"] = ""  # 데이터는 prefix+날짜명.csv 로 저장됨
    confs["MONTHLY_SKIP"] = 6   # montly 데이터를 skip할 간격
    confs["MONTHLY_PREFIX"] = "m_" # montly 데이터는 prefix+연월.csv로 저장됨
    confs["FTP_SERVER"] = ""  # FTP 서버 주소
    confs["FTP_PORT"] = 21  # FTP 포트
    confs["FTP_ID"] = "pi"  # FTP login id
    confs["FTP_PW"] = "raspberry" # FTP login passwd
    confs["FTP_DIR"] = ""   # FTP에서 데이터가 저장된 폴더

    # CONF_DIR가 공란이면 현재 디렉토리에서 CONF_INIT을 찾음

    try:
        os.chdir(parseDir(CONF_DIR))
        lines = open(CONF_INIT, 'r', encoding='utf-8').readlines()
        print("Reading config file: "+getFullPath(CONF_DIR, CONF_INIT))
    except:
        # 폴더나 파일이 존재하지 않으면 기본값 그대로 사용
        print("Config file does not exist")
        pass
    for line in lines:
        if line[0] != "#":
            k, v = line.strip('\n').split(',')
            try:
                confs[k] = int(v)  # 숫자의 경우 int로 변환
            except:
                confs[k] = v  # 문자면 그냥 둠

    return confs

# rawCsv 및 dailyHeader 파일을 받아 최종 csv 파일을 만들어 저장함
def makeFinalCsv(confs, rawName):
    #
    #   rawCsv 파일 중 dailyHeader에 명시된 데이터만 골라 scale 후 저장
    #   Parameters
    #       confs: 설정이 담긴 dictionary
    #       rawName: raw csv 파일 이름
    #   Local variables
    #       header: Header 정보를 담은 DataFrame, SMPC, Name, 변수설명, Scale, Unit, Col로 구성
    #           SMPC : S, M1, M2,.. P1, P2,.., C1, C2..
    #           Name: 변수이름, 변수설명: 설명, Unit: 값 단위
    #           Scale: -면 음수변환을 해야함, +면 음수변환하지 않음, 절대값은 scale, 즉 최종값 = 기록값*abs(scale)
    #           Col: raw 파일 중 해당 정보가 기록된 컬럼번호
    #       fileName: raw csv 파일 이름
    #       folder: 파일을 저장할 폴더
    #
    success = True

    # 헤더파일 얻기
    try:
        os.chdir(parseDir(confs["HDR_DIR"]))
        header = pd.read_csv(confs["HDR_FILE"], dtype='unicode', index_col = False)
    except Exception as exh:
        print("Cannot open the header file. ", exh)
        success = False
        return

    # Raw 데이터 얻기
    try:
        rawfile = getFullPath(confs["RAW_DIR"], rawName)
        raw = pd.read_csv(rawfile, dtype='unicode', index_col=False)
    except Exception as exr:
        print("Cannot open the raw file. ", exr)
        success = False
        return

    newDf = pd.DataFrame()    #신규 DataFrame 생성
    newDf['Time'] = raw['Time'] #시간 행은 그대로 복사

    try:
        for i in range(len(header)):
            col = int(header.iloc[i].Col)    # 값이 들어있는 컬럼번호
            scale = float(header.iloc[i].Scale) # 컬럼번호에 해당하는 scale
            name = header.iloc[i].SMPC+'.'+'.'.join(header.iloc[i].Name.split('.')[1:])  # 컬럼번호에 해당하는 Name
            rawValues = list(map(int, raw.iloc[:, col].tolist()))   # 값에 해당하는 rawValue (int로 변경)
            # scale 된 값을 newDf에 추가
            newDf[name] = scaleValues(rawValues, scale)
    except Exception as ex:
        print("Error in processing raw file: ", rawName, ex)
        success = False
        pass

    try:
        # 헤더만들기
        src = getFullPath(confs["HDR_DIR"], confs["TMPL_FILE"])
        dst = getFullPath(confs["SAVE_DIR"], confs["SAVE_PREFIX"]+rawName)
        shutil.copyfile(src,dst)
        # 파일 쓰기
        newDf[::confs["SAVE_SKIP"]].to_csv(dst, mode='a', index=False, header=None)  #일정시간 간격으로 추출한 것 저장
        print("Successfully generated daily file: ", confs["SAVE_PREFIX"]+rawName)
        makeMonthlyData(confs, newDf[::confs["MONTHLY_SKIP"]], getMonthlyFileName(confs, rawName))
    except Exception as ex1:
        print("Cannot save daily file: ", rawName,  ex)
        success = False
        pass

    # 예외가 발생하지 않고 무사히 진행되었으면 raw data는 지움
    if success:
        os.remove(rawfile)
    return

# raw file명을 입력받아 monthly 파일 이름을 돌려줌
def getMonthlyFileName(confs, rawName):
    #
    #   rawName이 YYYY-MM-DD.csv 파일일 때 이를 "MONTHLY_PREFIX" + YYYY-MM.csv로 바꾸어 돌려줌
    #   ex) rawName: 2018-10-10.csv
    #       MONTHLY_PREFIX: monthly
    #       monthly2018-10.csv를 돌려줌
    #
    return confs["MONTHLY_PREFIX"]+"-".join(rawName.split("-")[0:2])+".csv"

# Dataframe을 입력받아 월간 정보에 저장할 정보만 추림
def makeMonthlyData(confs, df, filename):
    #
    #   rawCsv 파일 중 dailyHeader에 명시된 데이터만 골라 scale 후 저장
    #   Parameters
    #       confs: 설정이 담긴 dictionary
    #       df: daily data가 담긴 dataframe
    #       filename: 저장할 파일 이름
    #

    newDf = pd.DataFrame()    #신규 DataFrame 생성

    # 헤더파일 얻기
    try:
        os.chdir(parseDir(confs["HDR_DIR"]))
        header = pd.read_csv(confs["MONTHLY_FILE"], dtype='unicode', index_col=False)
    except Exception as exh:
        print("Cannot open the monthly header file: ", exh)
        return

    # 데이터에서 필요한 부분만 추출
    try:
        for i in range(len(header)):
            col = int(header.iloc[i].Col)  # 값이 들어있는 컬럼번호
            name = header.iloc[i].Name
            valueList = df.iloc[:, col].tolist()
            newDf[name] = valueList
    except Exception as ex:
        print("Error in making monthly data: ", ex)
        pass

    # 파일 저장
    try:
        fullFileName = getFullPath(confs["SAVE_DIR"], filename)
        # 파일이 존재하면 데이터만 씀
        if os.path.isfile(fullFileName):
            newDf.to_csv(fullFileName, mode="a", index=False, header=None)
        else:
            #파일이 존재하지 않으면 헤더도 함께 씀
            fileHdr = header.Name.tolist()  #monthly 파일의 헤더명
            newDf.to_csv(fullFileName, mode='w', index=False, header=fileHdr)
        print("Successfully updated monthly data: ", filename)
    except Exception as ex1:
        print("Error in writing monthly data. ", ex1)
        pass

    return

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
        print("Error in scaleValues: ", ex)
        pass
    
    return newValues

# Windows 경로 표현방식을 Python에서 읽을 수 있는 방식으로 바꿔줌
def parseDir(oldDir):
    return oldDir.replace("\\", "/")

# 폴더명과 파일명을 받아 full file 경로를 돌려줌
def getFullPath(dir,file):
    return parseDir(dir)+"/"+file

# ftp 접속하여 파일 저장
def getRemoteFile(confs, filename):

    ftp = ftplib.FTP()
    ftp.connect(confs["FTP_SERVER"], confs["FTP_PORT"])
    ftp.login(confs["FTP_ID"], confs["FTP_PW"])
    ftp.cwd(parseDir(confs["FTP_DIR"]))
    fd = open(parseDir(confs["RAW_DIR"]) + "/" + filename, 'wb')
    ftp.retrbinary("RETR " + filename, fd.write)
    fd.close()
    return

# 날짜 string과 format을 받아 datetime 형식으로 돌려줌
def getDate(dateStr,format):
    #
    # Parameters
    #

    pass

if __name__ == "__main__":

    # 데이터 세팅 저장
    confs = setConfig()

    # 날짜에 대해 반복
    start = "190115"
    end = "190122"


    # ftp 접속하여 csv 파일 받아옴
    getRemoteFile(confs, filename)

    # csv 파일을 처리하여 매일 데이터 저장으로 만듬
    makeFinalCsv(confs, filename)

    # 구글드라이브의 요금분석 데이터 업데이트

    # csv 파일을 처리하여 스택 및 시스템 효율 분석

    # 구글드라이브의 스택 효율 시트 업데이트

    pass
