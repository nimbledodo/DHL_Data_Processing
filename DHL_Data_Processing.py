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

import csv
import datetime
import ftplib
import numpy as np
import os
import pandas as pd
import shutil
import sys


# 데이터 처리 기본 세팅 설정
def setConfig():
    confs = {}
    cwd = os.getcwd()   # 현재 디렉토리
    # 인자가 없을 경우 기본값
    confs["CONF_INIT"] = "init.conf" # 변경을 원하는 default variable을 저장하는 파일명
    confs["CONF_DIR"] = cwd # 기본값은 실행경로과 동일한 경로
    confs["START"] = "190101"    # 데이터처리 시작날짜
    confs["END"] = "190101"  # 데이터처리 종료날짜
    confs["DATE_FMT"] = "yymmdd"  # 날짜표기형식

    # 인자가 있을 경우 argv[1]은 파일명, argv[2]는 폴더 경로
    try:
        if len(sys.argv)>=2:    # 인자가 하나라도 있을 경우
            for i in range(len(sys.argv)-1):
                name = sys.argv[i+1].split("=")[0].upper()  # = 앞에 있는 것이 이름
                confs[name] = sys.argv[i+1].split("=")[1]   # = 뒤에 있는 것이 설정값
    except:
        pass

    # CONF_DIR\CONF_INIT 파일에 해당 데이터가 없을 경우 default value로 대신함
    confs["HDR_DIR"] = cwd  # 헤더파일이 저장된 폴더
    confs["DAILY_HDR"] = "dailyHeader.csv"  # daily 헤더파일명
    confs["MONTHLY_FILE"] = "monthlyHeader.csv" # montly 헤더파일명
    confs["TMPL_DAILY"] = "templateDaily.csv"    # daily 저장파일의 헤더가 기록된 파일명
    confs["RAW_DIR"] = cwd  # Raw 데이터가 저장된 폴더
    confs["RAW_PREFIX"] = "r_" # Raw 데이터를 읽어오고 난 후 파일이름 prefix
    confs["SAVE_DIR"] = cwd  # 데이터가 저장될 폴더
    confs["SAVE_SKIP"] = 1  # daily 데이터를 skip할 간격
    confs["SAVE_PREFIX"] = "d_"  # 데이터는 prefix+날짜명.csv 로 저장됨
    confs["MONTHLY_SKIP"] = 6   # montly 데이터를 skip할 간격
    confs["MONTHLY_PREFIX"] = "m_" # montly 데이터는 prefix+연월.csv로 저장됨
    confs["EFF_HDR"] = 'effHeader.csv' # efficiency 계산용 헤더파일명
    confs["EFF_FILE"] = 'efficiency.csv'    #efficiency 결과가 저장될 파일명
    confs["EFF_SKIP"] = 6   # efficiency 계산 시 skip 간격
    confs["FTP_SERVER"] = ""  # FTP 서버 주소
    confs["FTP_PORT"] = 21  # FTP 포트
    confs["FTP_ID"] = "pi"  # FTP login id
    confs["FTP_PW"] = "raspberry" # FTP login passwd
    confs["FTP_DIR"] = ""   # FTP에서 데이터가 저장된 폴더
    confs["MS_SEP"] = '/'  # 모듈번호 및 스택번호의 구분자
    confs["MODULES"] = ""   # 효율 계산에서 사용할 모듈 번호
    confs["STACKS"] = ""    # 효율 계산에서 사용할 스택 번호

    # CONF_DIR가 공란이면 현재 디렉토리에서 CONF_INIT을 찾음

    try:
        os.chdir(parseDir(confs["CONF_DIR"]))
        lines = open(confs["CONF_INIT"], 'r', encoding='utf-8').readlines()
        print("Reading config file: "+getFullPath(confs["CONF_DIR"], confs["CONF_INIT"]))
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

# rawCsv을 받아 daily csv 저장, monthly csv 업데이트, 시스템 및 스택 효율정보 업데이트
def processData(confs, rawName):
    #
    #   1. rawCsv 파일 중 dailyHeader에 명시된 데이터만 골라 scale 후 저장
    #   2. monthly 파일에 저장할 항목은 별도 저장
    #   3. 스택 및 시스템 효율을 보기위한 정보 처리
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

    # 헤더파일 얻기
    try:
        os.chdir(parseDir(confs["HDR_DIR"]))
        header = pd.read_csv(confs["DAILY_HDR"], dtype='unicode', index_col = False)
    except Exception as exh:
        print("Cannot open the header file. ", exh)
        return False

    # Raw 데이터 얻기
    try:
        rawfile = getFullPath(confs["RAW_DIR"], rawName)
        raw = pd.read_csv(rawfile, dtype='unicode', index_col=False)
    except Exception as exr:
        print("Cannot open the raw file. ", exr)
        return False

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
        return False

    # rawfile prefix가 제거된 파일 이름
    rawName_wo_prefix = rawName[len(confs["RAW_PREFIX"]):]

    # daily 파일 저장하기
    try:
        # 헤더만들기
        dailyfile = confs["SAVE_PREFIX"] + rawName_wo_prefix
        src = getFullPath(confs["HDR_DIR"], confs["TMPL_DAILY"])
        dst = getFullPath(confs["SAVE_DIR"], dailyfile)
        shutil.copyfile(src,dst)
        # 파일 쓰기
        newDf[::confs["SAVE_SKIP"]].to_csv(dst, mode='a', index=False, header=None)  #일정시간 간격으로 추출한 것 저장
        print("Successfully generated daily file: ", dailyfile)
        os.remove(rawfile)
    except Exception as ex1:
        print("Cannot save daily file: ", dailyfile,  ex)
        return False

    # monthly data 저장하기
    success = makeMonthlyData(confs, newDf[::confs["MONTHLY_SKIP"]], getMonthlyFileName(confs, rawName_wo_prefix))
    if not success:
        return False

    # efficiency 계산하기
    dateStr = getDate(rawName, confs["RAW_PREFIX"]+'YYYY-MM-DD.csv').isoformat()
    success = calculateEff(confs, newDf[::confs["EFF_SKIP"]], dateStr)

    return success

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
        return False

    # 데이터에서 필요한 부분만 추출
    try:
        for i in range(len(header)):
            col = int(header.iloc[i].Col)  # 값이 들어있는 컬럼번호
            name = header.iloc[i].Name
            valueList = df.iloc[:, col].tolist()
            newDf[name] = valueList
    except Exception as ex:
        print("Error in making monthly data: ", ex)
        return False

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
        return False

    return True

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

    try:
        ftp = ftplib.FTP()
        ftp.connect(confs["FTP_SERVER"], confs["FTP_PORT"])
        ftp.login(confs["FTP_ID"], confs["FTP_PW"])
        ftp.cwd(parseDir(confs["FTP_DIR"]))
        fd = open(parseDir(confs["RAW_DIR"]) + "/" + confs["RAW_PREFIX"] + filename, 'wb')
        ftp.retrbinary("RETR " + filename, fd.write)
        fd.close()
        print ("Successfully downloaded the raw data: ", filename)
    except Exception as ex:
        print ("Error getting a file from the ftp server: ", filename, ex)
        return False

    return True

# 날짜 string과 format을 받아 datetime 형식으로 돌려줌
def getDate(dateStr, format):
    #
    # Parameters
    #   dateStr: 날짜 문자열
    #   format: 날짜 문자열 형식 (대소문자 구분 안함)
    #           Y: 연도 (YYYY이면 2018, YY 면 18)
    #           M: 월 (항상 leading zero를 포함할 것!)
    #           D: 일 (항상 leading zero를 포함할 것!)
    #
    #       ex) YYMMDD: 190212, MM/DD/YY: 02/23/18, dd-mm-yyyy: 23-01-2018
    #

    format = format.upper()

    #연도찾기
    cYear = format.count('YY')  #year가 발생한 횟수
    if cYear == 1:  # 연도가 두 자리 형식일 경우
        loc = format.find('YY') #year의 위치
        year = 2000 + int(dateStr[loc:loc+2])
    elif cYear == 2:    #연도가 네 자리 형식일 경우
        loc = format.find('YYYY')   #year의 위치
        year = int(dateStr[loc:loc+4])
    else:
        print("Year does not exist")
        return

    #월 찾기
    loc = format.find('MM')    #month의 위치
    if loc != -1:  # 월이 두자리 형식
        month = int(dateStr[loc:loc+2])
    else:
        print("Month does not exist")
        return

    #일 찾기
    loc = format.find('DD')    #day의 위치
    if loc != -1:  # 월이 두자리 형식
        day = int(dateStr[loc:loc+2])
    else:
        print("Day does not exist")
        return

    return datetime.date(year, month, day)

# df를 받아 충방전량, 효율을 계산함
def calculateEff(confs, df, dateStr):
    #
    #   dataframe을 입력받아 각 모듈 및 스택별 충방전양, VE, EE를 계산함
    #   Parameters
    #       confs: 설정이 담긴 dictionary
    #       df: 각종 정보가 담긴 df
    #       dateStr: 날짜 (iso-format)
    #

    POWER_ZERO_CUT = 1  # 0으로 간주할 power의 절대값
    # 사용할 모듈번호 list
    modules = list(map(int, confs['MODULES'].split(confs['MS_SEP'])))
    # 사용할 스택번호 list
    stacks = list(map(int, confs['STACKS'].split(confs['MS_SEP'])))

    newDf = pd.DataFrame()    # 신규 DataFrame 생성

    results = [dateStr]    # 결과 데이터 저장 (처음 데이터는 날짜)

    # 계산에 필요한 필요한 데이터 헤더파일 읽어옴
    try:
        os.chdir(parseDir(confs["HDR_DIR"]))
        header = pd.read_csv(confs["EFF_HDR"], dtype='unicode', index_col=False)
        print("Successfully read efficiency header file")
    except Exception as exh:
        print("Cannot open the efficiency header file: ", exh)
        return False

    # 필요한 데이터만 추출 (시간, 각 스택 전압, 각 모듈별 전류)
    # 단, 필요한 데이터는 모두 header에 기록되어있다고 가정함
    try:
        for i in range(len(header)):
            col = int(header.iloc[i].Col)  # 값이 들어있는 컬럼번호
            name = header.iloc[i].Name
            valueList = df.iloc[:, col].tolist()
            newDf[name] = valueList
    except Exception as ex:
        print("In module calculateEff: Error in reading data: ", ex)
        return False

    # 시간차 계산
    newDf['Time'] = pd.to_datetime(newDf['Time'], format = '%Y-%m-%d %H:%M:%S')
    dt = newDf['Time'].diff().apply(lambda x: x/np.timedelta64(1, 's')/3600).fillna(0)  # hr로 환산한 시간차

    # 에너지 및 효율 계산
    try:
        for m in modules:
            m_str = str(m)
            cur = newDf['M'+m_str+".Cur"]   # 전체전류
            vol = newDf['M'+m_str+".Vol"]   # 전체전압
            results = results + calEnergyAndEff(vol, cur, dt)
            for s in stacks:
                s_str = str(s)
                vol = newDf['M'+m_str+".Vol"+s_str]
                results = results + calEnergyAndEff(vol, cur, dt)
        print("Successfully calculated energies and efficiencies")
    except Exception as ex2:
        print("Error in calculating energies and efficiencies. ", ex2)
        return False

    # 데이터 저장
    try:
        dst = getFullPath(parseDir(confs["SAVE_DIR"]), confs["EFF_FILE"])
        saveEff(dst, modules, stacks, results)
        print("successfully updated efficiency file.")
    except Exception:
        return False

    return True

# 저장할 파일명, 헤더, 결과를 받아 주어진 양식에 맞게 효율 데이터 저장
def saveEff(dst, modules, stacks, results):
    #   주어진 형식에 맞게 파일 저장
    #   Parameters
    #       dst: 저장할 파일의 이름 (directory 포함)
    #       modules: 해당 모듈
    #       stacks: 해당 스택
    #       results: 저장된 데이터:  [날짜, 모듈 정보] 로 저장
    #           모듈정보 = [모듈전체정보, 스택별 정보]
    #           모듈전체정보 = [chE dischE EE VE]
    #           스택별정보 = [chE dischE EE VE]
    #           ex) M1, M2, S1, S2일 경우
    #               [날짜, M1.chE, M1.dischE, M1.EE, M1.VE, M1.S1.chE, M1.S1.dischE, M1.S1.EE, M1.S1.VE, M1.S2.chE, M1.S2.dischE, M1.S2.EE, M1.S2.VE,
    #                      M2.chE, M2.dischE, M2.EE, M2.VE, M2.S1.chE, M2.S1.dischE, M2.S1.EE, M2.S1.VE, M2.S2.chE, M2.S2.dischE, M2.S2.EE, M2.S2.VE]
    #   프린트할 데이터 순서
    #       [날짜, 모듈전체정보, 스택별정보]
    #       모듈전체정보 = [chE dischE EE VE]
    #                   각각 성분은 모듈에 대해 반복
    #       스택별정보 = [chE dischE EE VE]
    #                   각각 성분은 스택에 대해 반복
    #


    oldHeader = getOriginalEffHeader(modules, stacks)
    newHeader = getEffHeader(modules, stacks)

    # dictionary로 만들어줌
    oldData = dict(zip(oldHeader, results))

    # newHeader 순서에 맞게 정리한 후 list화 해줌
    newData = pd.DataFrame(oldData, columns=newHeader, index=["0"]).iloc[0].tolist()

    try:
        with open(dst, 'a', newline='') as csvFile:
            writer = csv.writer(csvFile)
            if os.path.getsize(dst) == 0:
                writer.writerow(newHeader)
            writer.writerow(newData)
    except Exception as ex:
        print("Error in updating efficiency file. ", ex)

    return

# 효율 정보 데이터가 저장된 그대로의 헤더
def getOriginalEffHeader(modules, stacks):
    #
    #   Parameters
    #       modules: 사용할 모듈 번호 (in list)
    #       stacks: 사용할 스택 번호 (in list)
    #   efficiency header의 구조
    #       [날짜, 모듈 정보] 로 저장
    #           모듈정보 = [모듈전체정보, 스택별 정보]
    #           모듈전체정보 = [chE dischE EE VE CE]
    #           스택별정보 = [chE dischE EE VE CE]
    #           ex) M1, M2, S1, S2일 경우
    #               [날짜, M1.chE, M1.dischE, M1.EE, M1.VE, M1. CE, M1.S1.chE, M1.S1.dischE, M1.S1.EE, M1.S1.VE, M1.S1.CE, M1.S2.chE, M1.S2.dischE, M1.S2.EE, M1.S2.VE, M1.S2.CE,
    #                      M2.chE, M2.dischE, M2.EE, M2.VE, M2. CE, M2.S1.chE, M2.S1.dischE, M2.S1.EE, M2.S1.VE, M2.S1.CE, M2.S2.chE, M2.S2.dischE, M2.S2.EE, M2.S2.VE, M2.S2.CE]
    #
    header = ['Date']
    data = ['chE', 'dischE', 'EE', 'VE', 'CE']

    for m in modules:
        m_str = 'M' + str(m) + '.'
        for d in data:
            header = header + [m_str + d]
        for s in stacks:
            s_str = m_str + 'S' + str(s) + '.'
            for d in data:
                header = header + [s_str+d]

    return header

# efficiency file의 헤더를 쓴다
def getEffHeader(modules, stacks):
    #
    #   Parameters
    #       modules: 사용할 모듈 번호 (in list)
    #       stacks: 사용할 스택 번호 (in list)
    #   efficiency header의 구조
    #       [시간, 모듈전체정보, 스택별정보]
    #       모듈전체정보 = [chE dischE EE VE CE]
    #                   각각 성분은 모듈에 대해 반복
    #       스택별정보 = [chE dischE EE VE CE]
    #                   각각 성분은 스택에 대해 반복

    header = ['Date']
    data = ['chE', 'dischE', 'EE', 'VE', 'CE']
    for d in data:
        for m in modules:
            m_str = 'M'+str(m)+'.'
            header = header + [m_str + d]
    for m in modules:
        m_str = 'M' + str(m) + '.'
        for d in data:
            for s in stacks:
                s_str = m_str + 'S'+str(s)+'.'
                header = header + [s_str + d]
    return header

# Voltage, Cur, dt를 받아 charge energy, discharge energy, EE, VE를 계산
def calEnergyAndEff(vol, cur, dt):
    #
    #   Parameters
    #       vol: voltage (dataframe 형식)
    #       cur: current (dataframe 형식)
    #       dt: delta t in hr (dataframe 형식)
    #   Return
    #       [charge energy, discharge energy, energy efficiency, voltage efficiency]
    #

    MIN_POWER = 2000    # in W, 이 이상의 power가 아니면 없는 것으로 간주함
    E_DECIMAL = 1   # 에너지 소수점 수
    EFF_DECIMAL = 3 # 효율 소수점 수
    # 전력계산
    power = (cur * vol).apply(lambda x: (0 if abs(x) < MIN_POWER else x))   # power = cur * vol
    # 충방전 여부 판단
    ifCharge = power.apply(lambda x: (1 if x > 0 else 0))   # 충전이면 1, 아니면 0
    ifDischarge = power.apply(lambda x: (1 if x < 0 else 0))    # 방전이면 1, 아니면 0
    # 충전에너지 (in kWh) (충전 시 power*dt의 합)
    chE = round((power * ifCharge * dt).sum() / 1000.0, E_DECIMAL)
    # 방전에너지 (in kWh) (방전 시 power*dt의 합)
    dischE = -round((power * ifDischarge * dt).sum() / 1000.0, E_DECIMAL)
    # 충전, 방전이 모두 일어난 경우에만 계산
    if chE != 0 and dischE!=0:
        # EE 계산
        EE = round(dischE / chE, EFF_DECIMAL)
        # 평균 충전전압 (충전 시 voltage 합 / 충전 시간 수 합)
        avgVch = (vol * ifCharge).sum() / ifCharge.sum()
        # 평균 방전전압 (방전 시 voltage 합 / 방전 시간 수 합)
        avgVdisch = (vol * ifDischarge).sum() / ifDischarge.sum()
        # VE 계산 (평균 방전전압/평균 충전전압)
        VE = round(avgVdisch / avgVch, EFF_DECIMAL)
        # CE 계산 (EE/VE)
        CE = round(EE / VE, EFF_DECIMAL)
    else:
        EE = 0  # 충전 혹은 방전이 안되면 0으로 표기
        VE = 0  # 충전 혹은 방전이 안되면 0으로 표기
        CE = 0
    return [chE, dischE, EE, VE, CE]


# 효율분석용 데이터 저장된 것에서 VE, CE 다시 계산
def tempCalculateEff(confs, date):

    POWER_ZERO_CUT = 1  # 0으로 간주할 power의 절대값
    # 사용할 모듈번호 list
    modules = list(map(int, confs['MODULES'].split(confs['MS_SEP'])))
    # 사용할 스택번호 list
    stacks = list(map(int, confs['STACKS'].split(confs['MS_SEP'])))

    dateStr = date.isoformat()
    fileName = str(date.year)[2:]+str(date.month).zfill(2)+str(date.day).zfill(2)+'.csv'

    # 파일 읽어와서 df로 저장

    os.chdir(parseDir(confs["SAVE_DIR"]))
    df = pd.read_csv(fileName, dtype='unicode', index_col=False, )

    newDf = pd.DataFrame()  # 신규 DataFrame 생성

    results = [dateStr]  # 결과 데이터 저장 (처음 데이터는 날짜)

    # 계산에 필요한 필요한 데이터 헤더파일 읽어옴
    try:
        os.chdir(parseDir(confs["HDR_DIR"]))
        header = pd.read_csv(confs["EFF_HDR"], dtype='unicode', index_col=False)
        print("Successfully read efficiency header file")
    except Exception as exh:
        print("Cannot open the efficiency header file: ", exh)
        return False

    # 필요한 데이터만 추출 (시간, 각 스택 전압, 각 모듈별 전류)
    # 단, 필요한 데이터는 모두 header에 기록되어있다고 가정함
    try:
        for i in range(len(header)):
            col = int(header.iloc[i].Col)  # 값이 들어있는 컬럼번호
            name = header.iloc[i].Name
            # 시간은 시간으로 변환
            if name == "Time":
                valueList = df.iloc[:, col].tolist()
            # 나머지는 float 형으로 변환
            else:
                valueList = list(map(float, df.iloc[:, col].tolist()))
            newDf[name] = valueList
    except Exception as ex:
        print("In module tempCalculateEff: Error reading data: ", ex)
        return False

    # 시간차 계산
    newDf['Time'] = pd.to_datetime(newDf['Time'], format='%Y-%m-%d %H:%M:%S')
    dt = newDf['Time'].diff().apply(lambda x: x/np.timedelta64(1, 's')/3600).fillna(0)  # hr로 환산한 시간차

    # 에너지 및 효율 계산
    try:
        for m in modules:
            m_str = str(m)
            cur = newDf['M'+m_str+".Cur"]   # 전체전류
            vol = newDf['M'+m_str+".Vol"]   # 전체전압
            results = results + calEnergyAndEff(vol, cur, dt)
            for s in stacks:
                s_str = str(s)
                vol = newDf['M'+m_str+".Vol"+s_str]
                results = results + calEnergyAndEff(vol, cur, dt)
        print("Successfully calculated energies and efficiencies")
    except Exception as ex2:
        print("Error in calculating energies and efficiencies. ", ex2)
        return False

    # 데이터 저장
    try:
        dst = getFullPath(parseDir(confs["SAVE_DIR"]), confs["EFF_FILE"])
        saveEff(dst, modules, stacks, results)
        print("successfully updated efficiency file.")
    except Exception:
        return False

    return True

if __name__ == "__main__":
    #
    # argument
    #   DHL_DATA_Processing.py (arg1) (arg2) (arg3) (arg4) (arg5)
    #   arg 형식
    #   name=value (공백넣지 말 것, name은 대소문자 구분 안함)
    #       CONF_INIT: 설정 읽을 파일명
    #       CONF_DIR: CONF_INIT 데이터가 저장된 경로
    #       start:  데이터 처리 시작날짜
    #       end: 데이터 처리 종료날짜
    #       date_fmt: 날짜표기포맷 (ex: yymmdd)
    #

    # 데이터 세팅 저장
    confs = setConfig()

    start = confs["START"]
    end = confs["END"]
    dateFormat = confs["DATE_FMT"]

    startD = getDate(start, dateFormat)
    endD = getDate(end, dateFormat)
    diff = endD - startD

    # 날짜에 대해서 반복
    for day in range(diff.days + 1):
        isodate = (startD + datetime.timedelta(day)).isoformat()
        filename = isodate+'.csv'
        print("Processing: ", isodate)

        # tempCalculateEff(confs, startD + datetime.timedelta(day))

        # ftp 접속하여 csv 파일 받아옴
        success = getRemoteFile(confs, filename)

        # rawCsv을 받아 daily csv 저장, monthly csv 업데이트, 시스템 및 스택 효율정보 업데이트
        success = processData(confs, confs["RAW_PREFIX"]+filename)

        # 함수 실행에 실패하면 다음 날짜로 진행하지 않음
        if not success:
            print("Quit due to error, day = ", isodate)
            break

    # 구글드라이브의 요금분석 데이터 업데이트
        # 이건 script 파일로 처리 필요

    # 구글드라이브의 스택 효율 시트 업데이트
        # 이것도 script 파일로 처리 필요

    # 구글 시트의 매일 저장된 데이터를 읽어 원하는 column만 정해진 기간 동안 일정 간격을 두고 뽑아서 csv로 저장 혹은 그래프 그릴 수 있는 함수 만들기!

    pass
