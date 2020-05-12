# -*- coding: utf-8 -*-
try:
    from os import scandir, walk
except ImportError:
    from scandir import scandir, walk

import os
import csvkit
import numpy
import codecs
import linecache
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from datetime import datetime, timedelta
from mysql.connector.errors import IntegrityError, DataError, DatabaseError
from chardet.universaldetector import UniversalDetector

from DBConnection import DBObject


# config file in ../config/
def getConfigPath(configName):
    curFilePath = os.path.abspath(__file__)
    curDir = os.path.abspath(os.path.join(curFilePath, os.pardir))
    parentDir = os.path.abspath(os.path.join(curDir, os.pardir))
    configPath = os.path.join(parentDir, 'config', configName)
    return configPath


def sendEmail(emailBody, isIntegrityError):
    # print emailBody
    # return
    COMMASPACE = ', '
    if isIntegrityError:
        address = ['jianli@leightonobrien.com']
    else:
        address = ['jianli@leightonobrien.com']
        # address = ['jianli@leightonobrien.com', 'bradbloomfield@leightonobrien.com',
        #            'steveobrien@leightonobrien.com']

    sender = 'lobicinganotification@gmail.com'
    msg = MIMEMultipart()
    body = MIMEText(emailBody, 'plain')
    msg['Subject'] = 'CSV Import Error'
    # me == the sender's email address
    # family = the list of all recipients' email addresses
    msg['From'] = sender
    msg['To'] = COMMASPACE.join(address)
    msg.attach(body)
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    try:
        server.login('lobicinganotification@gmail.com', 'vssywoykbojtcvgg')
        server.sendmail(sender, address, msg.as_string())
    except smtplib.SMTPException:
        time.sleep(10)
        server.login('lobicinganotification@gmail.com', 'vssywoykbojtcvgg')
        server.sendmail(sender, address, msg.as_string())
    server.quit()


class BaseObject:
    alarmDict = {
        'companyID': None,
        'siteID': None,
        'AlarmSource': None,
        'ATGAlarmRaisedRecordID': None,
        'ATGAlarmClosedRecordID': None,
        'ATGAlarmCategory': None,
        'ATGAlarmType': None,
        'AlarmState': None,
        'AlarmingEntityID': None,
        'PumpID': None,
        'DataDrivenSource': None,
        'DataDrivenAlarmRaiseValue': None,
        'DataDrivenAlarmClearValue': None,
        'AlarmRaisedDate': None,
        'AlarmClosedDate': None,
        'LastRecordChangedDateHour': None,
        'AlarmAckDate': None,
        'AlarmAckBy': None,
        'AlarmAckNotes': None
    }

    def __init__(self):
        self.out_cursor = None
        self.out_cnx = None
        pass

    def raiseAlarm(self, alarmDict):
        # PK
        companyID = alarmDict['companyID']
        siteID = alarmDict['siteID']
        AlarmingEntityID = alarmDict['AlarmingEntityID']
        PumpID = alarmDict['PumpID']
        ATGAlarmCategory = alarmDict['ATGAlarmCategory']
        ATGAlarmType = alarmDict['ATGAlarmType']
        AlarmClosedDate = alarmDict['AlarmClosedDate']
        if AlarmClosedDate is None:
            AlarmClosedDate = '1970-01-01 00:00:00'

        # Not PK
        AlarmRaisedDate = alarmDict['AlarmRaisedDate']
        if AlarmRaisedDate is None:
            AlarmRaisedDate = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        ATGAlarmRaisedRecordID = alarmDict['ATGAlarmRaisedRecordID']
        if ATGAlarmRaisedRecordID is None:
            ATGAlarmRaisedRecordID = 'NONE'

        AlarmState = alarmDict['AlarmState']
        DataDrivenAlarmRaiseValue = alarmDict['DataDrivenAlarmRaiseValue']
        if DataDrivenAlarmRaiseValue is None:
            DataDrivenAlarmRaiseValue = 0.0

        DataDrivenAlarmClearValue = alarmDict['DataDrivenAlarmClearValue']
        if DataDrivenAlarmClearValue is None:
            DataDrivenAlarmClearValue = 0.0

        AlarmSource = alarmDict['AlarmSource']
        if AlarmSource is None:
            AlarmSource = 'DataDriven'

        DataDrivenSource = alarmDict['DataDrivenSource']
        LastRecordChangedDateHour = alarmDict['LastRecordChangedDateHour']

        sql1 = 'INSERT INTO `Alarms` ' \
               '(`companyID`,`siteID`,' \
               '`AlarmSource`,`ATGAlarmRaisedRecordID`,' \
               '`ATGAlarmCategory`,`ATGAlarmType`,' \
               '`AlarmState`,`AlarmingEntityID`,' \
               '`PumpID`, ' \
               '`DataDrivenSource`, `DataDrivenAlarmRaiseValue`, ' \
               '`DataDrivenAlarmClearValue`,`AlarmRaisedDate`,' \
               '`AlarmClosedDate`, `LastRecordChangedDateHour`) ' \
               'VALUES (%(companyID)s,%(siteID)s,' \
               '%(AlarmSource)s,%(ATGAlarmRaisedRecordID)s,' \
               '%(ATGAlarmCategory)s,%(ATGAlarmType)s,' \
               '%(AlarmState)s, %(AlarmingEntityID)s,' \
               '%(PumpID)s,' \
               '%(DataDrivenSource)s,%(DataDrivenAlarmRaiseValue)s,' \
               '%(DataDrivenAlarmClearValue)s,%(AlarmRaisedDate)s,' \
               '%(AlarmClosedDate)s, %(LastRecordChangedDateHour)s)'

        values1 = {'companyID': companyID, 'siteID': siteID,
                   'AlarmSource': AlarmSource, 'ATGAlarmRaisedRecordID': ATGAlarmRaisedRecordID,
                   'ATGAlarmCategory': ATGAlarmCategory, 'ATGAlarmType': ATGAlarmType,
                   'AlarmState': AlarmState, 'AlarmingEntityID': AlarmingEntityID,
                   'PumpID': PumpID,
                   'DataDrivenSource': DataDrivenSource, 'DataDrivenAlarmRaiseValue': DataDrivenAlarmRaiseValue,
                   'DataDrivenAlarmClearValue': DataDrivenAlarmClearValue,
                   'AlarmRaisedDate': AlarmRaisedDate, 'AlarmClosedDate': AlarmClosedDate,
                   'LastRecordChangedDateHour': LastRecordChangedDateHour}

        try:
            self.out_cursor.execute(sql1, values1)
            self.out_cnx.commit()
        except IntegrityError:
            pass

    def closeAlarm(self, alarmDict):
        # PK
        companyID = alarmDict['companyID']
        siteID = alarmDict['siteID']
        AlarmingEntityID = alarmDict['AlarmingEntityID']
        PumpID = alarmDict['PumpID']
        ATGAlarmCategory = alarmDict['ATGAlarmCategory']
        ATGAlarmType = alarmDict['ATGAlarmType']
        AlarmClosedDate = alarmDict['AlarmClosedDate']

        # Not PK
        AlarmState = alarmDict['AlarmState']
        DataDrivenAlarmClearValue = alarmDict['DataDrivenAlarmClearValue']

        # Looking for exist record which has older date but same other fields.
        alarmSQL = 'SELECT AlarmRaisedDate ' \
                   'FROM `Alarms` ' \
                   'WHERE AlarmState = %(AlarmState)s ' \
                   'AND companyID = %(companyID)s ' \
                   'AND siteID = %(siteID)s ' \
                   'AND AlarmingEntityID = %(AlarmingEntityID)s ' \
                   'AND PumpID = %(PumpID)s ' \
                   'AND ATGAlarmCategory = %(ATGAlarmCategory)s ' \
                   'AND ATGAlarmType = %(ATGAlarmType)s ' \
                   'AND AlarmClosedDate = %(DefaultAlarmClosedDate)s;'

        # DataDrivenAlarmClearValue of raised alarm is 0
        alarmValues = {'companyID': companyID, 'siteID': siteID,
                       'ATGAlarmCategory': ATGAlarmCategory, 'ATGAlarmType': ATGAlarmType,
                       'AlarmState': 2, 'AlarmingEntityID': AlarmingEntityID,
                       'PumpID': PumpID, 'ClearedAlarmState': AlarmState,
                       'DefaultAlarmClosedDate': '1970-01-01 00:00:00',
                       'DataDrivenAlarmClearValue': 0.0,
                       }

        self.out_cursor.execute(alarmSQL, alarmValues)
        rows = self.out_cursor.fetchall()

        # Ignore closed alarm which doesn't match exist raised alarm
        # You should find only 1 result
        for row in rows:
            # datetime.datetime type
            raisedDate = row['AlarmRaisedDate']
            closedDate = datetime.strptime(AlarmClosedDate, '%Y-%m-%d %H:%M:%S')
            # Need update exist record
            if raisedDate < closedDate:
                AlarmRaisedDate = raisedDate.strftime('%Y-%m-%d %H:%M:%S')
                AlarmClosedDate = closedDate.strftime('%Y-%m-%d %H:%M:%S')
                # Update item in alarmValues
                alarmValues['AlarmRaisedDate'] = AlarmRaisedDate
                alarmValues['AlarmClosedDate'] = AlarmClosedDate
                # Need update DataDrivenAlarmClearValue for raised alarm
                alarmValues['DataDrivenAlarmClearValue'] = DataDrivenAlarmClearValue
                updateSQL = 'UPDATE `Alarms` ' \
                            'SET `AlarmState` = %(ClearedAlarmState)s, ' \
                            'AlarmClosedDate = %(AlarmClosedDate)s, ' \
                            'DataDrivenAlarmClearValue = %(DataDrivenAlarmClearValue)s ' \
                            'WHERE `companyID`= %(companyID)s ' \
                            'AND `siteID`= %(siteID)s ' \
                            'AND `AlarmingEntityID`= %(AlarmingEntityID)s ' \
                            'AND `PumpID`= %(PumpID)s ' \
                            'AND `ATGAlarmCategory`= %(ATGAlarmCategory)s ' \
                            'AND `ATGAlarmType`= %(ATGAlarmType)s ' \
                            'AND `AlarmRaisedDate` = %(AlarmRaisedDate)s;'

                self.out_cursor.execute(updateSQL, alarmValues)
                self.out_cnx.commit()

    def resetDict(self):
        for key in self.alarmDict.keys():
            self.alarmDict[key] = None


class BaseInputDBIPData(BaseObject):
    def __init__(self, configName):
        dbObject = DBObject(configName)
        # IN tables
        self.in_cnx = dbObject.in_cnx
        self.in_cursor = dbObject.in_cursor

        # OUT tables
        self.out_cnx = dbObject.out_cnx
        self.out_cursor = dbObject.out_cursor

    def queryRecords(self, utcnow=None):
        raise NotImplementedError('queryRecords must be provided by each subclass of BaseInputDBIPData.')

    def insertRecords(self, result):
        raise NotImplementedError('insertRecords must be provided by each subclass of BaseInputDBIPData.')

    def median(self, lst):
        return numpy.median(numpy.array(lst))


class BaseInputData(BaseObject):
    field_names = ()
    rootPath = ''
    hasFile = False
    parentDir = None
    needSendEmail = True

    def __init__(self, configName):
        self.numFiles = 0
        dbObject = DBObject(configName)
        # Path contains all folders of OUT tables
        self.parentDir = dbObject.out_rootPath

        # Used by csv import
        self.cnx = dbObject.out_cnx
        self.cursor = dbObject.out_cursor

        # Used by raise and close alarm
        self.out_cnx = dbObject.out_cnx
        self.out_cursor = dbObject.out_cursor

        self.detector = UniversalDetector()

    def setFieldName(self, field_names):
        self.field_names = field_names

    def setRootPath(self, rootPath):
        self.rootPath = rootPath

    def setNeedSendEmail(self):
        self.needSendEmail = True

    # Return companyID and SiteID
    def getID(self, wholePath):
        lineNum = 2
        line = linecache.getline(wholePath, lineNum)
        values = line.split(',')
        return values[0], values[2]

    def importData(self, cnx, cursor, nextOne):
        raise NotImplementedError('importData must be provided by each subclass of BaseInputData.')

    def readCSV(self, wholePath, fileName, field_names, cnx, cursor):
        emailMsg = ''
        hasError = False
        isIntegrityError = False
        # File contains only header or nothing is treated as empty
        numberLines = len(open(wholePath, 'rU').readlines()) - 1
        if numberLines <= 0:
            print fileName + " is EMPTY!"
            self.moveToProcessed(fileName=fileName)
            return
        companyID, siteID = self.getID(wholePath)
        recordDateTime = '1970-01-01 00:00:00'
        fileReceivedDateTime = '1970-01-01 00:00:00'

        # Import records to target table
        with open(wholePath, 'r') as f:
            c = csvkit.DictReader(f, fieldnames=field_names)
            # skip first line which is field names
            c.next()
            totalDataLine = 0
            while True:
                try:
                    nextOne = c.next()
                    totalDataLine += 1
                    self.importData(cnx, cursor, nextOne)
                except StopIteration:
                    print ('Process Done')
                    break
                except IntegrityError as exp:
                    hasError = True
                    content = 'Error:' + str(exp) + ' FileName:' + wholePath + ' LineNumber:' + str(totalDataLine + 1)
                    emailMsg += content + '\n'
                    print (str(content))
                except (DataError, ValueError, DatabaseError) as exp:
                    hasError = True
                    content = 'Error:' + str(exp) + ' FileName:' + wholePath + ' LineNumber:' + str(totalDataLine + 1)
                    emailMsg += content + '\n'
                    print (str(content))

            if hasError:
                f.close()
                self.moveToError(fileName=fileName)
            else:
                f.close()
                self.moveToProcessed(fileName=fileName)

            if self.needSendEmail and hasError:
                if "Duplicate entry" in emailMsg:
                    isIntegrityError = True
                sendEmail(emailMsg, isIntegrityError)

            print ('Total Data line:' + str(totalDataLine))

    def process(self):
        self.createProcessedDir()
        self.createErrorDir()
        for root, dirs, files in walk(self.rootPath):
            # only process files in rootPath, ignore all subdir
            if root == self.rootPath:
                if len(files) > 0:
                    self.hasFile = True
                for fileName in files:
                    if "csv" not in fileName:
                        continue
                    self.numFiles += 1
                    print (str(self.numFiles) + " " + fileName)
                    wholePath = os.path.join(self.rootPath, fileName)
                    try:
                        self.readCSV(wholePath, fileName, self.field_names, self.cnx, self.cursor)
                    except Exception as e:
                        errorMsg = "Receive exception: %s when process %s. " % (str(e), wholePath)
                        sendEmail(emailBody=errorMsg, isIntegrityError=False)
                        self.moveToError(fileName=fileName)

    def processFile(self, fileName):
        self.createProcessedDir()
        self.createErrorDir()
        self.numFiles += 1
        print (str(self.numFiles) + " " + fileName)
        wholePath = os.path.join(self.rootPath, fileName)
        if "csv" not in fileName:
            self.moveToError(fileName=fileName)
        else:
            try:
                self.readCSV(wholePath, fileName, self.field_names, self.cnx, self.cursor)
            except Exception as e:
                errorMsg = "Receive exception: %s when process %s. " % (str(e), wholePath)
                sendEmail(emailBody=errorMsg, isIntegrityError=False)
                if 'No such file or directory' not in errorMsg:
                    self.moveToError(fileName=fileName)

    def convertDate(self, dateStr):
        if dateStr:
            date, timestamp = dateStr.split(' ')
            day, month, year = date.split('-')
            return year + '-' + month + '-' + day + ' ' + timestamp

    def createProcessedDir(self):
        dstPath = os.path.join(self.rootPath, 'Processed')
        if not os.path.exists(dstPath):
            os.mkdir(dstPath)

    def createErrorDir(self):
        dstPath = os.path.join(self.rootPath, 'Error')
        if not os.path.exists(dstPath):
            os.mkdir(dstPath)

    def moveToProcessed(self, fileName=None):
        dstPath = os.path.join(self.rootPath, 'Processed')

        orgName = os.path.join(self.rootPath, fileName)
        dstName = os.path.join(dstPath, fileName)
        os.rename(orgName, dstName)

    def moveToError(self, fileName=None):
        dstPath = os.path.join(self.rootPath, 'Error')

        orgName = os.path.join(self.rootPath, fileName)
        dstName = os.path.join(dstPath, fileName)
        os.rename(orgName, dstName)

    def createOriginalDir(self):
        dstPath = os.path.join(self.rootPath, 'Original')
        if not os.path.exists(dstPath):
            os.mkdir(dstPath)
        return dstPath

    def checkAndConvertToUTF8(self):
        for root, dirs, files in walk(self.rootPath):
            # only process files in rootPath, ignore all subdir
            if root == self.rootPath:
                for fileName in files:
                    wholePath = os.path.join(self.rootPath, fileName)
                    encodingType, confidence = self.detectEncoding(wholePath=wholePath)
                    if not encodingType == 'ascii':
                        self.convertToUTF8(fileName=fileName, encodingType=encodingType)

    def convertToUTF8(self, fileName, encodingType):
        orgPath = self.createOriginalDir()
        blockSize = 1048576  # or some other, desired size in bytes
        tmpFileName = fileName + 'tmp'
        wholePath = os.path.join(self.rootPath, fileName)
        originalPath = os.path.join(orgPath, fileName)
        tmpFilePath = os.path.join(self.rootPath, tmpFileName)
        with codecs.open(wholePath, "r", encodingType) as sourceFile:
            with codecs.open(tmpFilePath, "w", "utf-8") as targetFile:
                while True:
                    contents = sourceFile.read(blockSize)
                    if not contents:
                        break
                    targetFile.write(contents)
        os.rename(wholePath, originalPath)
        os.rename(tmpFilePath, wholePath)

    def detectEncoding(self, wholePath):
        self.detector.reset()
        for line in file(wholePath, 'rb'):
            self.detector.feed(line)
            if self.detector.done:
                break
        self.detector.close()
        encodingType = self.detector.result['encoding']
        confidence = self.detector.result['confidence']
        return encodingType, confidence

    def isFiveMinutesOlder(self, fileName):
        fiveMin = timedelta(minutes=5)
        now = datetime.now()
        try:
            mtime = os.path.getctime(filename=fileName)
        except OSError:
            mtime = 0

        last_modified_date = datetime.fromtimestamp(mtime)
        fiveMinsAgo = now - fiveMin
        if last_modified_date >= fiveMinsAgo:
            return True
        else:
            return False

    # For trends rolling, obs maps, obs tables
    def generate_update_statement(self, col_set, table_name, pk_set):
        sql_prefix = 'UPDATE `{}` SET '.format(table_name)
        sql_postfix = 'WHERE '
        sql = ''

        col_count = 1
        pk_count = 1

        col_total = len(col_set)
        pk_total = len(pk_set)

        for pk in pk_set:
            if pk_count == pk_total:
                sql_postfix += '`{0}` = %({0})s;'.format(pk)
            else:
                sql_postfix += '`{0}` = %({0})s AND '.format(pk)
            pk_count += 1

        for key in col_set:
            if col_count == col_total:
                sql += '`{0}` = %({0})s '.format(key)
            else:
                sql += '`{0}` = %({0})s, '.format(key)

            col_count += 1

        return sql_prefix + sql + sql_postfix


def writeToCSV(dstFile, rows, start, end, field_names):
    with open(dstFile, 'w') as f:
        c = csvkit.DictWriter(f, field_names)
        c.writeheader()
        for i in range(start, end):
            c.writerow(rows[i])


def checkOrCreateDir(dirPath):
    if not os.path.exists(dirPath):
        os.mkdir(dirPath)


# For all ATG csv export
class BaseExportATGData(object):
    # siteID : siteCode
    siteCodeIDMap = {}
    # siteCode doesn't exist in Sites table
    missingSiteCode = []

    def __init__(self, configName):
        dbObject = DBObject(configName)
        self.parentDir = dbObject.in_rootPath
        self.cnx = dbObject.in_cnx
        self.cursor = dbObject.in_cursor
        self.siteCode_list = dbObject.siteCode_list

    # def exportCSV(self, utc_now):
    #     raise NotImplementedError('exportCSV must be provided by each subclass of BaseExportATGData.')

    def buildSiteCodeIDMapping(self):
        if len(self.siteCode_list) == 0:
            return
        siteCodeList = self.siteCode_list.split(',')
        for siteCode in siteCodeList:
            sql = 'SELECT siteID FROM Sites WHERE siteCode = %(siteCode)s;'
            values = {'siteCode': siteCode}
            self.cursor.execute(sql, values)
            self.cnx.commit()
            result = self.cursor.fetchall()
            if len(result):
                siteID = result[0]['siteID']
                self.siteCodeIDMap[siteID] = siteCode
            else:
                self.missingSiteCode.append(siteCode)

    def getNeedProcessedSiteIDSet(self, siteIDSet):
        if len(self.siteCodeIDMap) == 0 and len(self.siteCode_list) == 0:
            return siteIDSet

        needProcessedSiteIDSet = siteIDSet.copy()
        for siteID in siteIDSet:
            if siteID not in self.siteCodeIDMap:
                needProcessedSiteIDSet.discard(siteID)
        return needProcessedSiteIDSet
