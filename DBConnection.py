#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 14/12/17 16:42
# @Author  : JackLi
# @File    : DBConnection.py

from utility.getDBinfo import getDBConfig
import os
from mysql import connector


def getConfigPath(configName):
    curFilePath = os.path.abspath(__file__)
    curDir = os.path.abspath(os.path.join(curFilePath, os.pardir))
    parentDir = os.path.abspath(os.path.join(curDir, os.pardir))
    configPath = os.path.join(parentDir, 'config', configName)
    return configPath


def checkOrCreateDir(dirPath):
    if not os.path.exists(dirPath):
        os.mkdir(dirPath)


class DBObject(object):
    def __init__(self, configName):
        self.configPath = getConfigPath(configName=configName)
        _config_dict = getDBConfig(configPath=self.configPath)

        if not ('DB-IN' in _config_dict and 'DB-OUT' in _config_dict):
            raise Exception('DB configure name must be DB-IN and DB-OUT in config file: ' + self.configPath + ' !')

        # DB-IN
        _info_dict = _config_dict['DB-IN']
        _user = _info_dict['user']
        _password = _info_dict['password']
        _host = _info_dict['host']
        _database = _info_dict['database']
        _sitecode_list = _info_dict['siteCodeList']
        # Folder which contains upload, error, archive, tmp, and empty folders
        # Or path for exported csv which from IN tables
        self.in_rootPath = _info_dict['inRootPath']
        self.in_cnx = connector.connect(host=_host, user=_user, password=_password, database=_database, autocommit=True)
        self.in_cursor = self.in_cnx.cursor(dictionary=True, buffered=True)
        self.siteCode_list = _sitecode_list

        # DB-OUT
        _info_dict = _config_dict['DB-OUT']
        _user = _info_dict['user']
        _password = _info_dict['password']
        _host = _info_dict['host']
        _database = _info_dict['database']
        # RooPath which contains all folders of OUT tables like OUT-TransRolling
        self.out_rootPath = _info_dict['outRootPath']
        self.out_cnx = connector.connect(host=_host, user=_user, password=_password, database=_database)
        self.out_cursor = self.out_cnx.cursor(dictionary=True, buffered=True)
