# -*- coding: utf-8 -*-

import json
import requests
import sys, argparse
import os
import csv
import urllib.request
from urllib import parse
from urllib3 import disable_warnings
from datetime import datetime as dt
import logging
import functools
from requests_negotiate_sspi import HttpNegotiateAuth
import pandas as pd
import numpy as np
import re


def save_to_csv(df, path, name,prefix='',postfix='',  encoding='utf-8', sep = ';'):
    file_name = prefix+name + postfix +'.csv'
    out_path = os.path.join(path,file_name )
    df.to_csv( out_path,
                sep=sep,
                encoding=encoding, 
                index=False, 
                quoting=csv.QUOTE_ALL ,
                quotechar='"' )
    print(file_name)
    return out_path
    

def open_dicts(cell):
    if type(cell) is dict:
        cell = list(cell.values())
        cell = cell[0] if len(cell)==1 else cell
    return cell


def replace_dashes(cell):
    if cell=='-':
        cell = ''
    return cell


def open_list(df, column):
    return pd.DataFrame({
            col:np.repeat(df[col].values, df[column].str.len())
            for col in df.columns.drop(column)
            }).assign(**{column:np.concatenate(df[column].values)})[df.columns]


class BitrixAPIRequester:
    def __init__(self,  url, method):
        self.url = url
        self.method   = method

    def get_data(self, params=None):
        url = self.url + self.method 
        params = params
        result = []
        response = requests.get(url,params=params, auth=HttpNegotiateAuth(domain="GAZPROM-NEFT", delegate=True),  
                                verify=False, timeout=60).json()
        result.append(response['result'])
        while 'next' in response:
            params["start"] = str(response['next'])
            response = requests.get(url,params=params, auth=HttpNegotiateAuth(domain="GAZPROM-NEFT", delegate=True),  
                                verify=False, timeout=60).json()
            result.append(response['result'])
        return result


if __name__ == "__main__":
    os.environ['NO_PROXY'] = '127.0.0.1'
    disable_warnings()
    source_path  = sys.argv[1] if 1 < len(sys.argv)  else r'\\gazprom-neft\dfs\Газпром нефть\Проекты\Фундамент\БЛПС\SUD_BITRIX_FILE'        #sys.argv[1]
    url          = sys.argv[2] if 2 < len(sys.argv)  else  r'https://sppr.gazprom-neft.local/rest/4009/xc973ioevrt5y3f7/'      #sys.argv[2]
    property_name  = sys.argv[3]  if 3 < len(sys.argv)  else r'Product_cost_approval'         #sys.argv[3]
    
    logger = logging.getLogger('bitrix_logger')
    file_handler = logging.FileHandler(os.path.join(source_path, 'Logs',dt.now().strftime("%Y%m%d") + '_bitrix_log.txt'))
    formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
    file_handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)

    postfix = '_'+dt.now().strftime("%Y%m%d%H%M%S")
    params  = {"IBLOCK_TYPE_ID":'bitrix_processes', "IBLOCK_CODE":'Change_assortiment_CM'}


    def write_log( logger,file_handler, message, type ="error"):
        logger.addHandler(file_handler)
        if type=="error":
            logger.error(message)
        else:
            logger.info(message)
        file_handler.close()
        logger.removeHandler(file_handler)
    

    def get_elements(url, params, columns=[]):
        elements_get = BitrixAPIRequester(url, r'lists.element.get')
        elements = elements_get.get_data(params=params )
        elements = [i for sublist in elements for i in sublist ]
        elements_df = pd.DataFrame(elements)
        if len(columns) > 0:
            elements_df = elements_df[columns]
        fields_get = BitrixAPIRequester(url, r'lists.field.get')
        fields = fields_get.get_data(params=params)
        fields = [v for k,v in fields[0].items()]
        fields_df = pd.DataFrame(fields)
        elements_df = elements_df.applymap(open_dicts)
        elements_df.replace(r'\n',' ', regex=True, inplace=True)
        elements_df.replace(r'\r',' ', regex=True, inplace=True)
        elements_df.replace(r'\t',' ', regex=True, inplace=True)
        # переименование столбцов lists.element на основе lists.field, а также присвоение значений свойствам, ссылающимся на lists.field 
        if {'CODE', 'DISPLAY_VALUES_FORM'}.issubset(fields_df.columns):  
            column_names = {k:v if not  pd.isna(v) else k for k,v in  dict(zip(fields_df["FIELD_ID"], fields_df["CODE"])).items()}
            elements_df.rename(columns=column_names, inplace =True)
            property_df =  fields_df[fields_df['DISPLAY_VALUES_FORM'].notnull()][['CODE','DISPLAY_VALUES_FORM']]
            for column in elements_df:
                if column in property_df['CODE'].values:
                    # составление словаря id свойств и их значений вида {'865938': '21_11 G-Motion  МЗСМ'}
                    mapping_dict = property_df['DISPLAY_VALUES_FORM'].loc[property_df['CODE']==column].values[0]
                    # все элементы свойств превращаем в list [], чтобы можно было пройтись в цикле по каждой строке
                    elements_df[column] = [[] if x is np.nan else x if type(x) is list else [x] for x in elements_df[column] ]  
                    # маппим значения свойств на их id. в каждой строке  бежим в цикле
                    elements_df[column] = elements_df[column]\
                                                .apply(lambda row: [mapping_dict[v] for v in row if mapping_dict.get(v)])
                    
        return elements_df


    def select_handler(property_name):
        return {
            'Product_test_costs': much_worse_property_handler
        }.get(property_name, property_handler)  

    # парсим свойство в виде dict   
    def property_handler(property_df,property_name):
        # преобразуем dict в столбцы
        property_df = pd.concat([property_df, 
                                 pd.DataFrame(property_df[property_name].values.tolist(), index=property_df.index)], axis=1).\
                                 drop(property_name, axis=1)
        # раскрывайем возможные внутренние lists                                   
        for column in property_df.columns:
            if  property_df[column].apply(lambda x: type(x) is list).any():
                property_df[column] = property_df[column].apply(lambda x: [x] if type(x) is not list else x)
                property_df = open_list(property_df,column )
        property_df = property_df.applymap(replace_dashes)
        property_df = property_df.fillna(0)
        return property_df

    # парсим свойство вида [1000|RUR, 1000 EUR]
    def much_worse_property_handler(property_df,property_name ):
        property_df = pd.concat([property_df, 
                                 pd.DataFrame(property_df[property_name]\
                                    .apply(lambda x: pd.Series(x.split('|'))),index=property_df.index)], axis=1)\
                                    .drop(property_name, axis=1)
        property_df.columns=['ID','VALUE', 'CURRENCY']
        return property_df

        
    try:
        iblock = get_elements(url,params )
        property_df = iblock[iblock[property_name].notnull() & iblock[property_name]!=False ][['ID',property_name]]
        property_df[property_name] = property_df[property_name].apply(lambda x: [x] if type(x) is not list else x)
        property_df = open_list(property_df,property_name )
        property_df = select_handler(property_name)(property_df,property_name) 
        write_log(logger,file_handler, f"{property_name} was succesfully loaded with arguments:  {sys.argv}", type="success")
    except Exception as e:
        write_log(logger,file_handler, "Exception during loading: %s" % repr(e)+f";with arguments:  {sys.argv}")
        sys.exit(1)

    try:
        source_path = ''
        out_path = save_to_csv(property_df, source_path,name =property_name ,postfix=postfix ,sep=';')
        write_log(logger,file_handler, f"{property_name} was succesfully saved to {out_path}" , type="success")
    except Exception as e:
        write_log(logger,file_handler, "Exception during saving: %s" % repr(e)+f";with arguments:  {sys.argv}")
        sys.exit(1)


