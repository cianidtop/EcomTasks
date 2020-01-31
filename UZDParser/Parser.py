from clickhouse_driver import Client
from configparser import ConfigParser
import pymysql.cursors
import zipfile
import io
import json

def UZD_to_Clickhouse():
    # cfg = ConfigParser()
    # cfg.read(db_config_path)
    #
    # db_name = "evo_archive_2019"
    # host = cfg.get(db_name, "host")
    # user = cfg.get(db_name, "user")
    # password = cfg.get(db_name, "password")
    # database = cfg.get(db_name, "database")
    # host = 'dev-dwh-pg.mailru.local'
    # user =  'pguser'
    # password = 'asdjksdd6fsv'
    # database = 'sandbox'
    #
    # connection = pymysql.connect(host=host,
    #                              user=user,
    #                              password=password,
    #                              db=database,
    #                              cursorclass=pymysql.cursors.DictCursor)
    #
    # # mycursor = connection.cursor()
    # # mycursor.execute(query)
    # # query_result = mycursor.fetchall()
    # connection.close()
    clickhouse_client = Client('172.30.200.27', user='EgorFokin', password='1zNYUd@MjC!N', database='sandbox')
    RowsFromFile = []
    z = zipfile.ZipFile('1.zip')
    json_data = z.read('fns_trans_data.json').decode('utf-8')
    r = json.loads(json_data)
    counter = 0
    batchID = 1

    for row in r:
        row['batchID'] = batchID
        RowsFromFile.append(row)
        counter+=1
        if counter == 5:
            # clickhouse_client.execute(
            #     'INSERT INTO sandbox.datavault_raw_test (batchID,intTransID,intFromTransID,intExiteDocID,varTransDatetime,varDocExchangeGUID,varTransGUID,varDocGUID,varSenderGUID,varRecipientGUID,varSosGUID,varTransType,varDocType,intTransState ) VALUES',
            #     RowsFromFile)
            with open('last_batch.txt', 'w') as last_batch:
                last_batch.write('Last TransID was:' + str(RowsFromFile[-1]['intTransID']))
            batchID+=1
            RowsFromFile = []
            counter = 0
    if counter != 0:
        # clickhouse_client.execute(
        #     'INSERT INTO sandbox.datavault_raw_test (batchID,intTransID,intFromTransID,intExiteDocID,varTransDatetime,varDocExchangeGUID,varTransGUID,varDocGUID,varSenderGUID,varRecipientGUID,varSosGUID,varTransType,varDocType,intTransState ) VALUES',
        #     RowsFromFile)
        with open('last_batch.txt', 'w') as last_batch:
            last_batch.write('last TransID was:' + str(RowsFromFile[-1]['intTransID']))

UZD_to_Clickhouse()