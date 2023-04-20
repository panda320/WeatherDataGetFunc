import azure.functions as func
import logging
import pandas as pd
import requests
from sqlalchemy import create_engine, text, NVARCHAR
import datetime
import os

#keyvaultからDB接続情報を取得するためのライブラリをインポート
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('(WeatherDataDbInsert)Python HTTP trigger function processed a request.')

    #AzureFunctionsの環境変数から実行環境を取得
    env = os.environ.get('ENVIRONMENT')
    #開発環境の場合
    if env == 'Dev':
        url = 'http://localhost:7071/api/WeatherDataGet'
        odbcdriver = 'ODBC Driver 18 for SQL Server'
        logging.info('(WeatherDataDbInsert)Development environment.')
    #テスト環境の場合(Functionの場合は、ODBC Driver 17 for SQL Serverに変更する必要がある)
    elif env == 'Test':
        url = 'https://weatherdatagetfunc-test.azurewebsites.net/api/WeatherDataGet'
        odbcdriver = 'ODBC Driver 17 for SQL Server'
        logging.info('(WeatherDataDbInsert)Test environment.')
    #本番環境の場合(Functionの場合は、ODBC Driver 17 for SQL Serverに変更する必要がある)
    elif env == 'Prod':
        url = 'https://weatherdatagetfunc.azurewebsites.net/api/WeatherDataGet'
        odbcdriver = 'ODBC Driver 17 for SQL Server'
        logging.info('(WeatherDataDbInsert)Prod environment.')
    #実行環境が取得できない場合
    else:
        logging.error('(WeatherDataDbInsert)Environment is not set.')
        return func.HttpResponse(
            "(WeatherDataDbInsert)Environment is not set.",
            status_code=500
        )

    #keyvault共通設定
    keyVaultName = "ssanoDbSecretKeyVault"
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    database = 'ssdom320sqldatabase01'
    tablename = "WeatherDataFull"
    dbuser = 'ssano'
    
    #IaaSDBかAzureSQLDBかを判定するためのパラメータを取得
    plat = req.params.get('plat')

    #IaaSのDBの場合
    if plat == 'IaaS':
        #keyvaultからDBパスワードを取得
        secretName = "iaasdbpassword"
        secret = client.get_secret(secretName)
        #共通以外のIaaSDB接続情報を設定
        server = 'bak-sql2022-01.ssdom.com'
        dbuserpass = secret.value
        #IaaSDBに接続するための接続文字列を設定
        conn = f"mssql+pyodbc://{dbuser}:{dbuserpass}@{server}/{database}?driver={odbcdriver}&TrustServerCertificate=yes"
    #AzureSQLDBの場合
    elif plat == 'AzureSQLDB':
        #keyvaultからDBパスワードを取得
        secretName = "sqldatabasedbpassword"
        secret = client.get_secret(secretName)
        #共通以外のAzureSQLDB接続情報を設定
        server = 'ssdom320sqlserver01.database.windows.net'
        dbuserpass = secret.value
        #AzureSQLDBに接続するための接続文字列を設定。接続タイムアウト対策としてタイムアウト値を60秒に設定
        conn = f"mssql+pyodbc://{dbuser}:{dbuserpass}@{server}/{database}?driver={odbcdriver}&timeout=60"
        #conn = f"mssql+pyodbc://{dbuser}:{dbuserpass}@{server}/{database}?driver={odbcdriver}"
    #IaaSDBかAzureSQLDBかを判定するためのパラメータがない場合
    else:
        logging.error('(WeatherDataDbInsert)Plat parameter is not set.')
        return func.HttpResponse(
            "(WeatherDataDbInsert)Plat parameter is not set.",
            status_code=500
        )

    #実行日取得
    dt = datetime.datetime.today()
    execdate = dt.strftime('%Y-%m-%d')

    #APIから気象データ取得しデータフレームに格納
    res = requests.get(url).json()
    df = pd.DataFrame(res)
    #リクエストのHTTPステータスコードが200の場合は正常ログを出力。それ以外はエラーログを出力
    if res.status_code == 200:
        logging.info('(WeatherDataDbInsert) Data is got from WeatherDataGetAPI.')
    else:
        logging.error('(WeatherDataDbInsert)Data is not got from WeatherDataGetAPI.')
        return func.HttpResponse(
            "(WeatherDataDbInsert)Data is not got from WeatherDataGetAPI.",
            status_code=500
        )
    
    #データフレームに取得日列を追加
    df.insert(0, 'date', execdate)
    #AzureSQLdatbaseに接続
    engine = create_engine(conn)
    #AzureSQLdatbaseにデータを追加
    #df.to_sql(tablename, engine, if_exists='append', index=False, dtype={'prefecture': NVARCHAR , 'location':NVARCHAR })
    #AzureSQLdatabaseにデータを追加。接続タイムアウト対策として3回リトライする。
    for i in range(3):
        try:
            df.to_sql(tablename, engine, if_exists='append', index=False, dtype={'prefecture': NVARCHAR , 'location':NVARCHAR })
            break
        except:
            if i == 2:
                logging.error('(WeatherDataDbInsert)Data is not inserted.')
                return func.HttpResponse(
                    "(WeatherDataDbInsert)Data is not inserted.",
                    status_code=500
                )
            else:
                continue

    #AzureSQLdatabaseに正常にデータが追加されたか確認
    sql_query = f"select * from {tablename} where date = '{execdate}'"
    df2 = pd.DataFrame(engine.connect().execute(text(sql_query)))
    if len(df2) == 0:
        logging.error('(WeatherDataDbInsert)Data is not inserted.')
        return func.HttpResponse(
            "(WeatherDataDbInsert)Data is not inserted.",
            status_code=500
        )
    
    #正常終了処理
    logging.info('(WeatherDataDbInsert)This HTTP-triggered function executed successfully.')
    return func.HttpResponse(
        "This HTTP-triggered function executed successfully.(WeatherDataDbInsert) ",
        status_code=200
    )
