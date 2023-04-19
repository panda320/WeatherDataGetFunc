import azure.functions as func
import logging
import requests
import os

def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python Time trigger function processe start.')

    #AzureFunctionsの環境変数から実行環境を取得
    env = os.environ.get('ENVIRONMENT')
    #開発環境の場合
    if env == 'Dev':
        url = 'http://localhost:7071/api/WeatherDataDbInsert'
    #テスト環境の場合
    elif env == 'Test':
        url = 'https://WeatherDataGetFunc-test.azurewebsites.net/api/WeatherDataDbInsert'
    #本番環境の場合
    elif env == 'Prod':
        url = 'https://WeatherDataGetFunc.azurewebsites.net/api/WeatherDataDbInsert'
    #実行環境が取得できない場合
    else:
        logging.error('(WeatherDataTimer)Environment is not set.')
        return func.HttpResponse(
            "(WeatherDataTimer)Environment is not set.",
            status_code=500
        )
    
    #requestsでWeatherDataDBInsertを実行&HTTPステータスコードを取得
    payload = {"plat":"AzureSQLDB"}
    r = requests.get(url, params=payload)
    logging.info('(WeatherDataTimer)HTTP Status Code: %s', r.status_code)
    #HTTPステータスコードが200以外の場合
    if r.status_code != 200:
        logging.error('(WeatherDataTimer)HTTP Status Code is not 200.')
        return func.HttpResponse(
            "(WeatherDataTimer)HTTP Status Code is not 200.",
            status_code=500
        )

    logging.info('(WeatherDataTimer)Python Time trigger function processe end.')
