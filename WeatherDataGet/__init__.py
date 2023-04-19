import azure.functions as func
import logging
import pandas as pd

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('(WeatherDataGet)Python HTTP trigger function processed a request.')

    #URL設定とデータフレーム初期化
    url = 'https://www.data.jma.go.jp/obd/stats/data/mdrr/tem_rct/alltable/mxtemsadext00_rct.csv'
    df = None

    #気象庁のデータを取得しユニコードに変換
    df = pd.read_csv(url, encoding='shift-jis')
    #32列目から36列目を削除
    df = df.drop(df.columns[31:36], axis=1)
    #列名を変更
    df = df.rename(columns={'観測所番号':'station_id',
                            '都道府県':'prefecture',
                            '地点':'location',
                            '国際地点番号':'international_id',
                            '現在時刻(年)':'current_year',
                            '現在時刻(月)':'current_month',
                            '現在時刻(日)':'current_day',
                            '現在時刻(時)':'current_hour',
                            '現在時刻(分)':'current_minute',
                            '今日の最高気温(℃)':'max_temp',
                            '今日の最高気温の品質情報':'max_temp_quality',
                            '今日の最高気温起時（時）':'max_temp_hour',
                            '今日の最高気温起時（分）':'max_temp_minute',
                            '今日の最高気温起時の品質情報':'max_temp_hour_quality',
                            '平年差（℃）':'normal_diff',
                            '前日差（℃）':'prev_diff',
                            '該当旬（月）':'month_of_season',
                            '該当旬（旬）':'season_of_year',
                            '極値更新':'extreme_update',
                            '10年未満での極値更新':'extreme_update_10yrs',
                            '今年最高':'highest_this_year',
                            '今年の最高気温（℃)（昨日まで）':'highest_this_year_temp',
                            '今年の最高気温（昨日まで）の品質情報':'highest_this_year_temp_quality',
                            '今年の最高気温（昨日まで）を観測した起日（年）':'highest_this_year_date_year',
                            '今年の最高気温（昨日まで）を観測した起日（月）':'highest_this_year_date_month',
                            '今年の最高気温（昨日まで）を観測した起日（日）':'highest_this_year_date_day',
                            '昨日までの観測史上1位の値（℃）':'highest_ever_temp',
                            '昨日までの観測史上1位の値の品質情報':'highest_ever_temp_quality',
                            '昨日までの観測史上1位の値を観測した起日（年）':'highest_ever_date_year',
                            '昨日までの観測史上1位の値を観測した起日（月）':'highest_ever_date_month',
                            '昨日までの観測史上1位の値を観測した起日（日）':'highest_ever_date_day',
                            '統計開始年':'start_year_of_stats'})
    #JSON形式に変換
    json_str = df.to_json(orient='records', force_ascii=False)

    #データ取得確認
    if df is None:
        logging.error('(WeatherDataGet)Failed to get weather data.(WeatherDataGet)')
        return func.HttpResponse(
            "(WeatherDataGet)Failed to get weather data.",
            status_code=500
        )
    else:
        logging.info('(WeatherDataGet)Success to get weather data.(WeatherDataGet)')
        return func.HttpResponse(
            json_str,
            status_code=200
        )
