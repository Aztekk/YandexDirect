from typing import Dict, List
import requests
import json
from time import sleep
import pandas as pd
from io import StringIO
from datetime import datetime

TOKEN = ""  # Вставьте сюда свой OAuth токен
_DIRECT_URL = "https://api.direct.yandex.com/json/v5/"

TODAY = datetime.today().date().isoformat()


class YandexDirect(object):
    """
        Класс для работы с Yandex Direct
    """

    token = None

    def __init__(self, token=None):
        """
        Конструктор объекта

        :param token: OAuth-token
        :type token: str

        :return: nothing
        """

        self.token = token

        if not token:
            print('Недействительный OAuth-токен')

    @staticmethod
    def exception(result):
        """
        Печатает текст ошибки

        :param result: response object
        :type result: response object

        :return: nothing
        """

        print(result.json()['error']['error_string'])
        print(result.json()['error']['error_detail'])

    def get_header(self):
        """
        Генерирует заголовок для http-запроса

        :rtype: dict
        :return: header object
        """

        return {
            "Authorization": "Bearer {}".format(self.token),
            "Accept-Language": "ru",  # Язык ответных сообщений
        }

    def get_campaigns(self) -> Dict:
        """
        Возвращает словарь с кампаниями

        :rtype: dict
        :return: campaigns
        """

        url = _DIRECT_URL + 'campaigns'

        header = self.get_header()

        body = {"method": "get",
                "params": {"SelectionCriteria": {},  # Choosing campaigns
                           "FieldNames": ["Id",
                                          "Name",
                                          "State",
                                          "Type",
                                          "StartDate",
                                          "EndDate"]
                           }}

        # Encoding query to JSON
        json_body = json.dumps(body, ensure_ascii=False).encode('utf8')

        result = requests.post(url, json_body, headers=header)

        if b'error' in result.content:
            self.exception(result)
        elif result.status_code == 500:
            print("Server is unavaliable. Try again later or contact Yandex Support")
        else:
            return result.json()['result']['Campaigns']

    def get_ad_groups(self, campaign_ids: List) -> Dict:
        """
        Возвращает словарь с группами объявлений

        :param campaign_ids: список
        :type campaign_ids: list if ints

        :rtype: dict
        :return: группы объявлений
        """

        url = _DIRECT_URL + 'adgroups'

        header = self.get_header()

        body = {"method": "get",
                "params": {"SelectionCriteria": {"CampaignIds": campaign_ids},
                           "FieldNames": ["Id",
                                          "Name",
                                          "CampaignId",
                                          "Status",
                                          "Type",
                                          "Subtype"]
                           }}

        json_body = json.dumps(body, ensure_ascii=False).encode('utf8')

        result = requests.post(url, json_body, headers=header)

        if b'error' in result.content:
            self.exception(result)
        elif result.status_code == 500:
            print("Сервер не отвечает. Попробуйте позднее. Если ошибка повторяется, обратитесь в техподдержку Яндекс")
        else:
            return result.json()['result']['AdGroups']

    def get_ads(self, campaign_ids: List):
        """
        Возвращает словарь с объявлениями

        :param campaign_ids: список
        :type campaign_ids: list if ints

        :rtype: dict
        :return: слоарь с объявлениями
        """

        url = _DIRECT_URL + 'ads'

        header = self.get_header()

        body = {"method": "get",
                "params": {"SelectionCriteria":
                               {"CampaignIds": campaign_ids},
                           "FieldNames": ["Id",
                                          "AdGroupId",
                                          "CampaignId",
                                          "AdCategories",
                                          "State",
                                          "Status",
                                          "Type",
                                          "Subtype",
                                          "StatusClarification"]
                           }
                }

        json_body = json.dumps(body, ensure_ascii=False).encode('utf8')

        result = requests.post(url, json_body, headers=header)

        if b'error' in result.content:
            self.exception(result)
        elif result.status_code == 500:
            print("Server is unavaliable. Try again later or contact Yandex Support")
        else:
            return result.json()['result']['Ads']

    def get_report(self, report_type, field_names, date_range_type='CUSTOM_DATE', date_from=TODAY, date_to=TODAY,
                   include_vat='YES'):
        """
        Возвращает отчет об эффективности рекламных кампаний

        :param report_type: Тип отчета. Возможные значения в справке:
        https://yandex.ru/dev/direct/doc/reports/fields-list.html/
        :type report_type: str

        :param field_names: Параметры и показатели. Возможные значения в справке:
        https://yandex.ru/dev/direct/doc/reports/fields-list.html/
        :type field_names: list of str

        :param date_range_type: тип временного интервала. Возможные значения и подробности в справке:
        https://yandex.ru/dev/direct/doc/reports/period.html
        :type date_range_type: str

        :param date_from: дата начала отчётного периода "YYYY-MM-DD"
        :type date_from: str

        :param date_to: дата конца отчётного периода "YYYY-MM-DD"
        :type date_to: str

        :param include_vat: добавлять ли информацию об НДС
        :type include_vat: str

        :rtype: TSV string
        :return: returns report of chosen type and chosen dimensions & metrics
        """

        url = _DIRECT_URL + 'reports'

        header = {
            "Authorization": "Bearer {}".format(self.token),
            "Accept-Language": "ru",
            "processingMode": "auto",
            "returnMoneyInMicros": "true",
            "skipReportHeader": "true",
            "skipReportSummary": "true"
        }

        body = {
            "params": {
                "FieldNames": field_names,
                "ReportName": "Report1",
                "ReportType": report_type,
                "DateRangeType": date_range_type,
                "Format": "TSV",
                "IncludeVAT": include_vat,
                "IncludeDiscount": "NO"
            }
        }

        if date_range_type == 'CUSTOM_DATE':
            #  then date_from и date_to should be in SelectionCriteria
            body['params']["SelectionCriteria"] = {
                "DateFrom": date_from,
                "DateTo": date_to
            }
        else:
            # else - they shouldn't
            body['params']["SelectionCriteria"] = {}

            # encoding request to JSON
        json_body = json.dumps(body, ensure_ascii=False).encode('utf8')

        result = requests.post(url, json_body, headers=header)
        result.encoding = 'utf-8'

        if result.status_code == 201:

            time_to_wait = int(result.headers['retryIn'])
            print("Offline report. Waiting " + result.headers['retryIn'] + "seconds")
            sleep(time_to_wait)

            result = requests.post(url, json_body, headers=header)
            result.encoding = 'utf-8'

            if result.status_code == 202:
                while result.status_code == 202:
                    time_to_wait = int(result.headers['retryIn'])
                    print("Offline report. Waiting " + result.headers['retryIn'] + " seconds")
                    sleep(time_to_wait)

                    result = requests.post(url, json_body, headers=header)
                    result.encoding = 'utf-8'

                return result.text

        elif result.status_code == 400:
            self.exception(result)
        elif result.status_code == 500:
            print("Server is unavaliable. Try again later or contact Yandex Support")
        else:
            return result.text


if __name__ == "__main__":
    ydir = YandexDirect(TOKEN)
    campaigns = ydir.get_campaigns()
    campaigns_df = pd.DataFrame(campaigns).head()
    print(campaigns_df)

    report = ydir.get_report(report_type="CAMPAIGN_PERFORMANCE_REPORT",
                             field_names=[
                                 "Date",
                                 "CampaignId",
                                 "CampaignName",
                                 "LocationOfPresenceName",
                                 "Impressions",
                                 "Clicks",
                                 "Cost"],
                             date_range_type="CUSTOM_DATE",
                             date_from="2020-10-01",
                             date_to="2020-10-02",
                             include_vat="YES"
                             )
    report_df = data = pd.read_csv(StringIO(report), sep='\t')
    print(report_df)
