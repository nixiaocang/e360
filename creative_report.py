import json
import asyncio
import logging
import logging.config
import time
import datetime
import pandas as pd
from _sem_service import E360SemService
from _report_base import ReportBase

logger = logging.getLogger()


'''
    每个接口中需要转换的字段映射表，都统一放在单独的业务接口中
'''
fmap = {
        "f_source": "f_source",
        "f_company_id": "f_company_id",
        "f_email": "f_email",
        "f_account": "f_account",
        "f_account_id": "f_account_id",
        "date": "f_date",
        "campaignName": "f_campaign",
        "campaignId": "f_campaign_id",
        "groupName": "f_set",
        "groupId": "f_set_id",
        "clicks": "f_click_count",
        "totalCost": "f_cost",
        "views": "f_impression_count",
        "creativeId": "f_creative_id",
        "title": "f_creative_title",
        "type": "f_device",
        "destinationUrl":"f_creative_visit_url",
        "mobileDestinationUrl":"f_creative_mob_visit_url",
    }


class CreativeReport(ReportBase):
    def __init__(self, request_params):
        self.f_account = request_params.get("account", None)
        self.f_password = request_params.get("password", None)
        self.f_email = request_params.get("pt_email", None)
        self.f_company_id = request_params.get("pt_company_id", None)
        self.f_source = request_params.get("pt_source", None)
        self.f_from_date = request_params.get("pt_data_from_date", None)
        self.f_to_date = request_params.get("pt_data_to_date", None)
        self.apiKey = request_params.get("apiKey", None)
        self.apiSecret = request_params.get("apiSecret", None)


    async def get_data(self):
        res_code = 0
        res_message = None

        try:
            if self.f_account is None \
                    or self.f_password is None \
                    or self.f_email is None \
                    or self.f_company_id is None \
                    or self.f_source is None \
                    or self.apiKey is None \
                    or self.apiSecret is None:
                res_code = 2100
                res_message = "Failed to get_report_data, account or password or apiKey or apiSecret is missed"
            else:
                sem_service = E360SemService(self.f_account, self.f_password, self.apiKey, self.apiSecret)

                if self.f_from_date is None or self.f_to_date is None:
                    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))
                    start_date = yesterday
                    end_date = yesterday
                else:
                    start_date = self.f_from_date
                    end_date = self.f_to_date
                payload = "startDate=%s&endDate=%s&level=account&format=json&type=" % (start_date, end_date)
                res_code, res_message = await self._get_and_save_report_data_to_db(sem_service, "creative",
                                                                                   payload)
        except Exception as e:
            res_code = 2101
            res_message = ("Exception is thrown during get_report_data : %s" % str(e))
        finally:
            logger.info(res_message)
            print(res_code, res_message)
            return {
                "status": res_code,
                "message": res_message
            }

    async def _get_and_save_report_data_to_db(self, sem_service, method, payload):
        '''
            开始获取数据计时
        '''
        start = time.time()
        get_report_data_res = await ReportBase.get_report_data(sem_service, method, payload)
        cost = time.time()-start
        print("%s耗时==> %s s" % (method, cost))
        '''
            计算获取数据用时
        '''
        report_data_length = get_report_data_res.get("length", None)
        report_data = get_report_data_res.get("report_data", None)
        retry_times = get_report_data_res.get("retry_times", None)
        # get_creative
        auth_res = await sem_service.get_access_token()
        accessToken = auth_res['accessToken']
        ids = report_data["creativeId"].tolist()
        ids = list(set(ids))
        data = []
        for i in range(0, len(ids), 1000):
            temp = ids[i:i+1000]
            idList = json.dumps(temp)
            payload = "format=json&idList=" + idList
            res = await sem_service.get_creative(accessToken, request_body=payload)
            sub_data = res["creativeList"]
            data += sub_data
        df = pd.read_json(json.dumps(data))
        df.rename(columns={'id':'creativeId'},inplace=True)
        report_data = pd.merge(report_data, df, how='left', on=['creativeId'])

        '''

            1. 如过该接口返回的数据中包含特殊值，比如--, null, 空，请在此处转换成接口文档中的默认值
            2. 清洗完数据之后，到此返回数据即可，数据可以缓存在csv文件中。
        '''
        fres = ReportBase.convert_sem_data_to_pt(report_data, self.f_source, self.f_company_id, self.f_email, fmap, self.f_account)
        fres.to_csv("csv/%s.csv" % method)
        return 2000, "OK"
