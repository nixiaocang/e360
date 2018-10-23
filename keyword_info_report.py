import asyncio
import logging
import logging.config
import time
import json
import datetime
import pandas as pd
from _sem_service import E360SemService
from _report_base import ReportBase
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger()


'''
    每个接口中需要转换的字段映射表，都统一放在单独的业务接口中
'''
fmap = {
        "f_source": "f_source",
        "f_company_id": "f_company_id",
        "f_email": "f_email",
        "date": "f_date",
        "f_account": "f_account",
        "f_account_id": "f_account_id",
        "type": "f_device",
        "id": "f_keyword_id",
        "f_campaign_id": "f_campaign_id",
        "groupId": "f_set_id",
        "word": "f_keyword",
        "price": "f_keyword_offer_price",
        "destinationUrl": "f_pc_url",
        "mobileDestinationUrl": "f_mobile_url",
        "matchType": "f_matched_type",
    }


class KeywordInfoReport(ReportBase):
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
                res_code, res_message = await self._get_and_save_report_data_to_db(sem_service, "keyword",
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
        auth_res = await sem_service.get_access_token()
        if auth_res.get("accessToken") is None:
            raise Exception("user info auth failure")
        accessToken = auth_res['accessToken']
        f_account_id = auth_res['uid']

        start = time.time()
        get_report_data_res = await ReportBase.get_report_data(sem_service, method, payload)
        df = get_report_data_res.get("report_data", None)
        dates = pd.date_range(self.f_from_date, self.f_to_date)
        data = []
        for date in dates:
            date = str(date)[:10]
            tdf = df[df['date']==date]
            for device in ["mobile", "computer"]:
                ttdf = tdf[tdf['type']==device]
                ids = ttdf["keywordId"].tolist()
                ids = list(set(ids))
                ids = json.dumps(ids)
                payload = "format=json&idList=" + ids
                res = await sem_service.get_keyword(accessToken, request_body=payload)
                sub_data = res["keywordList"]
                for sub in sub_data:
                    sub["type"] = device
                    sub["date"] = date
                data += sub_data
        cost = time.time()-start
        print("keyword_info耗时==> %s s" % cost)
        '''
            计算获取数据用时
        '''

        '''

            1. 如过该接口返回的数据中包含特殊值，比如--, null, 空，请在此处转换成接口文档中的默认值
            2. 清洗完数据之后，到此返回数据即可，数据可以缓存在csv文件中。
        '''
        report_data = pd.read_json(json.dumps(data))
        tdf = df[['campaignId','keywordId']]
        tdf.rename(columns={'keywordId':'id'}, inplace = True)
        report_data = pd.merge(report_data, tdf, on='id')
        report_data['f_account_id'] = f_account_id
        fres = ReportBase.convert_sem_data_to_pt(report_data, self.f_source, self.f_company_id, self.f_email, fmap, self.f_account)
        fres.to_csv("csv/keyword_info.csv")
        return 2000, "OK"
