import json
import asyncio
import logging
from io import StringIO
from tornado.httpclient import AsyncHTTPClient
import pandas as pd
import tornado.gen
logger = logging.getLogger()


class ReportBase(object):
    @staticmethod
    async def get_report_data(sem_service, method, payload):
        logger.info("########### Ready to get report data ###########")
        auth_res = await sem_service.get_access_token()
        if auth_res.get("accessToken") is None:
            raise Exception("user info auth failure")
        accessToken = auth_res['accessToken']
        f_account_id = auth_res['uid']
        data_info = {}
        field = method + "List"
        for device in ['computer', 'mobile']:
            request_body = payload + device

            logger.info("########### Start to get_report_data, device: %s ###########" % device)
            page_info = await sem_service.get_report_data(accessToken, method+"Count", request_body)
            if "failures" in page_info:
                raise Exception("获取report data失败：%s" % page_info['failures'][0]['message'])
            pages = page_info["totalPage"]
            result = []
            for page in range(pages):
                temp_request_body = request_body + "&page=" + str(page+1)
                res = await sem_service.get_report_data(accessToken, method, temp_request_body)
                if "failures" in res:
                    raise Exception("获取report data失败：%s" % res['failures'][0]['message'])
                result += res[field]

            logger.info("########### Finished to get_report_data ###########")
            data_info[device] = pd.read_json(json.dumps(result))
            data_info[device]['type'] = device
            data_info[device]['f_account_id'] = f_account_id
        _report_data = pd.concat([data_info["computer"], data_info["mobile"]])
        return {
            'length': len(_report_data),
            'report_data': _report_data,
            'retry_times': 0
        }

    @staticmethod
    def convert_sem_data_to_pt(fres, f_source, f_company_id, f_email, fmap, f_account):
        '''
            注意：
            此处只是数据源的原始字段转换为ptming的入库字段，不做字段特殊值处理
            (比如--，null等转换为'')，每个接口的字段特殊处理在每个业务接口中单独处理

        '''
        fres['f_source'] = f_source
        fres['f_company_id'] = f_company_id
        fres['f_email'] = f_email
        fres['f_account'] = f_account

        cols = [col for col in fres]
        new_cols = []
        for col in cols:
            if col not in fmap.keys():
                del fres[col]
            else:
                new_cols.append(fmap[col])
        fres.columns = new_cols
        return fres
