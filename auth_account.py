import asyncio
import logging
from _sem_service import E360SemService
logger = logging.getLogger()


class AccountAuth(object):
    def __init__(self, request_params):
        self.f_account = request_params.get("account", None)
        self.f_password = request_params.get("password", None)
        self.apiKey = request_params.get("apiKey", None)
        self.apiSecret = request_params.get("apiSecret", None)

    async def auth_account(self):
        res_code = 0
        res_message = None

        try:
            sem_service = E360SemService(self.f_account, self.f_password, self.apiKey, self.apiSecret)
            res = await sem_service.auth_account()
            if "failures" in res:
                res_code = 2100
                res_message = "Failed to auth with 360 server, user info is NOT matched"
            else:
                res_code = 2000
                res_message = "Succeed to auth with 360 server."
        except Exception as e:
            res_code = 2101
            res_message = ("Exception is thrown during authing: %s" % str(e))
        finally:
            logger.info(res_message)
            print(res_code, res_message)
            return {
                "status": res_code,
                "message": res_message
            }

