import json
import urllib
import logging
import asyncio
import hashlib
import binascii
import tornado.gen
from Crypto.Cipher import AES
from tornado.httpclient import AsyncHTTPClient

logger = logging.getLogger()

class E360SemService(object):
    def __init__(self, username=None, password=None, apiKey=None, apiSecret=None):
        self.username = username
        self.password = password
        self.apiKey = apiKey
        self.apiSecret = apiSecret

    async def get_access_token(self):
        username = urllib.parse.quote(self.username)
        passwd = aes_encrypt(self.apiSecret, self.password)
        passwd = urllib.parse.quote(passwd)
        payload = "username={username}&passwd={passwd}&format=json".format(**{"username":username, "passwd":passwd})
        headers = {
            "apiKey": self.apiKey,
            "content-type": "application/x-www-form-urlencoded",
            }
        client = AsyncHTTPClient()
        url = "https://api.e.360.cn/account/clientLogin"
        r = await client.fetch(url, method="POST", headers=headers, body=payload)
        res = json.loads(r.body.decode("utf-8"))
        return res



    async def __execute(self, service, accessToken, method, request_body):
        if request_body is None:
            request_body = "format=json"
        url = "https://api.e.360.cn/2.0/" + service + "/" + method

        headers = {
                "apiKey": self.apiKey,
                "accessToken": accessToken,
                "content-type": "application/x-www-form-urlencoded",
                            }
        logger.info("Request body 360: %s" % request_body)

        client = AsyncHTTPClient()
        r = await client.fetch(url, method="POST", headers=headers, body=request_body)
        return json.loads(r.body.decode("utf-8"))

    async def auth_account(self, request_body=None):
        return await self.get_access_token()

    async def get_report_data(self, accessToken, method, request_body=None):
        return await self.__execute(service="report", accessToken=accessToken, method=method, request_body=request_body)

    async def get_keyword(self, accessToken, request_body=None):
        return await self.__execute(service="keyword", accessToken=accessToken, method="getInfoByIdList", request_body=request_body)

def aes_encrypt(apiSecret, passwd):
    plaintext = hashlib.md5(passwd.encode(encoding="UTF-8")).hexdigest()
    key = apiSecret[:16]
    aes_mode = AES.MODE_CBC
    obj = AES.new(key, aes_mode, apiSecret[16:])
    ciphertext = obj.encrypt(plaintext)
    ciphertext = binascii.b2a_hex(ciphertext)
    ciphertext = str(ciphertext, encoding = "utf8")
    return ciphertext
