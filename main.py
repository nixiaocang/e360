import asyncio
from auth_account import AccountAuth
from campaign_report import CampaignReport

if __name__ == "__main__":
    request_params = {
            "account":"北京铂金智慧",
            "password":"td360**",
            "pt_email":"1234@qq.com",
            "pt_company_id":"1234abcd",
            "pt_source":"e360",
            "pt_data_from_date":"2018-07-21",
            "pt_data_to_date":"2018-07-21",
            "apiKey":"ea0d3ab84a98e43100022282b223c44b",
            "apiSecret":"f37a3e8a1ceb222b492b9b4a108e3bc4",
            }
    loop = asyncio.get_event_loop()
    tasks = [
    asyncio.ensure_future(AccountAuth(request_params).auth_account()),
    asyncio.ensure_future(CampaignReport(request_params).get_data())
    ]
    loop.run_until_complete(asyncio.wait(tasks))
    print("all report get finish")
