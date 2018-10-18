#  api server说明文档
### 依赖环境
```
python 3.6.2
tornado 5.1.1
```
### 部署说明
* 切换到项目根目录下: `cd $project`
* 安装第三方依赖包: `pip3 install -r requirements.txt`
* 创建文件夹: mkdir csv
* 执行测试:python3 main.py



### 目录结构
```
├── auth_account.py    账号信息验证接口
├── campaign_report.py 计划报告接口
├── creative_report.py 创意报告接口
├── csv
├── __init__.py
├── keyword_info_report.py 关键词信息接口
├── keyword_report.py  关键词报告接口
├── main.py            测试入库
├── README.md
├── region_report.py  市级地域报告接口
├── _report_base.py   基础报告
├── requirements.txt  
├── search_report.py  搜索词报告接口
└── _sem_service.py   核心服务

```

### 特殊说明
```
报告的获取时间限制为最近90天
```
