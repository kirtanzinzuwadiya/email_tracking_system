from scrapy import Selector
from curl_cffi import requests
import pymysql
import pandas as pd
from sqlalchemy import create_engine
cookies = {
    '_optux_rwRunkFTSC': '0',
    '_optux_hZt2EmOzAa': '0',
    '_optux_4y3StPsC8c': '1035',
    '_optux_H18az1Zeo6': '1114',
    '_optux_9JOynI5LqF': '1070',
    '_optux_8QWpBTUraG': '0',
    '_optux_aQSjCAQOcg': '1230',
    '_optux_larMHNUpBv': '0',
    '_optux_wpGjy0KnwY': '0',
    '_optux_uid': 'OUX-6C732697-537D-4FAB-9CB4-EA11607EAECA',
    'CookieConsent': '{stamp:%27FhyWH/RQv942XwmtK4//qossuo55S6703JaEN6VYC9BZdP1rJrv2HA==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27implied%27%2Cver:1%2Cutc:1743879692441%2Cregion:%27in%27}',
    '_ga': 'GA1.1.1098711821.1743879694',
    '_gcl_au': '1.1.1461837549.1743879694',
    'FPID': 'FPID2.2.lZBCJolHZqJUsz0FdreHovRD0t2bljl6bNwfKl16j5o%3D.1743879694',
    'FPAU': '1.2.483285771.1743879695',
    'hubspotutk': 'df3f8880645c182cb03a77a39b4b276b',
    '_optux_1c2n3Y5brU': '0',
    '_optux_OpB0Y5BcHJ': '0',
    '_optux_yu71wQpz4S': '0',
    '_optux_hBiwbRj51i': '0',
    '_optux_57IsNxxPSo': '0',
    '_optux_kyPdiWhdXR': '0',
    '_optux_3CrwJZ5dKs': '0',
    '_optux_PceOPUQTq8': '0',
    '_optux_IQltwhjAkO': '0',
    '_optux_q4VAkUX2cS': '0',
    '_optux_SSsQk97P7D': '0',
    '_optux_sYAAv4WMOr': '0',
    '_optux_crKlq6362q': '0',
    '_optux_um7jmjeKSh': '0',
    '_optux_jt1JCKjfK7': '0',
    '_optux_MRACDjBQZW': '0',
    '_optux_2gUtUrG8jG': '0',
    '_optux_CahSKC6lQ0': '0',
    '_optux_lzbwbXpfcU': '0',
    '_optux_aWFDmJ5ig8': '0',
    '_optux_NPQJJ726cQ': '0',
    '_optux_U1EEfLOY29': '0',
    '_optux_1H8xmEw2Qv': '0',
    '_optux_4vfM6dFzN6': '0',
    '_optux_5obstjEyZy': '0',
    '_optux_fLbiOvjLKJ': '0',
    '_optux_qYj55rEjHO': '0',
    '_optux_9dIpFORU6i': '0',
    '_optux_fEAw5gH52U': '0',
    '_optux_7MV4u4AqBp': '0',
    '_optux_mHrGQBPVnL': '0',
    '_optux_fpVBxpwqwB': '0',
    '_optux_08hca0Zb6F': '0',
    '_optux_9JQtfxRXpB': '0',
    '_optux_NMaVTWTuJO': '0',
    '_optux_KyJ1UR5rR4': '0',
    '_optux_yTlyNHl84n': '0',
    '_optux_TAs1LWXKVk': '0',
    '_optux_4xSiYslFgs': '0',
    '_optux_u2A8shUGTl': '0',
    '_optux_sVL0yFLmRH': '0',
    '_optux_OV5sW0Z6YV': '0',
    'FPLC': 'IPhdslaXRwLbxubo9nbwHlLjCssgVw7zFlCWeVt4d7sTvBftypxeWkXfSki7WnpwanjvN%2B%2FmjZ4ovpqGJlf9ShQpM2fiwpdMq7RP9HbCwHy4Ed18f8tR%2F6WgZdRGlQ%3D%3D',
    '__hssrc': '1',
    '__csrftoken': 'ea2f57c145c6eb1e7f6864fa03e1e1d651e93771f9bf54a76e4e0e9ae94726f8e4eb26888a278c7e120851d27c37536756f603e926eeff475f2f45d5bc82e049|1744199880144',
    'cf_clearance': 'hIwaAZLajF4JatWc4flQ3LSvMBc_OcUKetUg7PULxa0-1744188259-1.2.1.1-qRzWBX.07.XR.IXuHv2Ti8y_K_7FBXZ_kFCnGx2mVu8kIGeX2u9.faEvQ.4cXK9238Q.kOI08uN9E2wKNNtWcyIwk9DEJw8kP2znugGwn5cmvKZBj0gBK5ni39eNl_BnOLRSL7Xiu8q6UAUwBMZLYuoC9povwmhfdso6s1oWehh8M_MyJJ8GKltn.yDIKFp.UGyel2FeIIVnWBSFYgrQZHKxczP3r_74pAhvPaeJ9.wMNa.Ssnb1wqI0wXC78OiZHdwxkM41MdY5L3MzvciFOY9Yf8LulC8bv18umMfofTDS0foGES1lqDIV1pkB_5JmwMIlyTrU6I9JXRhfcvAD_QqvCRPZIegPDwrlmiph8NQ',
    '_hp2_props.1079324124': '%7B%22analyticjs%22%3Atrue%2C%22category%22%3A%22IT%20Services%22%2C%22content_group%22%3A%22directory%22%2C%22debug_mode%22%3Afalse%2C%22page_canonical_id%22%3A1058022600000%2C%22page_number%22%3A0%2C%22page_type%22%3A%22directory%22%2C%22trace_id%22%3A%2292d8b8b56baf46c5%22%2C%22transport_url%22%3A%22https%3A%2F%2Fg.clutch.co%22%2C%22bot%22%3A%22%22%7D',
    '__hstc': '238368351.df3f8880645c182cb03a77a39b4b276b.1743879708504.1744140442871.1744188270842.3',
    'seen-sign-in-tooltip': 'true',
    '_hp2_id.1079324124': '%7B%22userId%22%3A%225739182946439688%22%2C%22pageviewId%22%3A%225619497488068790%22%2C%22sessionId%22%3A%224044693386585222%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D',
    '_ga_D0WFGX8X3V': 'GS1.1.1744188258.3.1.1744189095.60.0.483936931',
    '__cf_bm': 'tFOOaS1eWCWUzMOCdP_gDeyobbZw.vgI4oQSdXmRXRQ-1744190457-1.0.1.1-wXimvdVElfJzh8WBE7MPkcBJPKJi.FyWqTmSycBumxq0PELJyZ8E5lrwlYoXxlsvWh4HxHZ1CJZDwTwCD8fXmri54_tzsV2kj6JwG6HskEg',
}
headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    # 'cookie': '_optux_rwRunkFTSC=0; _optux_hZt2EmOzAa=0; _optux_4y3StPsC8c=1035; _optux_H18az1Zeo6=1114; _optux_9JOynI5LqF=1070; _optux_8QWpBTUraG=0; _optux_aQSjCAQOcg=1230; _optux_larMHNUpBv=0; _optux_wpGjy0KnwY=0; _optux_uid=OUX-6C732697-537D-4FAB-9CB4-EA11607EAECA; CookieConsent={stamp:%27FhyWH/RQv942XwmtK4//qossuo55S6703JaEN6VYC9BZdP1rJrv2HA==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27implied%27%2Cver:1%2Cutc:1743879692441%2Cregion:%27in%27}; _ga=GA1.1.1098711821.1743879694; _gcl_au=1.1.1461837549.1743879694; FPID=FPID2.2.lZBCJolHZqJUsz0FdreHovRD0t2bljl6bNwfKl16j5o%3D.1743879694; FPAU=1.2.483285771.1743879695; hubspotutk=df3f8880645c182cb03a77a39b4b276b; _optux_1c2n3Y5brU=0; _optux_OpB0Y5BcHJ=0; _optux_yu71wQpz4S=0; _optux_hBiwbRj51i=0; _optux_57IsNxxPSo=0; _optux_kyPdiWhdXR=0; _optux_3CrwJZ5dKs=0; _optux_PceOPUQTq8=0; _optux_IQltwhjAkO=0; _optux_q4VAkUX2cS=0; _optux_SSsQk97P7D=0; _optux_sYAAv4WMOr=0; _optux_crKlq6362q=0; _optux_um7jmjeKSh=0; _optux_jt1JCKjfK7=0; _optux_MRACDjBQZW=0; _optux_2gUtUrG8jG=0; _optux_CahSKC6lQ0=0; _optux_lzbwbXpfcU=0; _optux_aWFDmJ5ig8=0; _optux_NPQJJ726cQ=0; _optux_U1EEfLOY29=0; _optux_1H8xmEw2Qv=0; _optux_4vfM6dFzN6=0; _optux_5obstjEyZy=0; _optux_fLbiOvjLKJ=0; _optux_qYj55rEjHO=0; _optux_9dIpFORU6i=0; _optux_fEAw5gH52U=0; _optux_7MV4u4AqBp=0; _optux_mHrGQBPVnL=0; _optux_fpVBxpwqwB=0; _optux_08hca0Zb6F=0; _optux_9JQtfxRXpB=0; _optux_NMaVTWTuJO=0; _optux_KyJ1UR5rR4=0; _optux_yTlyNHl84n=0; _optux_TAs1LWXKVk=0; _optux_4xSiYslFgs=0; _optux_u2A8shUGTl=0; _optux_sVL0yFLmRH=0; _optux_OV5sW0Z6YV=0; FPLC=IPhdslaXRwLbxubo9nbwHlLjCssgVw7zFlCWeVt4d7sTvBftypxeWkXfSki7WnpwanjvN%2B%2FmjZ4ovpqGJlf9ShQpM2fiwpdMq7RP9HbCwHy4Ed18f8tR%2F6WgZdRGlQ%3D%3D; __hssrc=1; __csrftoken=ea2f57c145c6eb1e7f6864fa03e1e1d651e93771f9bf54a76e4e0e9ae94726f8e4eb26888a278c7e120851d27c37536756f603e926eeff475f2f45d5bc82e049|1744199880144; cf_clearance=hIwaAZLajF4JatWc4flQ3LSvMBc_OcUKetUg7PULxa0-1744188259-1.2.1.1-qRzWBX.07.XR.IXuHv2Ti8y_K_7FBXZ_kFCnGx2mVu8kIGeX2u9.faEvQ.4cXK9238Q.kOI08uN9E2wKNNtWcyIwk9DEJw8kP2znugGwn5cmvKZBj0gBK5ni39eNl_BnOLRSL7Xiu8q6UAUwBMZLYuoC9povwmhfdso6s1oWehh8M_MyJJ8GKltn.yDIKFp.UGyel2FeIIVnWBSFYgrQZHKxczP3r_74pAhvPaeJ9.wMNa.Ssnb1wqI0wXC78OiZHdwxkM41MdY5L3MzvciFOY9Yf8LulC8bv18umMfofTDS0foGES1lqDIV1pkB_5JmwMIlyTrU6I9JXRhfcvAD_QqvCRPZIegPDwrlmiph8NQ; _hp2_props.1079324124=%7B%22analyticjs%22%3Atrue%2C%22category%22%3A%22IT%20Services%22%2C%22content_group%22%3A%22directory%22%2C%22debug_mode%22%3Afalse%2C%22page_canonical_id%22%3A1058022600000%2C%22page_number%22%3A0%2C%22page_type%22%3A%22directory%22%2C%22trace_id%22%3A%2292d8b8b56baf46c5%22%2C%22transport_url%22%3A%22https%3A%2F%2Fg.clutch.co%22%2C%22bot%22%3A%22%22%7D; __hstc=238368351.df3f8880645c182cb03a77a39b4b276b.1743879708504.1744140442871.1744188270842.3; seen-sign-in-tooltip=true; _hp2_id.1079324124=%7B%22userId%22%3A%225739182946439688%22%2C%22pageviewId%22%3A%225619497488068790%22%2C%22sessionId%22%3A%224044693386585222%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; _ga_D0WFGX8X3V=GS1.1.1744188258.3.1.1744189095.60.0.483936931; __cf_bm=tFOOaS1eWCWUzMOCdP_gDeyobbZw.vgI4oQSdXmRXRQ-1744190457-1.0.1.1-wXimvdVElfJzh8WBE7MPkcBJPKJi.FyWqTmSycBumxq0PELJyZ8E5lrwlYoXxlsvWh4HxHZ1CJZDwTwCD8fXmri54_tzsV2kj6JwG6HskEg',
}
a = requests.get("https://clutch.co/in/it-services/ahmedabad?industries=field_pp_if_it&related_services=field_pp_sl_it_strategy2", headers=headers, cookies=cookies)
b = a.status_code
response = Selector(text=a.text)
name = response.xpath('//h3[@class="provider__title"]/a/text()').getall()
name_lst = []
for i in name:
    new = i.strip()
    name_lst.append(new)

print(name_lst)
print(len(name_lst))
size = response.xpath('//div[@class="provider__highlights-item sg-tooltip-v2 employees-count"]//text()').getall()
size_lst = []
for j in size:
    n = j.strip()
    if n:
        size_lst.append(n)
print(size_lst)
print(len(size_lst))
location = response.xpath('//div[@class="provider__highlights-item sg-tooltip-v2 location"]/text()').getall()
location_lst = []
for k in location:
    n = k.strip()
    if n:
        location_lst.append(n)
print(location_lst)
print(len(location_lst))


host = "localhost"
user = "root"
password = "kz#1212"
database = "linkedin"

my_dict = {"name":name_lst,"location":location_lst,"size":size_lst,"industry":'Information Technology'}
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

print("1")
df = pd.DataFrame(my_dict)
print(2)
df.to_sql('data2',con=engine,if_exists='append',index=False)



