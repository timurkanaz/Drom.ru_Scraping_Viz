#!/usr/bin/env python
# coding: utf-8

# In[1]:


alert_freq=10 # How frequent messages should appear
n_proxies=8 # Number of proxies
multiplicator=7 # Number of threads for each proxy ip
pause=7 # How much time to sleep
threads_models=4


# In[2]:


proxies=["http://login:password@ip:port"
,"http://login:password@ip:port"
,"http://login:password@ip:port"
,"http://login:password@ip:port"
,"http://login:password@ip:port"
,"http://login:password@ip:port"
,"http://login:password@ip:port"
,"http://login:password@ip:port"]
proxies_dict=proxies*multiplicator
proxies_dict=[{'https':i} for i in proxies_dict]


# In[3]:


import requests as r
import pandas as pd
from IPython.display import clear_output
from bs4 import BeautifulSoup
import time
import random
import urllib3
import urllib
import os
import shutil
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import re
from numpy import random, array_split
from multiprocessing.pool import ThreadPool


desktop_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0',
                 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'
                 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.135 YaBrowser/21.6.3.757 Yowser/2.5 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 YaBrowser/21.6.4.786 Yowser/2.5 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36 OPR/74.0.3911.107',
                 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15',
                 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.18 Safari/537.36 OPR/55.0.2962.0 (Edition developer)',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.152 YaBrowser/21.2.2.102 Yowser/2.5 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.135 YaBrowser/21.6.2.855 Yowser/2.5 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 YaBrowser/20.12.3.138 Yowser/2.5 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 YaBrowser/20.8.3.112 Yowser/2.5 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3343.4 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.170 Safari/537.36 OPR/53.0.2907.68',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Safari/605.1.15',
                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 YaBrowser/20.8.2.92 Yowser/2.5 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36 OPR/73.0.3856.344',
                 'Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201',
                 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:59.0) Gecko/20100101 Firefox/59.0',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36 OPR/74.0.3911.160',
                 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36 Edg/87.0.664.60',
                 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.101 YaBrowser/20.7.0.899 Yowser/2.5 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; ) AppleWebKit/53']


# In[4]:


def beautiful_html(resp):
    return BeautifulSoup(resp.text,"html.parser")


# In[5]:


def get_regions():
    u=0
    while u==0:
        try:
            raw_data=r.get("https://www.drom.ru/my_region/",proxies=urllib.request.getproxies(),headers={"User-Agent":random.choice(desktop_agents)},verify=False)
            data=beautiful_html(raw_data)
            regions=[]
            for link in data.find_all('a',{'class':'b-link regionLink'},href=True):
                regions.append((link.text.strip(),re.search('=(.+)',link['href']).group(1)))
            u=1
        except:
            time.sleep(60)            
    print("Регионы собраны")
    return regions


# In[6]:


def get_models(tuples):
    print(f"Info about models in all regions. Thread {tuples[0]}")
    model_hrefs=[]
    with r.Session() as s:
        for tup in tuples[1]:
            to=0
            u=0
            length=0
            while u==0:
                try:
                    raw_data=s.get(f"https://auto.drom.ru/region{int(tup[1])}/?grouping=1&ph=1&unsold=1#tabs",headers={"User-Agent":random.choice(desktop_agents)},verify=False)
                    data=beautiful_html(raw_data)
                    if raw_data.reason=='OK': 
                        distinct_models=int(re.findall('.+?(?=модел)',data.find_all('a',{"href":f"https://auto.drom.ru/region{int(tup[1])}/?grouping=1&ph=1&unsold=1#tabs"})[0].text.replace('\xa0',u' '))[0].replace(' ',''))
                        number_of_pages=distinct_models//20
                        if  (distinct_models - number_of_pages*20)>0:
                            number_of_pages+=1
                        u=1
                    else:
                        print(f"Thread-{tuples[0]},{tup[1]},не подходящий ответ")
                        to+=1
                        if to%150==0:
                            time.sleep(7)
                        else:
                            time.sleep(3)
                   
                except:
                    print(f"Thread-{tuples[0]}, {tup[1]} ,не удался get запрос")
                    to+=1
                    if to%150==0:
                        time.sleep(7)
                    else:
                        time.sleep(3)
            page=1
            to=0
            try:
                while page<=number_of_pages:
                    u=0
                    while u==0:
                        try:
                            raw_data=s.get(f"https://auto.drom.ru/region{int(tup[1])}/page{int(page)}/?grouping=1&ph=1&unsold=1",headers={"User-Agent":random.choice(desktop_agents)},verify=False)
                            data=beautiful_html(raw_data)
                            if len(data.find_all('div',{"data-app-root":"bulls-list-models-range"})[0].find_all('a',{'class':"css-ew3ldr e13zubtn5"},href=True))==0:  #Class is changing
                                to+=1
                                print(f"Thread-{tuples[0]} , {f'https://auto.drom.ru/region{int(tup[1])}/page{int(page)}/?grouping=1&ph=1&unsold=1'} ,нет ссылок")
                                if to%150==0:
                                    time.sleep(7)
                                else:
                                    time.sleep(3)
                            else:
                               
                                u=1
                        except:
                            print(f"Thread-{tuples[0]} , {f'https://auto.drom.ru/region{int(tup[1])}/page{int(page)}/?grouping=1&ph=1&unsold=1'} ,не удался get запрос")
                            to+=1
                            if to%150==0:
                                time.sleep(7)
                            else:
                                time.sleep(3)
                    hrefs=[(i['href'],re.findall('.+?(?=,)',i.text)[0],tup[0],tup[1]) for i in data.find_all('div',{"data-app-root":"bulls-list-models-range"})[0].find_all('a',{'class':"css-ew3ldr e13zubtn5"},href=True)] #Class is changing
                    for val in hrefs:
                        model_hrefs.append(val)
                    length+=len(hrefs)
                    page+=1
                print(f"{tup[1]},{tup[0]},pages: {number_of_pages}  models: {length} ")
            except:
                print(f"Thread-{tuples[0]} , {tup[0]} ,не удалось перейти по ссылке")
                print(f"{tup[1]},{tup[0]}")
    model_hrefs=set(model_hrefs)
    return model_hrefs


# In[7]:


def get_ad_info(tuples,s):
    #print(f"Collecting all ads hrefs.Thread {tuples[0]}")
    ads_info=[]
    for ind,tup in enumerate(tuples[1]):
        u=0
        to=0
        region_code=tup[-1]
        region=tup[-2]
        model=tup[-3]
        while u==0:
            try:
                raw_data=s.get(f"{tup[0]}",headers={"User-Agent":random.choice(desktop_agents)},verify=False)
                data=beautiful_html(raw_data)
                if 'Запрошенная вами страница не существует!' in raw_data.text:
                    break
                elif 'Подать объявление' not in raw_data.text:
                    raise Exception   
                elif len([i.text.replace("\xa0"," ") for i in data.find_all("table",{"class":"css-xalqz7 eo7fo180"})[0].find_all("td",{"class":"css-1azz3as eka0pcn0"})])>0: 
                    u=1
                else:
                    to+=1
                    if to%150==0:
                        time.sleep(pause*3)
                        print(f"{tup[0]} - не пройденных запросов : {to} ")
                    else:
                        time.sleep(pause)
            except:
                to+=1
                if to%150==0:
                    time.sleep(pause*3)
                    print(f"{tup[0]} - не пройденных запросов : {to} ")
                else:
                    time.sleep(pause)
            
        p=0
        try:
            l2=[i.text.replace("\xa0"," ") for i in data.find_all("table",{"class":"css-xalqz7 eo7fo180"})[0].find_all('th',{'class':'css-1dzcqnh eka0pcn1'})] #All classes are changing!
            l1=[i.text.replace("\xa0"," ") for i in data.find_all("table",{"class":"css-xalqz7 eo7fo180"})[0].find_all('td',{'class':'css-1azz3as eka0pcn0'})] #All classes are changing!
        except:
            print(f"{tup[0]} - невалидная ссылка. {raw_data.reason}")
#                print(raw_data.text)
#                print(raw_data)
#                print(data)
            p=1
        if p==0:
            dct=dict(zip(l2,l1))
            city=data.find_all("div",{"class":"css-inmjwf e162wx9x0"})[-1].text
            idd=re.search("\/(\d+).html",tup[0]).group(1)
            ads_info.append((region,model,region_code,idd,city,dct))
        #print(f"Thread: {tuples[0]}, {ind+1}/{len(tuples[1])}")
    return ads_info


# In[8]:


def models_exists(data):
    try:
        k=re.findall(".+?(?= объяв)",data.find_all("div",{"class":"css-1xkq48l eckkbc90"})[0].text)[0].replace('\xa0',u' ').replace(' ','') #Class is changing!
        return 1
    except:
        return -1


# In[9]:


def get_ads_hrefs(tuples):
    #print(f"Collecting all ads hrefs.Thread {tuples[0]}")
    region=[]
    model=[]
    region_code=[]
    idc=[]
    city=[]
    engine=[]
    power=[]
    transmission=[]
    drive=[]
    body_type=[]
    color=[]
    mileage=[]
    swheel=[]
    
    dupl=0
    with r.Session() as s:
        s.proxies=tuples[2]
        for ind,tup in enumerate(tuples[1]):
            if (ind+1)%alert_freq==0:
                print(f"Обработка {tup[0]}, Thread: {tuples[0]}")
            to=0
            u=0
            length=0
            while u==0:
                try:
                    raw_data=s.get(f"{tup[0]}",headers={"User-Agent":random.choice(desktop_agents)},verify=False)
                    data=beautiful_html(raw_data)
                    p=models_exists(data)
                    if ('Запрошенная вами страница не существует!' in raw_data.text):
                        number_of_pages=0
                        break
                    if raw_data.reason=='OK' and p==1: 
                        distinct_ads=int(re.findall(".+?(?= объяв)",data.find_all("div",{"class":"css-nlq3fc edzrckn0","id":"tabs"})[0].text)[0].replace('\xa0',u' ').replace(' ',''))
                        number_of_pages=distinct_ads//20
                        if  (distinct_ads - number_of_pages*20)>0:
                            number_of_pages+=1
                        u=1
                    elif 'Подать объявление' not in raw_data.text:
                        raise Exception
                    elif p==-1:
                        number_of_pages=0
                        break
                    else:
                        to+=1
                        if to%150==0:
                            print(f"{tup[0]} не пройденных запросов : {to} ")
                            time.sleep(pause*3)
                        else:
                            time.sleep(pause)
                except:
                    to+=1
                    if to%150==0:
                        print(f"{tup[0]} не пройденных запросов : {to} ")
                        time.sleep(pause*3)
                    else:
                        time.sleep(pause)
            u=0
            page=1
            to=0
            try:
                while page<=number_of_pages:
                    u=0
                    while u==0:
                        try:
                            raw_data=s.get(f"{tup[0].replace('?ph=1&unsold=1','')}page{page}/?unsold=1&ph=1",headers={"User-Agent":random.choice(desktop_agents)},verify=False)
                            data=beautiful_html(raw_data)
                            m=models_exists(data)
                            if ('Запрошенная вами страница не существует!' in raw_data.text):
                                break
                            elif 'Подать объявление' not in raw_data.text:
                                raise Exception
                            elif m==-1:
                                break
                            else:
                                u=1
                        except:
                            to+=1
                            if to%150==0:
                                time.sleep(pause*3)
                                print(f"{tup[0].replace('?ph=1&unsold=1','')}page{page}/?unsold=1&ph=1 - не пройденных запросов: {to}")
                            else:
                                time.sleep(pause)
                    if m==1:
                        hrefs=[(i['href'],tup[0],tup[1],tup[2],tup[3]) for i in data.find_all("div",{"data-bulletin-list":"true"})[0].find_all("a",{"class":"g6gv8w4 g6gv8w8 _1ioeqy90"})] #Class of 'a' element is changing!
                        length+=len(hrefs)
                        for val in hrefs:
                            i=(val,get_ad_info((1,[val]),s))
                            if dupl==0:
                                try:
                                    region.append(i[1][0][0])
                                except:
                                    region.append('No info')
                                try:
                                    model.append(i[1][0][1])
                                except:
                                    model.append('No info')
                                try:
                                    region_code.append(i[1][0][2])
                                except:
                                    region_code.append('No info')
                                try:
                                    idc.append(i[1][0][3])
                                except:
                                    idc.append('No info')
                                try:
                                    city.append(i[1][0][4])
                                except:
                                    city.append('No info')
                                try:
                                    engine.append(i[1][0][5]['Двигатель'])
                                except:
                                    engine.append('No info')
                                try:
                                    power.append(i[1][0][5]['Мощность'])
                                except:
                                    power.append('No info')
                                try:
                                    transmission.append(i[1][0][5]['Коробка передач'])
                                except:
                                    transmission.append('No info')
                                try:
                                    drive.append(i[1][0][5]['Привод'])
                                except:
                                    drive.append('No info')
                                try:
                                    body_type.append(i[1][0][5]['Тип кузова'])
                                except:
                                    body_type.append('No info')
                                try:
                                    color.append(i[1][0][5]['Цвет'])
                                except:
                                    color.append('No info')
                                try:
                                    mileage.append(i[1][0][5]['Пробег'])
                                except:
                                    mileage.append('No info')
                                try:
                                    swheel.append(i[1][0][5]['Руль'])
                                except:
                                    swheel.append('No info')
                            else:
                                pass
                    page+=1
                if (ind+1)%alert_freq==0:
                    print(f"Thread:{tuples[0]} {ind+1}/{len(tuples[1])}, {tup[0]} , {tup[1]}, {tup[2]} , pages: {number_of_pages}  ads:{length} ")
            except:
                print(f"Thread:{tuples[0]} {ind+1}/{len(tuples[1])}")
                pass
    return pd.DataFrame([idc,region,region_code,city,model,body_type,color,swheel,mileage,engine,power,transmission,drive]).T


# In[10]:


def extract_city(city):
    try:
        return re.findall('Город:([-\sа-яА-Я\d]+)',city)[0].strip()
    except:
        return city
    
def extract_brand(name):
    try:
        return re.findall('^([А-Яа-яA-Za-z]+)',name)[0].replace('Great','Great Wall')
    except:
        return name

def extract_mileage(mil):
    try:
        return int(re.findall('([\s0-9]+) км',mil)[0].replace(' ',''))
    except:
        return mil
    
def extract_HP(val):
    try:
        return int(re.findall('([0-9]+) л.с.,',val)[0])
    except:
        return val

def correct_errors(val):
    if val=='Iran':
        return 'Iran Khodro'
    elif val=='KG':
        return 'KG Mobility'
    elif val=='Land':
        return 'Land Rover'
    elif val=='M':
        return 'M-Hero'
    elif val=='No':
        return 'No info'
    elif val=='Rolls':
        return 'Rolls Royce'
    else:
        return val 


# In[11]:


def Drom_Parser():
    regs=get_regions()
    rows_divided=list(array_split(regs,threads_models))
    nums_models=[i+1 for i in range(threads_models)]
    rd=list(zip(nums_models,rows_divided))
    pool=ThreadPool(threads_models)
    l=pool.map(get_models,rd)
    hrefs=[]
    for val in l:
        for val_val in val:
            hrefs.append(val_val) 
    threads_ads=n_proxies*multiplicator
    rows_divided=list(array_split(hrefs,threads_ads))
    nums_ads=[i+1 for i in range(threads_ads)]
    hd=list(zip(nums_ads,rows_divided,proxies_dict))
    pool=ThreadPool(threads_ads)
    time.sleep(150)
    l=pool.map(get_ads_hrefs,hd)
    df=pd.concat(l)
    print('Предобработка данных')
    df.columns=['ID','Region','Region code','City','Model','Body type','Color','Steering wheel','Mileage','Engine','Horse power','Transmission','Drive']
    df['City']=df['City'].map(lambda x:extract_city(x))
    df['Brand']=df.Model.map(lambda x:extract_brand(x))
    df['Brand']=df.Brand.map(lambda x:correct_errors(x))
    df['Mileage']=df.Mileage.map(lambda x:extract_mileage(x))
    df['Horse power']=df['Horse power'].map(lambda x:extract_HP(x))
    df.drop_duplicates(subset=['ID'],keep='last',inplace=True)
    print('Сохранение')
    df.to_excel("Drom_Scraping_Results.xlsx",sheet_name='Drom Scraping Results',index=False)
    


# In[12]:


Drom_Parser()

