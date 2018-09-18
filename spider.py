import os
import requests
import time
from requests import RequestException
import json
from pyquery import PyQuery as pq
from pymongo import MongoClient
from config import *
from multiprocessing.pool import Pool

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client[MONGODB_DB]
switch_col = {
    1: MONGODB_COLLECTION1,
    2: MONGODB_COLLECTION2,
    3: MONGODB_COLLECTION3
}
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/68.0.3440.106 Safari/537.36 '
}


def get__page_index(page, _type):
    """
    获得列表页json
    :param page:
    :param _type:
    :return:
    """
    base_url = "https://www.cqgp.gov.cn/gwebsite/api/v1/notices/stable"
    switch = {
        1: '100,200,201,202,203,204,205,206,207,309,400,401,402,3091,4001',
        2: '301,303',
        3: '300,302,304,3041,305,306,307,308'
    }
    params = {
        'pi': page,
        'ps': 20,
        'timestamp': round(time.time() * 1000),
        'type': switch.get(_type)
    }
    response = requests.get(base_url, params=params, headers=headers)
    try:
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        print("Request list json error")
        return None


def parse_page_index(text):
    """
    解析列表页的json内容
    :param text:
    :return:
    """
    data = json.loads(text)
    if data and 'notices' in data.keys():
        for item in data.get('notices'):
            yield item.get('id')


def get_page_detail(_id):
    """
    获得公告页面json
    :param _id:
    :return:
    """
    url = 'https://www.cqgp.gov.cn/gwebsite/api/v1/notices/stable/' + _id
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.text
    except RequestException:
        print('Request notification page error')


def parse_page_detail(text):
    """
    解析公告页面json
    :param text:
    :return:
    """
    data = json.loads(text)
    if data and 'notice' in data.keys():
        # get()返回的类型就是键对应的值的类型
        notice = data.get('notice')
        html = notice.get('html')
        doc = pq(html)
        doc.find('style').remove()
        return {
            'agentName': notice.get('agentName'),
            'bidBeginTime': notice.get('bidBeginTime'),
            'bidEndTime': notice.get('openBidTime'),
            'buyerName': notice.get('buyerName'),
            'creatorOrgName': notice.get('creatorOrgName'),
            'districtName': notice.get('districtName'),
            'issueTime': notice.get('issueTime'),
            'projectBudget': notice.get('projectBudget'),
            'projectDirectoryName': notice.get('projectDirectoryName'),  # 类别
            'projectName': notice.get('projectName'),
            'projectPurchaseWayName': notice.get('projectPurchaseWayName'),
            'title': notice.get('title'),
            'content': doc.text(),
            'attachments': notice.get('attachments')  # 不一定有
        }


def save_attachment(attachment):
    """
    保存附件到本地
    :param attachment:
    """
    name = attachment.get('name')
    value = attachment.get('value')

    file_path = os.getcwd() + '/attachments/' + value
    dir_path = os.path.split(file_path)[0]
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    try:
        if not os.path.exists(file_path):  # 不存在才请求
            content = download_attachment(name, value)
            if content:
                with open(file_path, 'wb') as f:
                    f.write(content)
                    print(name, "was successfully saved in", file_path)
        else:
            print(name, "has already been saved in", file_path)
    except IOError:
        print("IOError")


def download_attachment(name, value):
    """
    请求附件下载地址
    :param name:
    :param value:
    :return:
    """
    print("Downloading attachment", name)
    base_url = 'https://www.cqgp.gov.cn/gwebsite/files'
    params = {
        'filePath': value,
        'fileName': name
    }
    try:
        response = requests.get(base_url, params=params, headers=headers)
        if response.status_code == 200:
            return response.content
        print(name, "Request attachment failed", response.url)
        return None
    except RequestException:
        print(name, "Request attachment failed", response.url)
        return None


def save_to_mongodb(_type, result):
    """
    保存文档到MongoDB
    :param _type:
    :param result:
    """
    col = db[switch_col[_type]]
    if not col.find_one({'_id': result['_id']}):
        if col.insert_one(result).inserted_id:
            print('Successfully Saved to mongodb', result)
        else:
            print('save to mongodb failed')
    else:
        print(result['title'], "is already in mongodb")


def main(params):
    _type = params[0]
    page = params[1]
    text = get__page_index(page, _type)
    if text:
        for _id in parse_page_index(text):
            text = get_page_detail(_id)
            if text:
                result = parse_page_detail(text)
                attachments = result.get('attachments')
                if attachments:
                    attachments = eval(attachments)  # 转list
                    result['attachments'] = attachments
                    for attachment in attachments:
                        save_attachment(attachment)
                result['_id'] = _id
                save_to_mongodb(_type, result)


if __name__ == '__main__':
    groups = []
    for i in range(1, TYPE_COUNT + 1):
        for j in range(1, MAX_PAGE + 1):
            groups.append([i, j])
    print(groups)
    pool = Pool()
    pool.map(main, groups)
    pool.close()
    pool.join()
