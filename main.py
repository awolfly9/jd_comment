#-*- coding: utf-8 -*-

import json
import os
import logging
import requests
import redis
import config
import time
import subprocess
import utils

if __name__ == '__main__':
    if not os.path.exists('log'):
        os.makedirs('log')

    logging.basicConfig(
            filename = 'log/%s.log' % 'main',
            format = '%(levelname)s %(asctime)s: %(message)s',
            level = logging.DEBUG
    )

    url = '%sjd/register_spider' % config.domain
    r = requests.get(url = url)
    data = json.loads(r.text)
    utils.log('register_spider data:%s' % data)
    guid = data.get('guid', -1)
    if guid == -1:
        utils.log('register_spider ERROR not get guid')
    else:
        red = redis.StrictRedis(host = config.redis_host, port = config.redis_part, db = config.redis_db,
                                password = config.redis_pass)
        process_list = []
        product_ids = []
        while True:
            product_id = red.lpop(guid)
            if product_id == None:
                time.sleep(0.5)
                continue

            product_ids.append(product_id)
            utils.log('start crawl spider product_id:%s' % product_id)
            for i in range(config.process_count):
                popen = subprocess.Popen('cd {dir};python runspider.py {param}'.format(
                        dir = os.getcwd(),
                        param = 'jd_comment %s' % product_id),
                        shell = True)
                data = {
                    'product_id': product_id,
                    'popen': popen,
                }

                process_list.append(data)

    # 删除 guid
    url = '%sjd/delete_spider?guid=%s' % (config.domain, guid)
    r = requests.get(url = url)
    utils.log(r.text)
