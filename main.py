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

    domain = config.domain

    url = '%s/jd/register_spider' % domain
    r = requests.get(url = url)
    utils.log(r.text)
    data = json.loads(r.text)
    guid = data.get('guid', -1)
    if guid == -1:
        utils.log('ERROR not get guid')
    else:
        red = redis.StrictRedis(host = config.redis_host, port = config.redis_part, db = config.redis_db,
                                password = config.redis_pass)
        process_list = []
        product_ids = []
        while True:
            # 清理抓取完成的进程
            for id in product_ids:
                if red.llen(id) <= 0:
                    for i, process in enumerate(process_list):
                        if id == process.get('product_id'):
                            popen = process.get('popen')
                            popen.poll()
                            popen.terminate()
                            process_list.remove(process)

                            utils.log('clear process:%s' % process)
                            break

                    utils.log('clear product_id:%s' % id)
                    product_ids.remove(id)
                    break

            product_id = red.lpop(guid)
            if product_id == None:
                time.sleep(0.5)
                continue

            product_ids.append(product_id)
            utils.log('product_id:%s' % product_id)
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

    url = '%s/jd/delete_spider?guid=%s' % (domain, guid)
    r = requests.get(url = url)
    utils.log(r.text)
