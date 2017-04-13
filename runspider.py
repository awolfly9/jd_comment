#-*- coding: utf-8 -*-

import os
import logging
import sys

from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings


def runspider(name, product_id):
    configure_logging(install_root_handler = False)
    logging.basicConfig(
            filename = 'log/%s.log' % product_id,
            format = '%(levelname)s %(asctime)s: %(message)s',
            level = logging.DEBUG
    )
    process = CrawlerProcess(get_project_settings())
    try:
        logging.info('runscrapy start spider:%s' % name)
        data = {
            'product_id': product_id
        }
        process.crawl(name, **data)
        process.start()
    except Exception, e:
        logging.error('runscrapy spider:%s exception:%s' % (name, e))
        pass

    logging.info('finish this spider:%s\n\n' % name)


if __name__ == '__main__':
    print(sys.argv)
    name = sys.argv[1] or 'jd_comment'
    product_id = sys.argv[2] or '-1'
    print('name:%s' % name)
    print ('project dir:%s' % os.getcwd())
    if product_id == -1:
        print('ERROR not get product_id')
    else:
        runspider(name, product_id)
