#-*- coding: utf-8 -*-

import sys
import chardet
import re
import json
import datetime
import config
import utils
import redis

from scrapy.http.cookies import CookieJar
from scrapy.utils.project import get_project_settings
from scrapy import Spider
from scrapy import Request
from sqlhelper import SqlHelper

reload(sys)
sys.setdefaultencoding('utf-8')


class JDSpider(Spider):
    name = 'jd_comment'

    def __init__(self, name = None, **kwargs):
        super(JDSpider, self).__init__(name, **kwargs)
        self.product_id = kwargs.get('product_id', -1)
        self.log('product_id:%s' % self.product_id)
        self.item_table = 'item_%s' % self.product_id
        self.product_page = '%s_page' % self.product_id

        self.log_dir = 'log/%s' % self.product_id
        self.is_record_page = False
        if self.is_record_page:
            utils.make_dir(self.log_dir)

        self.sql = SqlHelper()
        self.red = redis.StrictRedis(host = config.redis_host, port = config.redis_part, db = config.redis_db,
                                     password = config.redis_pass)

    def start_requests(self):
        while self.red.llen(self.product_id) > 0:
            data = self.red.lpop(self.product_id)
            if data == None:
                continue

            info = json.loads(data)
            url = 'https://club.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98vv' \
                  '{comment_version}&productId={product_id}&score=0&sortType={sort_type}&page={page}&pageSize=10' \
                  '&isShadowSku=0'. \
                format(product_id = self.product_id, comment_version = info.get('comment_version'),
                       sort_type = info.get('sort_type'), page = info.get('page'))

            yield Request(
                    url = url,
                    headers = {
                        'Accept': '*/*',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Connection': 'keep-alive',
                        'Host': 'club.jd.com',
                        'Referer': 'https://item.jd.com/%s.html' % self.product_id,
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:52.0) Gecko/20100101 '
                                      'Firefox/52.0',
                    },
                    method = 'GET',
                    meta = {
                        'page': info.get('page')
                    },
                    dont_filter = True,
                    callback = self.parse_comment
            )

    def parse_comment(self, response):
        self.save_page('%s_%s.html' % (self.product_id, response.meta.get('page')), response.body)

        detect = chardet.detect(response.body)
        encoding = detect.get('encoding', '')
        body = response.body.decode(encoding, 'ignore')

        pattern = re.compile('\((.*?)\);', re.S)
        item = re.search(pattern, body)
        if item != None and item.group(1) != None:
            data = json.loads(item.group(1))
            comments = data.get('comments', [])
            for comment in comments:
                id = comment.get('id')  # 评论的 id
                content = comment.get('content')  # 评论的内容
                creation_time = comment.get('creationTime', '')  # 评论创建的时间
                reply_count = comment.get('replyCount', '')  # 回复数量
                score = comment.get('score', '')  # 评星
                useful_vote_count = comment.get('usefulVoteCount', '')  # 其他用户觉得有用的数量
                useless_vote_count = comment.get('uselessVoteCount', '')  # 其他用户觉得无用的数量
                user_level_id = comment.get('userLevelId', '')  # 评论用户等级的 id
                user_province = comment.get('userProvince', '')  # 用户的省份
                nickname = comment.get('nickname', '')  # 评论用户的昵称
                product_color = comment.get('productColor', '')  # 商品的颜色
                product_size = comment.get('productSize', '')  # 商品的大小
                user_level_name = comment.get('userLevelName', '')  # 评论用户的等级
                user_client = comment.get('userClient', '')  # 用户评价平台
                user_client_show = comment.get('userClientShow', '')  # 用户评价平台
                is_mobile = comment.get('isMobile', '')  # 是否是在移动端完成的评价
                days = comment.get('days', '')  # 购买后评论的天数
                reference_time = comment.get('referenceTime', '')  # 购买的时间
                after_days = comment.get('afterDays', '')  # 购买后再次评论的天数
                images_count = len(comment.get('images', []))  # 评论总图片的数量
                after_user_comment = comment.get('afterUserComment', '')
                if after_user_comment != '' and after_user_comment != None:
                    ip = after_user_comment.get('ip', '')  # 再次评论的 ip 地址

                    h_after_user_comment = after_user_comment.get('hAfterUserComment', '')
                    after_content = h_after_user_comment.get('content', '')  # 再次评论的内容
                else:
                    ip = ''
                    after_content = ''

                content = content.replace('\'', '')
                after_content = after_content.replace('\'', '')

                msg = {
                    'id': id,
                    'content': content,
                    'creation_time': creation_time,
                    'reply_count': reply_count,
                    'score': score,
                    'useful_vote_count': useful_vote_count,
                    'useless_vote_count': useless_vote_count,
                    'user_level_id': user_level_id,
                    'user_province': user_province,
                    'nickname': nickname,
                    'product_color': product_color,
                    'product_size': product_size,
                    'user_level_name': user_level_name,
                    'user_client': user_client,
                    'user_client_show': user_client_show,
                    'is_mobile': is_mobile,
                    'days': days,
                    'reference_time': reference_time,
                    'after_days': after_days,
                    'images_count': images_count,
                    'ip': ip,
                    'after_content': after_content,
                    'save_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }

                self.sql.insert_json(msg, self.item_table)

        self.sql.commit()

        # 减少 page 标识
        page = self.red.get(self.product_page)
        if page != None:
            self.red.set(self.product_page, int(page) - 1)

    def save_page(self, filename, data):
        if self.is_record_page:
            with open('%s/%s' % (self.log_dir, filename), 'w') as f:
                f.write(data)
                f.close()

    def close(spider, reason):
        # 事务提交数据
        spider.sql.commit()
