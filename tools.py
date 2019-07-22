#!/usr/bin/env python
# -*- coding=utf-8 -*-
import time
import traceback
import hashlib
import datetime
import netifaces
import gevent
import random
import snappy
import gzip
import config
import StringIO
import requests
import json
from globals.consts import Const
if config.KAFKA_ONLINE:
    from public.kafka_client import KafkaProducerCli


class Tools(object):
    __logger = None

    @classmethod
    def set_logger(cls, logger):
        cls.__logger = logger

    @classmethod
    def __info(cls, msg):
        if cls.__logger:
            cls.__logger.info(msg)
        else:
            print msg

    @classmethod
    def __warn(cls, msg):
        if cls.__logger:
            cls.__logger.warn(msg)
        else:
            print msg

    @classmethod
    def __error(cls, msg):
        if cls.__logger:
            cls.__logger.error(msg)
        else:
            print msg

    # 创建进程内全局唯一的session号
    @classmethod
    def create_session_id(cls):
        try:
            now_time = time.time()
            session_id = int(now_time * 1000 * 1000)
            return session_id
        except:
            cls.__error(traceback.format_exc())
            return 0

    @classmethod
    def get_md5(cls, string):
        try:
            m = hashlib.md5()
            m.update(string)
            return m.hexdigest()
        except:
            cls.__error(traceback.format_exc())
            return None

    @classmethod
    def to_int(cls, string):
        try:
            v = int(string)
            return v
        except:
            cls.__error(traceback.format_exc())
            return None

    @classmethod
    # 分片日期
    def split_days(cls, check_in, check_out, split_day=1, split=0):
        """
        :type check_in: str
        :type check_out: str
        :type split_day: int
        :type split: int
        :return: list[tuple[str, str]]
        """
        try:
            date_tuple_list = list()
            if isinstance(check_in, datetime.date):
                check_in_date = check_in
                check_out_date = check_out
            else:
                check_in_date = datetime.datetime.strptime(check_in, '%Y-%m-%d')
                check_out_date = datetime.datetime.strptime(check_out, '%Y-%m-%d')
            date_delta = datetime.timedelta(days=split_day)

            # 不需要分片或不能分片
            if split == 0 or split_day <= 0:
                date_tuple = (check_in_date.strftime('%Y-%m-%d'), check_out_date.strftime('%Y-%m-%d'))
                date_tuple_list.append(date_tuple)
                return date_tuple_list

            b_date = check_in_date
            e_date = check_out_date
            while True:
                # 尝试找到节点
                m_date = b_date + date_delta
                if m_date >= e_date:
                    break
                # 加入节点
                date_tuple = (b_date.strftime('%Y-%m-%d'), m_date.strftime('%Y-%m-%d'))
                date_tuple_list.append(date_tuple)
                # 准备下一次循环
                b_date = m_date
            # 加入尾巴
            date_tuple = (b_date.strftime('%Y-%m-%d'), e_date.strftime('%Y-%m-%d'))
            date_tuple_list.append(date_tuple)
            return date_tuple_list
        except:
            cls.__error(traceback.format_exc())
            return list()

    @classmethod
    def days_later(cls, now, days=1):
        """
        :type now: string
        :type days: int
        """
        try:
            now = datetime.datetime.strptime(now, '%Y-%m-%d')
            delta = datetime.timedelta(days=days)
            later = now + delta
            result = later.strftime('%Y-%m-%d')
            return result
        except:
            cls.__error(traceback.format_exc())
            return None

    @classmethod
    def get_eth_addr(cls):
        try:
            eth_addr = dict()
            eth_list = netifaces.interfaces()
            for eth in eth_list:
                if not eth.startswith('eth'):
                    continue
                addr = netifaces.ifaddresses(eth)[netifaces.AF_INET][0]['addr']
                eth_addr[eth] = addr
            return eth_addr
        except:
            cls.__error(traceback.format_exc())
            return dict()

    @classmethod
    def get_eth0_addr(cls):
        try:
            return netifaces.ifaddresses('eth0')[netifaces.AF_INET][0]['addr']
        except:
            cls.__error(traceback.format_exc())
            return str()

    @classmethod
    def float_time_to_fmt(cls, t_string):
        """ 将1467255629.429908转换成2016-06-30 11:00:29
        :type t_string: string
        """
        try:
            # string直接转换成float
            try:
                v = float(t_string)
                tp_time = time.localtime(v)
                fmt_time = time.strftime('%Y-%m-%d %H:%M:%S', tp_time)
                return fmt_time
            except:
                pass

            return t_string
        except:
            return "0"

    @classmethod
    def fmt_time_to_float(cls, t_string):
        """ t_string可以是1467255629.429908这种格式也可以是'2016-06-30 11:00:29'这种格式
        :type t_string: string
        """
        try:
            try:
                # string先转换成TimeTuple，再转换成float
                p_time = time.strptime(t_string, '%Y-%m-%d %H:%M:%S')
                v = time.mktime(p_time)
                return v
            except:
                pass

            # string直接转换成float
            v = float(t_string)
            return v
        except:
            cls.__error(traceback.format_exc())
            return 0

    @classmethod
    def weight_choice(cls, lis, weight):
      """
      :param lis: 待选取序列
      :param weight: list对应的权重序列
      :return:选取的值
      """
      new_list = []
      for i, val in enumerate(lis):
        new_list.extend(val * weight[i])
      return random.choice(new_list)

    @classmethod
    def is_valid_date(cls, str_date):
        """
        判断是否是一个有效的日期字符串
        Args:
            str_date:
        Returns: bool
        """
        try:
            time.strptime(str_date, "%Y-%m-%d")
            return True
        except:
            return False

    @classmethod
    def get_square(cls, clock=0):
        """
        获取当前时间所在时间上一个时间段, 例如将一个小时分为4段 0-15-30-45
        如果当前时间为46:00 则返回30
        clock 向前偏移时刻数 0 1 2...
        Returns: 时间段 int
        """
        try:
            minute = datetime.datetime.fromtimestamp(time.time() - 15 * clock * 60).minute
            hour = datetime.datetime.fromtimestamp(time.time() - 15 * clock * 60).hour
            day = datetime.datetime.fromtimestamp(time.time() - 15 * clock * 60).day
            # minute = datetime.datetime.now().minute
            if 0 <= minute < 15:
                return day, hour, 0
            elif 15 <= minute < 30:
                return day, hour, 15
            elif 30 <= minute < 45:
                return day, hour, 30
            elif 45 <= minute < 60:
                return day, hour, 45
            else:
                return -1, -1, -1
        except:
            return -1, -1, -1

    @classmethod
    def rand_sleep(cls, high_range=1):
        """
        range 1 毫秒
        Args:
            high_range (float):

        Returns:

        """
        try:
            if high_range == 1:
                t = random.randint(0, 9) * 0.1
            else:
                t = random.randint(0, high_range)
            gevent.sleep(t)
        except:
            gevent.sleep(1)

    @classmethod
    def snappy_compress_data(cls, info):
        try:
            info = '' if info is None else info
            return snappy.compress(info)
        except:
            cls.__error(traceback.format_exc())
            return info

    @classmethod
    def snappy_uncompress_data(cls, info):
        try:
            if info[0] != '{':
                return snappy.uncompress(info)
            else:
                return info
        except:
            cls.__error(traceback.format_exc())
            return None

    @classmethod
    def gzip_data(cls, data):
        """
        压缩数据的方法
        Args:
            data:
        Returns:
        """
        buf = StringIO.StringIO()
        f = gzip.GzipFile(fileobj=buf, mode="wb")
        f.write(data)
        f.close()
        return buf.getvalue()

    @classmethod
    def ungzip_data(cls, data):
        """
        解压缩数据
        Args:
            data:

        Returns:

        """
        buf = StringIO.StringIO(data)
        f = gzip.GzipFile(fileobj=buf)
        return f.read()

    @classmethod
    def out_of_supplier_days(cls, supplier_id, checkin, checkout):
        """
        判断此请求针对此供应商是否超出日期限制
        Args:
            supplier_id:
            checkin:
            checkout:
        Returns: bool
        """
        try:
            if checkin < datetime.date.today() or checkout < checkin:
                return True
            if str(supplier_id) in config.SUPPLIER_CHECK_DAYS_OFFSET:
                offset = config.SUPPLIER_CHECK_DAYS_OFFSET[str(supplier_id)]
                if checkin < datetime.date.today() + datetime.timedelta(days=offset):
                    return True
            return False
        except:
            cls.__error(traceback.format_exc())
            return False

    @classmethod
    def get_specify_hqs_route(cls, sequence):
        """
        根据sequence 判断此条请求是否应该选取指定的hqs环境
        Args:
            sequence:
            :type sequence str
        Returns: None or 指定的hqs环境
        """
        try:
            for key, value in config.HQS_ROUTE_MAPPING.items():
                if sequence.startswith(key):
                    return value
            return None
        except:
            cls.__logger.error('sequence=%s except:%s' % (sequence, traceback.format_exc()))
            return None

    @classmethod
    def get_all_hotel_info(cls, supplier_id, hq_hotel_id):
        """
        获取此酒店的优选供应商酒店列表
        :param supplier_id
        :param hq_hotel_id:
        :return: list()
        """
        try:
            url = config.H2S_MAP_SERVER_HEAD % (supplier_id, hq_hotel_id)
            ret = requests.get(url, 30)
            ret = ret.json()
            export = ret[Const.data][Const.Export]
            hq_city_id = ret[Const.data][Const.hq_city_id]
            result = set()
            for item in export:
                try:
                    sp_city_code = item[Const.sp_city_code]
                    sp_city_name = item[Const.sp_city_name]
                    sp_hotel_code = item[Const.sp_hotel_code]
                    supplier_id = int(item[Const.supplier_id])
                    result.add((supplier_id, hq_hotel_id, sp_hotel_code, sp_city_code, sp_city_name, hq_city_id))
                except:
                    cls.__logger.error(traceback.format_exc())
            return result
        except:
            cls.__logger.error(traceback.format_exc())
            return set()

    @classmethod
    def make_dls_url(cls, supplier_id, hq_hotel_id, sp_hotel_id, hq_city_id, checkin, checkout,
                     sp_city_code, sp_city_name, sequence, froms, sp_country_code='', room_num=1, adult=2,
                     child=0, child_age='', citizenship='CN', forcepush=''):
        """
        拼装下载服务url
        :param supplier_id:
        :param hq_hotel_id:
        :param sp_hotel_id:
        :param hq_city_id:
        :param checkin:
        :param checkout:
        :param sp_city_code:
        :param sp_city_name:
        :param sequence:
        :param froms
        :param sp_country_code:
        :param room_num:
        :param adult:
        :param child:
        :param child_age:
        :param citizenship
        :param forcepush
        :return:
        """
        try:
            param = 'supplier_id=%s' % supplier_id
            param += '&hq_hotel_id=%s' % hq_hotel_id
            param += '&sp_hotel_id=%s' % sp_hotel_id
            param += '&hq_city_id=%s' % hq_city_id
            param += '&checkin=%s' % checkin
            param += '&checkout=%s' % checkout
            param += '&sp_city_code=%s' % sp_city_code
            param += '&sp_city_name=%s' % sp_city_name
            param += '&room_num=%s' % room_num
            param += '&adult=%s' % adult
            param += '&child=%s' % child
            param += '&child_age=%s' % child_age
            param += '&sp_country_code=%s' % sp_country_code
            param += '&citizenship=%s' % citizenship
            param += '&forcepush=%s' % forcepush
            param += '&sequence=%s' % sequence
            param += '&from=%s' % froms
            param += '&validity=%s' % config.DEFAULT_VALIDITY
            url = config.DLS_URL_HEADER + 'price?' + param
            return url
        except:
            cls.__logger.error(traceback.format_exc())
            return None

    @classmethod
    def make_dls_localurl(cls, supplier_id, hq_hotel_id, sp_hotel_id, hq_city_id, checkin, checkout,
                     sp_city_code, sp_city_name, sequence, froms, sp_country_code='', room_num=1, adult=2,
                     child=0, child_age='', citizenship='CN', forcepush=''):
        """
        拼装下载服务url
        :param supplier_id:
        :param hq_hotel_id:
        :param sp_hotel_id:
        :param hq_city_id:
        :param checkin:
        :param checkout:
        :param sp_city_code:
        :param sp_city_name:
        :param sequence:
        :param froms
        :param sp_country_code:
        :param room_num:
        :param adult:
        :param child:
        :param child_age:
        :param citizenship
        :param forcepush
        :return:
        """
        try:
            param = 'supplier_id=%s' % supplier_id
            param += '&hq_hotel_id=%s' % hq_hotel_id
            param += '&sp_hotel_id=%s' % sp_hotel_id
            param += '&hq_city_id=%s' % hq_city_id
            param += '&checkin=%s' % checkin
            param += '&checkout=%s' % checkout
            param += '&sp_city_code=%s' % sp_city_code
            param += '&sp_city_name=%s' % sp_city_name
            param += '&room_num=%s' % room_num
            param += '&adult=%s' % adult
            param += '&child=%s' % child
            param += '&child_age=%s' % child_age
            param += '&sp_country_code=%s' % sp_country_code
            param += '&citizenship=%s' % citizenship
            param += '&forcepush=%s' % forcepush
            param += '&sequence=%s' % sequence
            param += '&from=%s' % froms
            param += '&validity=%s' % config.DEFAULT_VALIDITY
            url = config.DLS_LOCAl_HEADER + 'price?' + param
            return url
        except:
            cls.__logger.error(traceback.format_exc())
            return None

    @classmethod
    def send_log_data_to_kafka(cls, data, topic, session_id, sequence):
        """
        将data的信息写入到kafka,用于log收集的topic中
        Args:
            data:
            topic:
            sequence:
        Returns: bool
        """
        try:
            if isinstance(data, dict):
                data = json.dumps(data)
            KafkaProducerCli.send_data(data, topic)
            return True
        except:
            cls.__logger.error(traceback.format_exc(), session_id, sequence)
            return False
