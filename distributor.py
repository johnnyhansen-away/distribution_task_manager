#!/usr/bin/env python
# -*- coding=utf-8 -*-
import ujson as json
import time
import os
import config
import traceback
import datetime
import random
import gevent
import gevent.monkey
gevent.monkey.patch_all()
from public.tools import Tools
from globals.global_var import Global


# 任务表类, 负责维护任务表和分配任务,保证任务均分给worker
class Distributor(object):
    def __init__(self, name, logger=None):
        """

        Args:
            name: 名牌
            logger:　日志
        Returns:
        """
        self.logger = logger
        self.ip = Tools.get_eth0_addr()
        self.pid = os.getpid()
        self.total_qps = 0
        self.supplier_set = set()
        self.name = name
        self.init_time = time.time()
        self.init_status = True
        self.all_suppliers = Global.get_all_suppliers()

    def get_all_suppliers_from_codis(self):
        """
        获取当前下载服务上线的所有供应商列表
        Returns:
        """
        try:
            all_suppliers = set()
            ret = Global.redis.get(Global.REDIS_KEY_DOWNLOAD_ALL_SUPPLIERS)
            if ret:
                all_suppliers = json.loads(ret)
                all_suppliers = set(all_suppliers)
            if config.DEBUG:
                print '上线所有供应商: %s', all_suppliers
            return all_suppliers
        except:
            self.logger.error('@get_all_suppliers except:%s' % traceback.format_exc())
            return set()

    def run(self):
        """
        Returns:
        """
        # 判断当前最小总qps进程
        # 如果是我, 扫描任务表,找到无主供应商,分配到自己名下,如果没有无主供应商,从中挑选一个分配到自己名下
        # 如果不是我,更新自己所有供应商的心跳状态,确认自活
        while True:
            try:
                # 刷新当前进程数据信息,遍历任务表
                self.get_my_supplier_info()
                # 判断当前进程是否为最小进程
                if self.i_am_min():
                    if int(time.time()) % 5 == 0:
                        self.logger.info('@run worker=%s is min,total_qps=%s checking schema' % (self.name, self.total_qps))
                    self.get_one_supplier()
                    self.update_min_info()
                    # 冷起时的快速分配,允许短暂混乱,保证快速分配
                    if self.init_status:
                        if time.time() - self.init_time > 10:
                            self.init_status = False
                        continue
                    else:
                        Tools.rand_sleep(1)
                self.update_supplier_work_status()
                Tools.rand_sleep(1)
            except:
                self.logger.error('@run except:%s' % traceback.format_exc())

    def update_supplier_work_status(self):
        """
        更新本进程处理供应商的状态,供应商keep-alive
        Returns:

        """
        for supplier_id in self.supplier_set:
            try:
                if config.DEBUG:
                    print 'update supplier_id=%s status keep alive' % supplier_id
                start = time.time()
                self.update_supplier_codis_status(supplier_id)
                end = time.time()
                used = end - start
                if used > 1:
                    self.logger.error('@update_supplier_work_status update uesd=%s supplier_id=%s' % (used, supplier_id))
            except:
                self.logger.info('@update_my_job_status except %s' % traceback.format_exc())

    def update_supplier_codis_status(self, supplier_id):
        """
        更新此供应商任务信息-用于keep-alive
        Args:
            supplier_id:
        Returns:
        """
        try:
            key = Global.REDIS_KEY_SUPPLIER_STATUS % supplier_id
            # Global.redis.hset(key, 'pid', self.my_name)
            # Global.redis.hset(key, 'update', time.time())
            Global.redis.hmset(key, {'pid': self.name, 'update': time.time()})
            return True
        except:
            self.logger.info('@update_supplier_codis_status except %s' % traceback.format_exc())
            return False

    def get_my_supplier_info(self):
        """
        统计
        Returns:
        """
        try:
            my_supplier_set = set()
            my_total_qps = 0
            self.all_suppliers = Global.get_all_suppliers()
            for supplier_id in self.all_suppliers:
                try:
                    key = Global.REDIS_KEY_SUPPLIER_STATUS % supplier_id
                    if Global.redis.exists(key):
                        # qps = float(Global.redis.hget(key, 'qps'))
                        qps = Global.get_supplier_qps(supplier_id)
                        ret = Global.redis.hget(key, 'pid')
                        if ret == self.name:
                            my_total_qps += qps
                            my_supplier_set.add(supplier_id)
                except:
                    self.logger.error('worker=%s except in get_my_supplier_info [%s]' % (self.name, traceback.format_exc()))
            self.total_qps = my_total_qps
            self.supplier_set = my_supplier_set
            if int(time.time()) % 5 == 0:
                self.logger.info('worker=%s total_qps=%s suppliers=%s' % (self.name, self.total_qps, self.supplier_set))
            return
        except:
            self.logger.error(traceback.format_exc())
            return

    def get_task_suppliers(self):
        """
        获取当前进程的整流供应商列表
        Returns: set()
        """
        return self.supplier_set

    def update_min_info(self):
        """
        更新最小进程
        Returns:
        """
        ret = Global.redis.hget(Global.REDIS_KEY_MIN_PROCESS_INFO, 'min')
        if ret:
            ret = json.loads(ret)
            min_pid = ret.get('pid')
            min_qps = ret.get('qps', 0)
            update_time = ret.get('update_time')
            if self.total_qps < min_qps or time.time() - float(update_time) > config.SCHEMA_UPDATE_TIMEOUT or min_pid == self.name:
                self.update_codis_min_process_status()
                return True
        else:
            self.update_codis_min_process_status()
            return True
        return False

    def i_am_min(self):
        """
        判断我当前持有的qps,是否比设置上的最小qps小
        Returns:
        """
        ret = Global.redis.hget(Global.REDIS_KEY_MIN_PROCESS_INFO, 'min')
        if ret:
            ret = json.loads(ret)
            # min_pid = ret.get('pid')
            min_qps = ret.get('qps', 0)
            update_time = ret.get('update_time')
            if self.total_qps < min_qps:
                self.update_codis_min_process_status()
                return True
            if time.time() - float(update_time) > config.SCHEMA_UPDATE_TIMEOUT:
                self.update_codis_min_process_status()
                return True
        else:
            self.update_codis_min_process_status()
            return True
        return False

    def update_codis_min_process_status(self):
        """
        更新codis中最小进程的信息
        Returns:
        """
        try:
            ret = dict()
            cur = Global.redis.hget(Global.REDIS_KEY_MIN_PROCESS_INFO, 'min')
            if cur:
                cur = json.loads(cur)
                if self.total_qps < cur['qps'] or time.time() - float(cur['update_time']) > config.SCHEMA_UPDATE_TIMEOUT or cur['pid'] == self.name:
                    ret['pid'] = self.name
                    ret['qps'] = self.total_qps
                    ret['update_time'] = time.time()
                    Global.redis.hset(Global.REDIS_KEY_MIN_PROCESS_INFO, 'min', json.dumps(ret))
            else:
                ret['pid'] = self.name
                ret['qps'] = self.total_qps
                ret['update_time'] = time.time()
                Global.redis.hset(Global.REDIS_KEY_MIN_PROCESS_INFO, 'min', json.dumps(ret))
            return
        except:
            self.logger.error(traceback.format_exc())
            return

    def get_one_supplier(self):
        """
        到任务表中领取一个任务
        Returns:
        """
        try:
            tmp_info = {
                'num': 0,
                'total_qps': 0.0,
                'pids': set(),
            }
            # 挑选无主供应商
            for supplier_id in self.all_suppliers:
                tmp_info['num'] += 1
                tmp_info['total_qps'] += Global.get_supplier_qps(supplier_id)
                if self.make_supplier_belong_me(supplier_id):
                    self.supplier_set.add(supplier_id)
                    return True
                owner = self.get_supplier_owner(supplier_id)
                if owner:
                    tmp_info['pids'].add(owner)
            # 挑选超时的
            for supplier_id in self.all_suppliers:
                if self.make_supplier_belong_me(supplier_id, timeout=True):
                    self.supplier_set.add(supplier_id)
                    return True
            # 强制转换一个供应商到自己名下
            if not self.supplier_set:
                x = random.randint(0, len(self.all_suppliers)-1)
                supplier_id = list(self.all_suppliers)[x]
                if self.make_supplier_belong_me(supplier_id, force=True):
                    self.supplier_set.add(supplier_id)
                    return True
            return False
        except:
            self.logger.error(traceback.format_exc())
            return False

    def get_supplier_owner(self, supplier_id):
        """
        获取此供应商的拥有者
        Args:
            supplier_id:
        Returns: str
        """
        try:
            key = Global.REDIS_KEY_SUPPLIER_STATUS % supplier_id
            ret = Global.redis.hget(key, 'pid')
            if ret:
                return ret
            else:
                return str()
        except:
            self.logger.error(traceback.format_exc())
            return str()

    def make_supplier_belong_me(self, supplier_id, timeout=False, force=False):
        """
        如果这个供应商是无主供应商, 或者 将他收到自己的任务表中
        Args:
            supplier_id:
            timeout:超时的是否选取
            force: 是否从其他进程抢一个供应商
        Returns:bool
        """
        try:
            key = Global.REDIS_KEY_SUPPLIER_STATUS % supplier_id
            qps = Global.get_supplier_qps(supplier_id)
            if not Global.redis.exists(key):    # 无主
                # Global.redis.hset(key, 'pid', self.my_name)
                # Global.redis.hset(key, 'update', time.time())
                # Global.redis.hset(key, 'qps', qps)
                Global.redis.hmset(key, {'pid': self.name, 'update': time.time(), 'qps': qps})
                ret = Global.redis.hget(key, 'pid')
                if ret == self.name:
                    self.total_qps += qps
                    self.logger.info('supplier_id=%s belong to %s by no pid my suppliers=%s' % (supplier_id, self.name, self.supplier_set))
                    return True
            if timeout:  # 超时
                last_update_time = Global.redis.hget(key, 'update')
                if time.time() - float(last_update_time) > config.SCHEMA_UPDATE_TIMEOUT:
                    # Global.redis.hset(key, 'pid', self.my_name)
                    # Global.redis.hset(key, 'update', time.time())
                    # Global.redis.hset(key, 'qps', qps)
                    Global.redis.hmset(key, {'pid': self.name, 'update': time.time(), 'qps': qps})
                    ret = Global.redis.hget(key, 'pid')
                    if ret == self.name:
                        self.total_qps += qps
                        self.logger.info('supplier_id=%s belong to %s by timeout my suppliers=%s' % (supplier_id, self.name, self.supplier_set))
                        return True
            if force:
                # todo 将此供应商强行修改成属于我的
                # Global.redis.hset(key, 'pid', self.my_name)
                # Global.redis.hset(key, 'update', time.time())
                # Global.redis.hset(key, 'qps', qps)

                Global.redis.hmset(key, {'pid': self.name, 'update': time.time(), 'qps': qps})
                ret = Global.redis.hget(key, 'pid')
                if ret == self.name:
                    self.total_qps += qps
                    self.logger.info('supplier_id=%s belong to %s by force my suppliers=%s' % (supplier_id, self.name, self.supplier_set))
                    return True
            return False
        except:
            self.logger.error(traceback.format_exc())
            return False


if __name__ == '__main__':
    import redis
    Global.init()
    handler = Distributor(4)
    handler.run()
