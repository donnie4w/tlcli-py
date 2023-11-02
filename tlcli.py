"""
Copyright 2023 tldb Author. All Rights Reserved.
email: donnie4w@gmail.com
"""
import _thread
import ssl
import struct
import threading
import time

from thrift.protocol import *
from thrift.transport import *
from thrift.transport.TSSLSocket import TSSLSocket
from consts import *
from Icli import *

logging.basicConfig(level=logging.DEBUG, format='%(message)s')


class client:
    transport = None
    conn = None
    host = ""
    port = 0
    tls = False
    auth = ""
    connid = 0
    pingNum = 1
    lock = threading.Lock()

    def __init__(self):
        self.timeout = 60 * 1000

    def __connect(self) -> Ack:
        try:
            if self.tls:
                socket = TSSLSocket(self.host, self.port, cert_reqs=ssl.CERT_NONE)
            else:
                socket = TSocket.TSocket(self.host, self.port)
            socket.setTimeout(self.timeout)
            self.transport = TTransport.TBufferedTransport(socket)
            protocol = TCompactProtocol.TCompactProtocol(self.transport)
            self.conn = Client(protocol)
            logging.debug("conn>>" + str(self.pingNum))
            self.transport.open()
            self.pingNum = 0
            return self.Auth(self.auth)
        except Exception as e:
            logging.error("connect error:" + str(e))

    def close(self) -> None:
        self.connid += 1
        self.transport.close()

    def setTimeout(self, timeout):
        self.timeout = timeout

    def newConnect(self, tls, host, port, auth) -> Ack:
        self.tls, self.host, self.port, self.auth = tls, host, port, auth
        self.connid += 1
        ack = self.__connect()
        _thread.start_new_thread(self.timer, (self.connid,))
        return ack

    def reconnect(self):
        logging.warning("reconnect")
        if self.conn is not None:
            try:
                self.transport.close()
            except Exception as e:
                pass
        self.newConnect(self.tls, self.host, self.port, self.auth)

    def Auth(self, _at) -> Ack:
        with self.lock:
            return self.conn.Auth(_at)

    def ping(self) -> Ack:
        with self.lock:
            return self.conn.Ping(1)

    def createTable(self, name, columns, Idx):
        with self.lock:
            cm = {}
            for k, v in columns.items():
                cm[k] = bytearray(v.value, "utf-8")
            im = None
            if Idx is not None:
                im = {}
                for iname in Idx:
                    im[iname] = 0
            tb = TableBean(name=name, columns=cm, Idx=im)
            return self.conn.Create(tb)

    def alterTable(self, name, columns, Idx):
        with self.lock:
            cm = {}
            for k, v in columns.items():
                cm[k] = bytearray(v.value, "utf-8")
            im = None
            if Idx is not None:
                im = {}
                for iname in Idx:
                    im[iname] = 0
            tb = TableBean(name=name, columns=cm, Idx=im)
            return self.conn.Alter(tb)

    def showTable(self, name):
        with self.lock:
            try:
                return self.conn.ShowTable(name)
            except Exception as e:
                pass

    def showAllTables(self):
        with self.lock:
            try:
                return self.conn.ShowAllTables()
            except Exception as e:
                pass

    def selectId(self, name) -> int:
        with self.lock:
            try:
                return self.conn.SelectId(name)
            except Exception as e:
                pass

    def selectIdByIdx(self, name, column, value) -> int:
        with self.lock:
            try:
                return self.conn.SelectIdByIdx(name, column, value)
            except Exception as e:
                pass

    def selectById(self, name, id):
        with self.lock:
            try:
                return self.conn.SelectById(name, id)
            except Exception as e:
                pass

    def selectByIdx(self, name, column, value):
        with self.lock:
            try:
                return self.conn.SelectByIdx(name, column, value)
            except Exception as e:
                pass

    def selectAllByIdx(self, name, column, value):
        with self.lock:
            try:
                return self.conn.SelectAllByIdx(name, column, value)
            except Exception as e:
                pass

    def selectsByIdLimit(self, name, startId, limit):
        with self.lock:
            try:
                return self.conn.SelectsByIdLimit(name, startId, limit)
            except Exception as e:
                pass

    def selectByIdxLimit(self, name, column, value, startId, limit):
        if isinstance(value, list):
            with self.lock:
                try:
                    return self.conn.SelectByIdxLimit(name, column, value, startId, limit)
                except Exception as e:
                    pass
        else:
            logging.error('value type error')

    def insert(self, name, columnMap):
        with self.lock:
            try:
                cm = {}
                for cname in columnMap:
                    cm[cname] = columnMap[cname]
                tb = TableBean(name=name, columns=cm)
                return self.conn.Insert(tb)
            except Exception as e:
                pass

    def update(self, name, id, columnMap):
        with self.lock:
            try:
                cm = {}
                for cname in columnMap:
                    cm[cname] = columnMap[cname]
                tb = TableBean(name=name, id=id, columns=cm)
                return self.conn.Update(tb)
            except Exception as e:
                pass

    def delete(self, name, id):
        with self.lock:
            try:
                tb = TableBean(name=name, id=id)
                return self.conn.Delete(tb)
            except Exception as e:
                pass

    def drop(self, name):
        with self.lock:
            try:
                return self.conn.Drop(name)
            except Exception as e:
                pass

    def deleteBatch(self, name, *ids):
        """
        Parameters:
         - name
         - ids
        """
        with self.lock:
            try:
                return self.conn.DeleteBatch(name, ids)
            except Exception as e:
                pass

    def selectByIdxDescLimit(self, name, column, value, startId, limit):
        """
        Parameters:
         - name
         - column
         - value
         - startId
         - limit
        Index fields that are updated frequently are not suitable for this method and may result in sorting errors
        频繁更新的索引字段不适合此方法，并且可能导致排序错误
        """
        with self.lock:
            try:
                return self.conn.SelectByIdxDescLimit(name, column, value, startId, limit)
            except Exception as e:
                pass

    def selectByIdxAscLimit(self, name, column, value, startId, limit):
        """
        Parameters:
         - name
         - column
         - value
         - startId
         - limit
        Index fields that are updated frequently are not suitable for this method and may result in sorting errors
        频繁更新的索引字段不适合此方法，并且可能导致排序错误
        """
        with self.lock:
            try:
                return self.conn.SelectByIdxAscLimit(name, column, value, startId, limit)
            except Exception as e:
                pass

    def timer(self, id):
        while id == self.connid:
            time.sleep(3)
            try:
                self.pingNum += 1
                ack = self.ping()
                if ack is not None and ack.ok:
                    self.pingNum -= 1
            except Exception as e:
                print("ping error:" + str(e))
            if self.pingNum > 5 and id == self.connid:
                self.reconnect()


if __name__ == "__main__":
    """
        tldb数据库 操作示例：
        1.建表      createTable   alterTable
        2.插入数据  insert
        3.更新数据  update
        4.查询数据  selectById  selectsByIdLimit  selectByIdxLimit  selectByIdxLimit  selectAllByIdx
        5.查询数据表 showTable showAllTables
        6.删除表    truncate  
    """
    cli = client()
    ack = cli.newConnect(True, "127.0.0.1", 3336, "mycli=123")
    print(ack)
    ab = cli.showAllTables()
    print(ab)
    time.sleep(600)
