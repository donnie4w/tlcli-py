"""
Copyright 2023 tldb Author. All Rights Reserved.
email: donnie4w@gmail.com
"""
import _thread
import logging
import ssl
import threading
import time
from thrift.protocol import *
from thrift.transport import *
from thrift.transport.TSSLSocket import TSSLSocket

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
            self.transport.open()
            self.pingNum = 0
            return self.Auth(self.auth)
        except Exception as e:
            logging.error("connect error" + str(e))

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
            finally:
                pass
        self.__connect()

    def Auth(self, _at) -> Ack:
        with self.lock:
            return self.conn.Auth(_at)

    def ping(self) -> Ack:
        with self.lock:
            return self.conn.Ping(1)

    def createTable(self, name, columns, Idx):
        with self.lock:
            cm = {}
            for cname in columns:
                cm[cname] = bytearray()
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
            for cname in columns:
                cm[cname] = bytearray()
            im = None
            if Idx is not None:
                im = {}
                for iname in Idx:
                    im[iname] = 0
            tb = TableBean(name=name, columns=cm, Idx=im)
            return self.conn.Alter(tb)

    def showTable(self, name):
        with self.lock:
            return self.conn.ShowTable(name)

    def showAllTables(self):
        with self.lock:
            return self.conn.ShowAllTables()

    def selectId(self, name) -> int:
        with self.lock:
            return self.conn.SelectId(name)

    def selectIdByIdx(self, name, column, value) -> int:
        with self.lock:
            return self.conn.SelectIdByIdx(name, column, value)

    def selectById(self, name, id):
        with self.lock:
            return self.conn.SelectById(name, id)

    def selectByIdx(self, name, column, value):
        with self.lock:
            return self.conn.SelectByIdx(name, column, value)

    def selectAllByIdx(self, name, column, value):
        with self.lock:
            return self.conn.SelectAllByIdx(name, column, value)

    def selectsByIdLimit(self, name, startId, limit):
        with self.lock:
            return self.conn.SelectsByIdLimit(name, startId, limit)

    def selectByIdxLimit(self, name, column, value, startId, limit):
        with self.lock:
            return self.conn.SelectByIdxLimit(name, column, value, startId, limit)

    def insert(self, name, columnMap):
        with self.lock:
            cm = {}
            for cname in columnMap:
                cm[cname] = columnMap[cname]
            tb = TableBean(name=name, columns=cm)
            return self.conn.Insert(tb)

    def update(self, name, id, columnMap):
        with self.lock:
            cm = {}
            for cname in columnMap:
                cm[cname] = columnMap[cname]
            tb = TableBean(name=name, id=id, columns=cm)
            return self.conn.Update(tb)

    def delete(self, name, id):
        with self.lock:
            tb = TableBean(name=name, id=id)
            return self.conn.Delete(tb)

    def drop(self, name):
        with self.lock:
            return self.conn.Drop(name)

    def timer(self, id):
        while id == self.connid:
            time.sleep(3)
            try:
                self.pingNum += 1
                ack = self.ping()
                if ack is not None and ack.ok:
                    self.pingNum -= 1
            except Exception as e:
                print("ping error " + str(e))
            if self.pingNum > 5 and id == self.connid:
                client.reconnect()


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
    ack = cli.newConnect(False, "127.0.0.1", 7100, "mycli=123")
    logging.debug(ack)
    ab = cli.showAllTables()
    logging.debug(ab)
    logging.debug("max id>>"+str(cli.selectId("pyuser")))
    logging.debug("max name id>>"+str(cli.selectIdByIdx("pyuser","name",bytearray("pyname", "utf-8"))))
    db = cli.selectById("user", 1)
    logging.debug(db)
    ack = cli.createTable("pyuser", ["name", "age", "level"], ["name", "age"])
    logging.debug(ack)
    for i in range(11):
        ab = cli.insert("pyuser",
                        {"name": bytearray("pyname" + str(i), "utf-8"), "age": bytearray(str(10 + i), "utf-8"),
                         "level": bytearray(str(100 + i), "utf-8")})

    ab = cli.update("pyuser", 1, {"name": bytearray("pyname", "utf-8"), "age": bytearray("20", "utf-8"),
                                  "level": bytearray("120", "utf-8")})
    logging.debug(ab)

    ab = cli.delete("pyuser", 3)  # 删除id=3的数据
    logging.debug(ab)

    ldb = cli.selectByIdxLimit("pyuser", "age", [bytearray("10", "utf-8"), bytearray("20", "utf-8")], 0, 10)
    for db in ldb:
        logging.debug(db)

    logging.debug("\n")
    ldb = cli.selectsByIdLimit("pyuser", 0, 5)
    for db in ldb:
        logging.debug(db)

    # ab = cli.drop("pyuser")  #删除表
    logging.debug(ab)
    time.sleep(10000)
