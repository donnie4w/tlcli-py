"""
Copyright 2023 tldb Author. All Rights Reserved.
email: donnie4w@gmail.com
"""
from tlcli import *
import pytest

def test_001():
    print("hello test")

cli = client()
def test_tlcli_init():
    ack = cli.newConnect(False, "127.0.0.1", 7001, "mycli=123")
    print(ack)
    ack = cli.createTable("pyuser", {"name": ColumnType.STRING, "age": ColumnType.INT64, "level": ColumnType.FLOAT64},
                          ["name", "level"])
    print(ack)


def test_insert():
    ack = cli.newConnect(False, "127.0.0.1", 7001, "mycli=123")
    for i in range(20):
        j = i + 0.1
        ab = cli.insert("pyuser",
                        {"name": bytearray("pyname" + str(i), "utf-8"), "age": i.to_bytes(8, 'big'),
                         "level": struct.pack('>f', j)})
        print(ab)


def test_select():
    ack = cli.newConnect(False, "127.0.0.1", 7001, "mycli=123")
    print(cli.selectId("pyuser"))
    print(cli.selectIdByIdx("pyuser", "name", bytearray("pyname0", "utf-8")))
    db = cli.selectById("pyuser", 1)
    print(db)


def test_selectlist():
    ack = cli.newConnect(False, "127.0.0.1", 7001, "mycli=123")
    ldb = cli.selectByIdxLimit("pyuser", "name", [bytearray("pyname0", "utf-8")], 0, 10)
    if ldb is not None:
        for db in ldb:
            print(db)

    ldb = cli.selectsByIdLimit("pyuser", 0, 5)
    if ldb is not None:
        for db in ldb:
            print(db)


def test_selectlist2():
    ack = cli.newConnect(False, "127.0.0.1", 7001, "mycli=123")
    ldb = cli.selectByIdxAscLimit("pyuser", "name", bytearray("pyname0", "utf-8"), 1, 10)
    if ldb is not None:
        for db in ldb:
            print(db)

    ldb = cli.selectByIdxDescLimit("pyuser", "name", bytearray("pyname0", "utf-8"), 11, 10)
    if ldb is not None:
        for db in ldb:
            print(db)

def test_update():
    ack = cli.newConnect(False, "127.0.0.1", 7001, "mycli=123")
    age, level = 20, 11.1
    ab = cli.update("pyuser", 10, {"name": bytearray("pyname0", "utf-8"), "age": age.to_bytes(8, 'big'),
                                  "level": struct.pack('>f', level)})


def test_deleteBatch():
    ack = cli.newConnect(False, "127.0.0.1", 7001, "mycli=123")
    cli.deleteBatch("pyuser", 4, 5)
