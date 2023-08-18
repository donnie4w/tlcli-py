"""
Copyright 2023 tldb Author. All Rights Reserved.
email: donnie4w@gmail.com
"""

from enum import Enum


class ColumnType(Enum):
    STRING = "0"
    INT64 = "1"
    INT32 = "2"
    INT16 = "3"
    INT8 = "4"
    FLOAT64 = "5"
    FLOAT32 = "6"
    BINARY = "7"
    BYTE = "8"
    # UINT64 = "9"
    # UINT32 = "10"
    # UINT16 = "11"
    # UINT8 = "12"
