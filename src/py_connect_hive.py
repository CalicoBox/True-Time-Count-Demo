#!/usr/bin/env python
# -*- coding: utf-8 -*-
 
import sys
import os

from TCLIService import TCLIService
from TCLIService.ttypes import TOpenSessionReq, TGetTablesReq, TFetchResultsReq, TStatusCode, TGetResultSetMetadataReq, TGetColumnsReq, TType, TExecuteStatementReq, TGetOperationStatusReq, TFetchOrientation, TCloseSessionReq, TGetSchemasReq, TCancelOperationReq
 
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
 
class HiveServerTColumnValue:
  def __init__(self, tcolumn_value):
    self.column_value = tcolumn_value
 
  @property
  def val(self):
    # TODO get index from schema
    if self.column_value.boolVal is not None:
      return self.column_value.boolVal.value
    elif self.column_value.byteVal is not None:
      return self.column_value.byteVal.value
    elif self.column_value.i16Val is not None:
      return self.column_value.i16Val.value
    elif self.column_value.i32Val is not None:
      return self.column_value.i32Val.value
    elif self.column_value.i64Val is not None:
      return self.column_value.i64Val.value
    elif self.column_value.doubleVal is not None:
      return self.column_value.doubleVal.value
    elif self.column_value.stringVal is not None:
      return self.column_value.stringVal.value
 
class HiveServerClient(object):
    user = 'fatkun'
    session_handle = None
 
    def connect(self):
        transport = TSocket.TSocket('10.3.181.235', 10000)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = TCLIService.Client(protocol)
        transport.open()
        self._client = client
 
    def open_session(self, username):
        req = TOpenSessionReq(username=username, configuration={})
        res = self._client.OpenSession(req)
        session_handle = res.sessionHandle
        print res
        return session_handle
 
    def call(self, fn, req, status=TStatusCode.SUCCESS_STATUS):
        if self.session_handle is None:
            self.session_handle = self.open_session(self.user)
 
        if hasattr(req, 'sessionHandle') and req.sessionHandle is None:
            req.sessionHandle = self.session_handle
 
        res = fn(req)
        return res
 
    def execute_statement(self, statement, max_rows=100):
        req = TExecuteStatementReq(statement=statement, confOverlay={})
        res = self.call(self._client.ExecuteStatement, req)
 
        return self.fetch_result(res.operationHandle, max_rows=max_rows)
 
 
    def fetch_result(self, operation_handle, orientation=TFetchOrientation.FETCH_NEXT, max_rows=100):
        fetch_req = TFetchResultsReq(operationHandle=operation_handle, orientation=orientation, maxRows=max_rows)
        res = self.call(self._client.FetchResults, fetch_req)
 
        if operation_handle.hasResultSet:
          meta_req = TGetResultSetMetadataReq(operationHandle=operation_handle)
          schema = self.call(self._client.GetResultSetMetadata, meta_req)
        else:
          schema = None
 
        return res, schema
 
def main():
 
    client = HiveServerClient()
    client.connect()
    client.execute_statement(statement='SET hive.server2.blocking.query=true')
 
    statement = 'show databases;'
    res, schema = client.execute_statement(statement)
    for row in res.results.rows:
        for column in row.colVals:
            print HiveServerTColumnValue(column).val
 
 
if __name__ == '__main__':
    main()