from ibapi import wrapper
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.common import BarData
import datetime
import time

class TestWrapper(wrapper.EWrapper):
    def __init__(self):
        self.data = []


class TestClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)


class TestApp(TestWrapper, TestClient):
    symbols = []

    def __init__(self, dwm,file):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)
        self.dwm = dwm
        self.file = file

    def start(self):
        queryTime = (datetime.datetime.today() - datetime.timedelta(days=0)).strftime("%Y%m%d %H:%M:%S")

        with open(self.file, 'r') as fh:
            data = fh.read()
            self.symbols = data.split('\n')

        id = 4100
        self.sym_dict = {}
        for symbol in self.symbols:
            contract = Contract()
            contract.secType = "STK"
            contract.exchange = "NSE"
            contract.currency = "INR"
            contract.symbol = symbol
            if self.dwm == 'd':
                print('Fetching daily data')
                self.reqHistoricalData(id, contract, queryTime, "8 W", "1 day", "TRADES", 1, 1, False, [])
            if self.dwm == 'w':
                print('Fetching weekly data')
                self.reqHistoricalData(id, contract, queryTime, "10 M", "1 week", "TRADES", 1, 1, False, [])
            # self.reqHistoricalData(id, contract, queryTime, "1 W", "15 min", "TRADES", 1, 1, False, [])
            self.sym_dict[id] = symbol
            id += 1
            time.sleep(1)

    def nextValidId(self, orderId: int):
        print("Setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId
        self.start()

    def historicalData(self, reqId: int, bar: BarData):
        self.data.append([bar.date, bar.close, bar.volume, self.sym_dict[reqId]])

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)
        self.disconnect()
        print("Finished")

    def historicalDataUpdate(self, reqId: int, bar: BarData):
        print("HistoricalDataUpdate. ReqId:", reqId, "BarData.", bar)

    def error(self, reqId, errorCode, errorString):
        print("Error. Id: ", reqId, " Code: ", errorCode, " Msg: ", errorString)