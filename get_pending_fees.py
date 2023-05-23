#!/usr/bin/env python
# coding: utf-8


import logging
import requests
import pandas as pd
import json
from web3 import Web3, HTTPProvider

logging.basicConfig(format='{asctime} {levelname}: {message}', datefmt='%m/%d/%Y %I:%M:%S %p',
                    style='{', level=logging.INFO)

BASIS_POINTS_DIVISOR = 10000
FUNDING_RATE_PRECISION = 1000000
PRICE_PRECISION = 10 ** 30
MIN_LEVERAGE = 10000
USDG_DECIMALS = 18
MAX_FEE_BASIS_POINTS = 500
MAX_LIQUIDATION_FEE_USD = 100 * PRICE_PRECISION
MIN_FUNDING_RATE_INTERVAL = 1
MAX_FUNDING_RATE_FACTOR = 10000


class GetPendingFees:

    def __init__(self):

        # subgraph url
        self.url_arbi = "https://api.thegraph.com/subgraphs/name/nissoh/gmx-arbitrum"
        self.url_avax = "https://api.thegraph.com/subgraphs/name/nissoh/gmx-avalanche"
        #
        # The Graph query -
        self.query = """
        {
          trades(
            first: 999
            orderBy: size
            where: { size_gte: "10000000000000000000000000000000000", status: open}
          ) {
            account
            averagePrice
            collateral
            collateralDelta
            collateralToken
            fee
            indexToken
            isLong
            key
            realisedPnl
            realisedPnlPercentage
            settledTimestamp
            size
            sizeDelta
            status
            timestamp
            updateList(orderBy: timestamp, orderDirection: desc, first: 1) {
              entryFundingRate
            }
          }
        }
        """

    def get_pending_fees(self):

        logging.info("Making subgraph query..")
        df_avax, df_arbi = self.get_subgraph_data()

        logging.info("Getting contracts..")
        contract_avax, contract_arbi = self.get_contracts()

        logging.info("Getting cumulative funding rates..")
        cumFundingRates = self.get_cumulative_funding_rates(contract_arbi,
                                                            contract_avax,
                                                            df_arbi,
                                                            df_avax)

        logging.info("Getting max & min prices..")
        minPrices, maxPrices = self.get_min_max_prices(contract_arbi,
                                                       contract_avax,
                                                       df_arbi,
                                                       df_avax)
        df = pd.concat([df_arbi, df_avax])

        logging.info("Building message..")
        return self.create_message(df, cumFundingRates, minPrices, maxPrices)

    # Private Methods
    def run_query(self, url):

        # endpoint where you are making the request
        request = requests.post(url, json={'query': self.query})
        if request.status_code == 200:
            return request.json()
        else:
            raise Exception('Query failed. return code is {}.      {}'.format(
                request.status_code, self.query))

    def get_subgraph_data(self):
        result = self.run_query(self.url_arbi,)

        data = result['data']['trades']
        df_arbi = pd.json_normalize(data)

        result = self.run_query(self.url_avax)
        data = result['data']['trades']
        df_avax = pd.json_normalize(data)

        column_headers = ["averagePrice",
                          "collateral",
                          "collateralDelta",
                          "fee",
                          "realisedPnl",
                          "size",
                          "sizeDelta"]
        df_arbi[column_headers] = df_arbi[column_headers].astype(float)
        df_avax[column_headers] = df_avax[column_headers].astype(float)

        df_arbi['entryFundingRate'] = pd.json_normalize(df_arbi['updateList'])
        df_arbi['entryFundingRate'] = pd.json_normalize(df_arbi['entryFundingRate'])
        df_avax['entryFundingRate'] = pd.json_normalize(df_avax['updateList'])
        df_avax['entryFundingRate'] = pd.json_normalize(df_avax['entryFundingRate'])

        return df_avax, df_arbi

    @staticmethod
    def get_contracts():

        web3_arbi = Web3(HTTPProvider('https://arb1.arbitrum.io/rpc'))
        web3_avax = Web3(HTTPProvider('https://rpc.ankr.com/avalanche'))
        address_arbi = Web3.toChecksumAddress('0x489ee077994B6658eAfA855C308275EAd8097C4A')
        address_avax = Web3.toChecksumAddress('0x9ab2de34a33fb459b538c43f251eb825645e8595')

        abi = json.loads('''[
           {"inputs":[{"internalType":"address","name":"", "type": "address"}], "name":"cumulativeFundingRates","outputs":[{"internalType": "uint256", "name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
           {"inputs":[{"internalType":"address","name":"_token","type":"address"}],"name":"getMaxPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
           {"inputs":[{"internalType":"address","name":"_token","type":"address"}],"name":"getMinPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"} 
        ]''')

        contract_arbi = web3_arbi.eth.contract(address=address_arbi, abi=abi)
        contract_avax = web3_avax.eth.contract(address=address_avax, abi=abi)

        return contract_avax, contract_arbi

    @staticmethod
    def get_cumulative_funding_rates(contract_arbi, contract_avax, df_arbi, df_avax):

        cumFundingRates = {}

        collTokens = df_arbi["collateralToken"].unique()
        for collToken in collTokens:
            cumFundingRates[collToken] = contract_arbi.functions.cumulativeFundingRates(
                Web3.toChecksumAddress(collToken)).call()

        collTokens = df_avax["collateralToken"].unique()
        for collToken in collTokens:
            cumFundingRates[collToken] = contract_avax.functions.cumulativeFundingRates(
                Web3.toChecksumAddress(collToken)).call()

        return cumFundingRates

    @staticmethod
    def get_min_max_prices(contract_arbi, contract_avax, df_arbi, df_avax):

        minPrices = {}
        maxPrices = {}

        indxTokens = df_arbi["indexToken"].unique()
        for indxToken in indxTokens:
            minPrices[indxToken] = contract_arbi.functions.getMinPrice(
                Web3.toChecksumAddress(indxToken)).call()
            maxPrices[indxToken] = contract_arbi.functions.getMaxPrice(
                Web3.toChecksumAddress(indxToken)).call()

        indxTokens = df_avax["indexToken"].unique()
        for indxToken in indxTokens:
            minPrices[indxToken] = contract_avax.functions.getMinPrice(
                Web3.toChecksumAddress(indxToken)).call()
            maxPrices[indxToken] = contract_avax.functions.getMaxPrice(
                Web3.toChecksumAddress(indxToken)).call()

        return minPrices, maxPrices

    @staticmethod
    def getFundingFee(cumFundingRates, _collateralToken, _size, _entryFundingRate):
        if _size == 0:
            return 0

        fundingRate = cumFundingRates[_collateralToken] - _entryFundingRate
        if fundingRate == 0:
            return 0

        return _size * fundingRate / FUNDING_RATE_PRECISION

    @staticmethod
    def getDelta(minPrices, maxPrices, _indexToken, _size, _averagePrice, _isLong):
        if (_averagePrice <= 0):
            return 0
        price = minPrices[_indexToken] if _isLong else maxPrices[_indexToken]
        priceDelta = _averagePrice - price if _averagePrice > price else price - _averagePrice
        return _size * priceDelta / _averagePrice

    def create_message(self, df, cumFundingRates, minPrices, maxPrices):

        _totalSize = 0
        _totalFee = 0
        _totalPnL = 0
        _totalFundingFee = 0
        _totalDelta = 0

        for index in range(len(df)):
            _row = df.iloc[index]
            _collateralToken = _row["collateralToken"]
            _indexToken = _row["indexToken"]
            _size = int(_row["size"])
            _fee = int(_row["fee"])
            _realisedPnl = int(_row["realisedPnl"])
            _entryFundingRate = int(_row["entryFundingRate"])
            _averagePrice = int(_row["averagePrice"])
            _isLong = bool(_row["isLong"])
            _FundingFee = self.getFundingFee(
                cumFundingRates,
                _collateralToken,
                _size,
                _entryFundingRate)
            _Delta = self.getDelta(minPrices,
                                   maxPrices,
                                   _indexToken,
                                   _size,
                                   _averagePrice,
                                   _isLong)
            _totalSize += _size
            _totalFee += _fee
            _totalPnL += _realisedPnl
            _totalFundingFee += _FundingFee
            _totalDelta += _Delta

        print(f"Stats for open 10k+ positions")
        print(f"Open positions count: {round(index+1):,}")
        print(f"Total positions size: {round(_totalSize/PRICE_PRECISION):,}")
        print(f"Realized PnL: {round(_totalPnL/PRICE_PRECISION):,}")
        print(f"Unrealized PnL: {round(_totalDelta/PRICE_PRECISION):,}")
        print(f"Paid fees: {round(_totalFee/PRICE_PRECISION):,}")
        print(f"Outstanding borrow fees: {round(_totalFundingFee/PRICE_PRECISION):,}")
        print(f"Closing fees: {round(_totalSize/PRICE_PRECISION/1000):,}")

        return {'open_positions_count': round(index+1),
                'total_open_interest': round(_totalSize/PRICE_PRECISION),
                'realised_pnl': round(_totalPnL/PRICE_PRECISION),
                'unrealized_pnl': round(_totalDelta/PRICE_PRECISION),
                'paid_fees': round(_totalFee/PRICE_PRECISION),
                'outstanding_borrow_fees': round(_totalFundingFee/PRICE_PRECISION),
                'closing_fees': round(_totalSize/PRICE_PRECISION/1000)}


if __name__ == '__main__':

    pending_fees_dict = GetPendingFees().get_pending_fees()
