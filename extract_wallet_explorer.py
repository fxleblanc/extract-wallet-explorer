#!/usr/bin/env python
# coding: utf-8
import datetime
import json
import urllib
import re
import logging
import pandas as pd
import numpy as np
import requests
import argparse
from bs4 import BeautifulSoup


logger = logging.getLogger('extract_wallet_explorer')
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')

fileHandler = logging.FileHandler('debug.log')
fileHandler.setLevel(logging.DEBUG)
fileHandler.setFormatter(formatter)

logger.addHandler(fileHandler)

today = datetime.datetime.now().strftime("%Y-%m-%d")
url = (
    "https://api.coindesk.com/v1/bpi/historical/close.json?start=2010-07-17&end="
    + today
)
conversion_table = json.loads(
    urllib.request.urlopen(url).read().decode("utf8")
)["bpi"]
conversion_table[today] = json.loads(
    urllib.request.urlopen(
        "https://api.coindesk.com/v1/bpi/currentprice/USD.json"
    )
    .read()
    .decode("utf8")
)["bpi"]["USD"]["rate_float"]

base_url = 'https://www.walletexplorer.com'

HOP_THRESHOLD = 2

def get_node_from_address(address):
    resp = requests.get(f"{base_url}/?q={address}")
    soup = BeautifulSoup(resp.text, 'html.parser')
    tx_csv_url = soup.find('a', string="Download as CSV")['href']
    return re.match('/wallet/(\w+)\?format=csv', tx_csv_url).group(1)

def transactions_from_node_id(node_id, current_hop):
    logger.debug(f'crawling node {node_id} with hop {current_hop}')
    url = f'{base_url}/wallet/{node_id}?format=csv'
    transactions = pd.read_csv(url, skiprows=1)
    
    # Remove the fee transactions
    transactions = transactions[~transactions['sent to'].isin(['(fee)'])]
    transactions['hop'] = np.where(transactions['received from'].isna(), current_hop, -current_hop)
    
    new_hop = current_hop + 1
    new_transactions = None
    if new_hop <= HOP_THRESHOLD:
        new_nodes = transactions['received from'].append(transactions['sent to']).str.extractall('(\w{16})')[0].tolist()
        tx_list = [transactions_from_node_id(x, new_hop) for x in new_nodes]
        new_transactions = pd.concat(tx_list)
    
    # Replace na values by the current node_id
    transactions['received from'].fillna(node_id, inplace=True)
    transactions['sent to'].fillna(node_id, inplace=True)
    
    # Set node_id
    transactions['node_id'] = [node_id] * transactions.shape[0]
    
    return transactions.append(new_transactions)

def crawl(address):
    logger.debug(f'crawling address {address}')
    node_id = get_node_from_address(address)
    transactions = transactions_from_node_id(node_id, 1)
    transactions['price_usd'] = transactions['date'].str.split(' ').str[0].apply(lambda x: conversion_table[x])
    transactions['sum_usd'] = np.where(transactions['received amount'].isna(), transactions['sent amount'] * transactions['price_usd'], transactions['received amount'] * transactions['price_usd'])

    # Write transactions
    col_map = {'received from': 'Source', 'sent to': 'Target', 'sum_usd': 'Weight'}
    final_txs = transactions[col_map.keys()].rename(columns=col_map)
    final_txs.to_csv(f'{address}({node_id})_ties.csv', index=False)

    # Write nodes
    transactions['other_node'] = np.where(transactions['received from'] == transactions['node_id'], transactions['sent to'], transactions['received from'])
    col_map = {'node_id': 'Id', 'other_node': 'Label', 'hop': 'hops'}
    final_nodes = transactions[col_map.keys()].rename(columns=col_map)
    final_nodes.to_csv(f'{address}({node_id})_nodes.csv', index=False)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--address", help="Specify an address to crawl on wallet explorer")
    parser.add_argument("-f", "--file", help="Specify a file containing a list of addresses to crawl on wallet explorer")
    args = parser.parse_args()
    if args.file:
        with open(args.file, "r") as addr_file:
            for address in addr_file:
                crawl(address.rstrip())
    if args.address:
        crawl(args.address)

if __name__ == "__main__":
    main()
