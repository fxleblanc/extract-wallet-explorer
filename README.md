# Wallet Explorer
Simple script to extract nodes and transactions related to bitcoin addresses. Uses https://www.walletexplorer.com/ as it's source.

## Usage

1. Clone the repo with `git clone https://github.com/fxleblanc/extract-wallet-explorer`
2. Initialize a python env with `virtualenv -p python env`
3. Install python dependencies with `pip install -r requirements.txt`
4. Launch the script with a single address: `python extract_wallet_explorer.py -a [btc_address]`
5. Launch the script with a file containing multiple addresses: `python extract_wallet_explorer.py -f [file]`
