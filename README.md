### Blob Dashboard
Dencun blob market dashboard and analysis powered by [Panel](https://panel.holoviz.org/) and [ethpandaops](https://docs.ethpandaops.io/xatu/Clickhouse/intro). 
The dashboard can be found [here](https://analytics.mev-commit.xyz/dashboard). Note this dashboard uses data from ethpandaops, which requires access request. 

### Getting Started
1. This repository uses [rye](https://rye-up.com/guide/) to manage dependencies and the virtual environment. To install, refer to this link for instructions here.
2. Once rye is installed, run rye sync to install dependencies and setup the virtual environment, which has a default name of .venv.
3. Activate the virtual environment with the command source .venv/bin/activate.
4. To run the panel dashboard locally, use `panel serve panel/beacon_block_blob_size.ipynb`

