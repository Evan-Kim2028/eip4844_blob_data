### Blob Dashboard
Dencun blob market analysis and dashboard powered by [Panel](https://panel.holoviz.org/) and [ethpandaops](https://docs.ethpandaops.io/xatu/Clickhouse/intro).

### Getting Started
This repository uses [rye](https://rye-up.com/guide/) and `Python 3.11.18`. To get started, use `rye sync` to setup the virtual environment and install dependencies. [See this repository](https://github.com/Evan-Kim2028/ethpandaops_python) for how to get started accessing clickhouse
There are example queries in jupyter notebooks using both sqlalchemy and the python clickhouse client.

### Panel Dashboard (WIP)
The panel dashboard can be started locally with `panel serve panel/beacon_block_blob_size.ipynb`. All dashboard charts can be found in the `panel` folder.