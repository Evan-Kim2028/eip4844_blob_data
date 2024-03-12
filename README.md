### Blob Dashboard
Dencun blob market analysis and dashboard powered by [Panel](https://panel.holoviz.org/) and [ethpandaops](https://docs.ethpandaops.io/xatu/Clickhouse/intro).


### Getting Started
This repository uses [rye](https://rye-up.com/guide/) and Python 3.1.18. To get started, use `rye sync` to start the virtual environment and install dependencies. 


### Environment Variables for Clickhouse Login
To run the clickhouse queries, add `DATABASE_URL` to your environment variables with the following format where XXX_USERNAME_XXX and XXX_PASSWORD_XXX are replaced with your credentials:
```
DATABASE_URL = "clickhouse+http://XXX_USERNAME_XXX:XXX_PASSWORD_XXX@clickhouse.analytics.production.platform.ethpandaops.io:443/default?protocol=https"
```

### Getting Started
There are example queries in jupyter notebooks using both sqlalchemy and the python clickhouse client.