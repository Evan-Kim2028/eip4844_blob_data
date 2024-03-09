### Getting Started

1. Install virtual environment with uv `uv .venv -python 3.11`
2. Install relevant drivers (todo, add a requirements txt)


### Environment Variables for Clickhouse Login
To run the clickhouse queries, add `DATABASE_URL` to your environment variables with the following format where XXX_USERNAME_XXX and XXX_PASSWORD_XXX are replaced with your credentials:
```
DATABASE_URL = "clickhouse+http://XXX_USERNAME_XXX:XXX_PASSWORD_XXX@clickhouse.analytics.production.platform.ethpandaops.io:443/default?protocol=https"
```

### Getting Started
There are example queries in jupyter notebooks using both sqlalchemy and the python clickhouse client.