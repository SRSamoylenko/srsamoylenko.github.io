from etl.config import Config
from etl.etl import create_tables

cfg = Config()
create_tables(cfg)
