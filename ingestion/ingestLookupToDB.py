#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from config.default import POSTGRES_CONN_STRING


def main():

    engine = create_engine(POSTGRES_CONN_STRING)

    df = pd.DataFrame()
    df_reader = pd.read_csv('data/taxi+_zone_lookup.csv', iterator=True, chunksize=100)

    for chunk in df_reader:
        df = df.append(chunk)

    df.to_sql('taxi_zone_lookup', engine, if_exists='replace', index=False)

if __name__ == '__main__':
    main()