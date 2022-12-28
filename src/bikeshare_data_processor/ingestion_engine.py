import os
import logging
import pandas as pd
from typing import List

import psycopg2
from psycopg2.extensions import connection

import pybikes


logger = logging.getLogger(__name__)


class IngestionEngine:
    """
    This class is responsible for the ingestion of target mobility systems,
    typically bike sharing systems. It heavily relies on pybikes to simplify
    interactions with required APIs.
    """
    station_inventory_dtypes = {
        'id': str,
        'name': str,
        'latitude': float,
        'longitude': float,
        'payment_terminal': bool,
        'last_updated': int,
        'has_ebikes': bool,
        'slots': int
    }

    station_statuses_history_dtypes = {
        'station_id': str,
        'bikes': int,
        'free': int,
        'renting': int,
        'returning': int,
        'ebikes': int
    }

    cnx_params = {
        'host': os.environ.get('DB_HOST'),
        'port': str(os.environ.get('DB_PORT')),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'database': os.environ.get('DB_NAME')
    }

    @classmethod
    def ingest_system_data(cls, system_name: str) -> None:
        """
        TODO: docstring
        :param system_name:
        :return:
        """
        # Fetch data and preprocess it
        df_raw = cls.fetch_system_data(system_name)
        df_raw['extra.uid'] = df_raw['extra.uid'].apply(lambda x: f'{system_name}-{x}')
        df_station_statuses = cls.prepare_stations_statuses(df_raw)
        df_stations_inventory = cls.prepare_station_inventory(df_raw)

        # Write to Postgres
        cls.write_station_statuses(df_station_statuses)
        cls.upsert_stations_inventory(df_stations_inventory)

    @classmethod
    def fetch_system_data(cls, system_name: str) -> pd.DataFrame:
        """
        TODO: docstring
        :param system_name:
        :return:
        """
        logger.debug(f'Instantiating pybikes object for {system_name}')
        system = pybikes.get(system_name)
        logger.info(f'Beginning ingestion for GBFS system {system_name}')
        system.update()
        logger.info(f'Found {len(system.stations)} stations status for system {system_name}')
        df_stations = pd.json_normalize([s.__dict__ for s in system.stations])
        return df_stations

    @classmethod
    def prepare_stations_statuses(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        TODO: docstring
        :param df:
        :return:
        """
        df = df[[
            'extra.uid',
            'timestamp',
            'bikes',
            'free',
            'extra.renting',
            'extra.returning',
            'extra.ebikes',
        ]]
        df = df.rename(columns={
            'extra.uid': 'station_id',
            'extra.renting': 'renting',
            'extra.returning': 'returning',
            'extra.ebikes': 'ebikes'
        })

        df['timestamp'] = pd.to_datetime(df['timestamp'], infer_datetime_format=True)

        df = df.astype(dtype=cls.station_statuses_history_dtypes)
        return df

    @classmethod
    def prepare_station_inventory(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        TODO: docstring
        :param df:
        :return:
        """
        df = df[[
            'extra.uid',
            'name',
            'latitude',
            'longitude',
            'extra.payment',
            'extra.payment-terminal',
            'extra.last_updated',
            'extra.has_ebikes',
            'extra.slots',
        ]]
        df = df.rename(columns={
            'extra.uid': 'id',
            'extra.payment': 'payment',
            'extra.payment-terminal': 'payment_terminal',
            'extra.last_updated': 'last_updated',
            'extra.has_ebikes': 'has_ebikes',
            'extra.slots': 'slots'
        })
        df['payment'] = df['payment'].apply(lambda x: cls.parse_payment_string(x))
        df = df.astype(dtype=cls.station_inventory_dtypes)
        return df

    @classmethod
    def write_station_statuses(cls, df_station_statuses: pd.DataFrame) -> None:
        """
        TODO: docstring
        :param df_station_statuses:
        :return:
        """
        sql = """
        INSERT INTO stations.status_history(station_id, timestamp, bikes, free, ebikes, renting)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = [
            (
                row.station_id,
                row.timestamp,
                row.bikes,
                row.free,
                row.ebikes,
                row.renting
            )
            for (index, row) in df_station_statuses.iterrows()
        ]
        cnx = cls.create_pg_connection()
        cursor = cnx.cursor()
        cursor.executemany(sql, values)
        cnx.close()

    @classmethod
    def upsert_stations_inventory(cls, df_stations_inventory: pd.DataFrame) -> None:
        """
        TODO: docstring
        :param df_stations_inventory:
        :return:
        """
        sql = """
        INSERT INTO stations.inventory(
            id, name, latitude, longitude, payment,
            payment_terminal, has_ebikes, slots, last_updated
        ) 
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id)
        DO UPDATE SET last_updated = EXCLUDED.last_updated, slots = EXCLUDED.slots;
        """
        values = [
            (
                row.id,
                row.name,
                row.latitude,
                row.longitude,
                row.payment,
                row.payment_terminal,
                row.has_ebikes,
                row.slots,
                row.last_updated
            )
            for (index, row) in df_stations_inventory.iterrows()
        ]
        cnx = cls.create_pg_connection()
        cursor = cnx.cursor()
        cursor.executemany(sql, values)
        cnx.close()

    @classmethod
    def create_pg_connection(cls) -> connection:
        """
        TODO: docstring
        :return:
        """
        cnx = psycopg2.connect(**cls.cnx_params)
        cnx.autocommit = True
        cur = cnx.cursor()
        cur.execute('SELECT version();')
        logger.debug('Postgres database version: %s', cur.fetchone()[0])
        return cnx

    @staticmethod
    def parse_payment_string(x) -> List:
        if isinstance(x, List):
            return x
        elif isinstance(x, str):
            return x.strip('][').split(', ')


if __name__ == '__main__':
    IngestionEngine.ingest_system_data('capital-bikeshare')
