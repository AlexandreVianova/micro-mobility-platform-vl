import abc
import logging
import os

import psycopg2
from psycopg2.extensions import connection
from pandas import DataFrame

logger = logging.getLogger()


class DatabaseHandler(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'write_stations_statuses') and
                callable(subclass.write_stations_statuses) and
                hasattr(subclass, 'upsert_stations_inventory') and
                callable(subclass.upsert_stations_inventory) and
                hasattr(subclass, 'upsert_systems_inventory') and
                callable(subclass.upsert_systems_inventory) and
                hasattr(subclass, 'extract_hourly_aggregations') and
                callable(subclass.extract_hourly_aggregations) or
                NotImplemented)

    @classmethod
    @abc.abstractmethod
    def write_stations_statuses(cls, df: DataFrame) -> None:
        """
        Load stations statuses to backend db
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def upsert_stations_inventory(cls, df: DataFrame, metadata: dict) -> None:
        """
        Load stations inventory to backend db
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def upsert_systems_inventory(cls, metadata: dict) -> None:
        """
        Load stations inventory to backend db
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def extract_hourly_aggregations(cls) -> DataFrame:
        """
        Perform hourly aggregations using backend db capacities
        """
        raise NotImplementedError


class PostgresHandler(DatabaseHandler):
    """
    Handles backend storage and aggregations computing
    for PostgreSQL.
    """
    cnx_params = {
        'host': os.environ.get('DB_HOST'),
        'port': str(os.environ.get('DB_PORT')),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'database': os.environ.get('DB_NAME')
    }

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

    @classmethod
    def write_stations_statuses(cls, df: DataFrame) -> None:
        sql = """
        INSERT INTO stations.status_history(station_id, timestamp, bikes, free, ebikes, renting)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = [
            (
                row['station_id'],
                row['timestamp'],
                row['bikes'],
                row['free'],
                row['ebikes'],
                row['renting']
            )
            for (index, row) in df.iterrows()
        ]
        cnx = cls.create_pg_connection()
        cursor = cnx.cursor()
        cursor.executemany(sql, values)
        cnx.close()

    @classmethod
    def upsert_stations_inventory(cls, df: DataFrame, metadata: dict) -> None:
        sql = """
        INSERT INTO stations.inventory(
            id, system_name, name, latitude, longitude, payment,
            payment_terminal, has_ebikes, slots, last_updated
        ) 
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id)
        DO UPDATE SET last_updated = EXCLUDED.last_updated, slots = EXCLUDED.slots;
        """
        values = [
            (
                row['id'],
                metadata['name'],
                row['name'],
                row['latitude'],
                row['longitude'],
                row['payment'],
                row['payment_terminal'],
                row['has_ebikes'],
                row['slots'],
                row['last_updated']
            )
            for (index, row) in df.iterrows()
        ]
        cnx = cls.create_pg_connection()
        cursor = cnx.cursor()
        cursor.executemany(sql, values)
        cnx.close()

    @classmethod
    def upsert_systems_inventory(cls, metadata: dict) -> None:
        sql = """
        INSERT INTO systems.inventory(
            system_name, city, country, latitude, longitude, 
            company, license, ebikes, gbfs_href
        ) 
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (system_name)
        DO NOTHING;
        """
        values = (
            metadata.get('name'),
            metadata.get('city'),
            metadata.get('country'),
            metadata.get('latitude'),
            metadata.get('longitude'),
            metadata.get('company'),
            metadata.get('license'),
            metadata.get('ebikes'),
            metadata.get('gbfs_href')
        )
        cnx = cls.create_pg_connection()
        cursor = cnx.cursor()
        cursor.execute(sql, values)
        cnx.close()

    @classmethod
    def extract_hourly_aggregations(cls) -> DataFrame:
        from pathlib import Path
        path = f'{str(Path(__file__).parent.resolve())}/sql/hourly_aggs.sql'
        with open(str(path), 'r') as f:
            sql = f.read()
        cnx = cls.create_pg_connection()
        cursor = cnx.cursor()
        cursor.execute(sql)
        cols = [col.name for col in cursor.description]
        rows = cursor.fetchall()
        cnx.close()
        return DataFrame(rows, columns=cols)
