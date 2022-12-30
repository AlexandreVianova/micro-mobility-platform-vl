import abc
from typing import List
import pandas as pd
from pandas import DataFrame


handled_systems = ['capital-bikeshare', 'velib']

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


class BikeShareDataTransformerInterface(metaclass=abc.ABCMeta):
    system_name = None

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'prepare_stations_inventory') and
                callable(subclass.prepare_stations_inventory) and
                hasattr(subclass, 'prepare_stations_statuses') and
                callable(subclass.prepare_stations_statuses) or
                NotImplemented)

    @classmethod
    @abc.abstractmethod
    def prepare_stations_inventory(cls, df: DataFrame) -> DataFrame:
        """
        Compute stations inventory from extracted data
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def prepare_stations_statuses(cls, df: DataFrame) -> DataFrame:
        """
        Compute stations statuses from extracted data
        """
        raise NotImplementedError


class VelibDataTransformer(BikeShareDataTransformerInterface):
    """
    Data Transformer for Vélib system (Paris, FR)
    """
    system_name = 'velib'

    @classmethod
    def prepare_stations_inventory(cls, df: DataFrame) -> DataFrame:
        df['extra.has_ebikes'] = None
        df = df[[
            'extra.station_id',
            'name',
            'latitude',
            'longitude',
            'extra.banking',
            'extra.payment',
            'extra.payment-terminal',
            'extra.last_updated',
            'extra.has_ebikes',
            'extra.slots',
        ]]
        df = df.rename(columns={
            'extra.station_id': 'id',
            'extra.banking': 'banking',
            'extra.payment': 'payment',
            'extra.payment-terminal': 'payment_terminal',
            'extra.last_updated': 'last_updated',
            'extra.has_ebikes': 'has_ebikes',
            'extra.slots': 'slots'
        })
        df['payment'] = df['payment'].apply(lambda x: parse_payment_string(x))
        df = df.astype(dtype=station_inventory_dtypes)
        return df

    @classmethod
    def prepare_stations_statuses(cls, df: DataFrame) -> DataFrame:
        df = df[[
            'extra.station_id',
            'timestamp',
            'bikes',
            'free',
            'extra.renting',
            'extra.returning',
            'extra.ebikes',
        ]]
        df = df.rename(columns={
            'extra.station_id': 'station_id',
            'extra.renting': 'renting',
            'extra.returning': 'returning',
            'extra.ebikes': 'ebikes'
        })
        df['timestamp'] = pd.to_datetime(df['timestamp'], infer_datetime_format=True)
        df = df.astype(dtype=station_statuses_history_dtypes)
        return df


class CapitalBikeShareDataTransformer(BikeShareDataTransformerInterface):
    """
    Data Transformer for capital Bikeshare system (Washington DC, USA)
    """
    system_name = 'capital-bikeshare'

    @classmethod
    def prepare_stations_inventory(cls, df: DataFrame) -> DataFrame:
        df['banking'] = None
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
        df['payment'] = df['payment'].apply(lambda x: parse_payment_string(x))
        df = df.astype(dtype=station_inventory_dtypes)
        return df

    @classmethod
    def prepare_stations_statuses(cls, df: DataFrame) -> DataFrame:
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

        df = df.astype(dtype=station_statuses_history_dtypes)
        return df


def parse_payment_string(x) -> List:
    if isinstance(x, List):
        return x
    elif isinstance(x, str):
        return x.strip('][').split(', ')
