import abc
import json
import logging
import pandas as pd
from pandas import DataFrame
import pybikes

logger = logging.getLogger()


class BikeShareDataExtractorInterface(metaclass=abc.ABCMeta):
    """
    Classes implementing this interface are responsible
    for extracting data from remote bike sharing systems.
    """
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'extract_data') and
                callable(subclass.extract_data) and
                hasattr(subclass, 'extract_metadata') and
                callable(subclass.extract_metadata) or
                NotImplemented)

    @classmethod
    @abc.abstractmethod
    def extract_data(cls, system_name: str) -> DataFrame:
        """
        Extracts data for a given system name
        and returns a pandas DataFrame
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def extract_metadata(cls, system_name: str) -> dict:
        """
        Extracts metadata for a given system name
        and returns a pandas DataFrame
        """
        raise NotImplementedError


class PyBikesDataExtractor(BikeShareDataExtractorInterface):
    """
    Heavily relies on pybikes to simplify
    interactions with required APIs.
    """

    @classmethod
    def extract_metadata(cls, system_name: str) -> dict:
        system: pybikes.BikeShareSystem = pybikes.get(system_name)
        meta = system.meta
        if 'license' in meta.keys():
            meta['license'] = json.dumps(meta['license'])
        return meta

    @classmethod
    def extract_data(cls, system_name: str) -> DataFrame:
        logger.debug(f'Instantiating pybikes object for {system_name}')
        system = pybikes.get(system_name)
        logger.info(f'Beginning ingestion for GBFS system {system_name}')
        system.update()
        logger.info(f'Found {len(system.stations)} stations status for system {system_name}')
        df_stations = pd.json_normalize([s.__dict__ for s in system.stations])
        return df_stations
