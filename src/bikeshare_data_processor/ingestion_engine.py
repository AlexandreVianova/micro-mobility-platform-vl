import logging

from bikeshare_data_processor.extractors import PyBikesDataExtractor as DataExtractor
from bikeshare_data_processor.transformers import BikeShareDataTransformerInterface,\
    VelibDataTransformer, CapitalBikeShareDataTransformer, handled_systems
from bikeshare_data_processor.database_handlers import PostgresHandler as DbHandler


logger = logging.getLogger()


class IngestionEngine:
    """
    This class is responsible for the
    ingestion of bike sharing systems.
    """
    handled_systems = handled_systems
    transformer = BikeShareDataTransformerInterface

    @classmethod
    def ingest_system_data(cls, system_name: str) -> None:
        """
        TODO: docstring
        :param system_name:
        :return:
        """
        # Fetch data and preprocess it
        df_raw = DataExtractor.extract_data(system_name)
        meta = DataExtractor.extract_metadata(system_name)
        
        # Pick the right transformer
        if system_name == CapitalBikeShareDataTransformer.system_name:
            cls.transformer = CapitalBikeShareDataTransformer
        elif system_name == VelibDataTransformer.system_name:
            cls.transformer = VelibDataTransformer

        # System-specific transformations
        df_status = cls.transformer.prepare_stations_statuses(df_raw)
        df_inventory = cls.transformer.prepare_stations_inventory(df_raw)

        # Common transformations
        df_status['station_id'] = df_status['station_id'].apply(lambda x: f'{cls.transformer.system_name}-{x}')
        df_inventory['id'] = df_inventory['id'].apply(lambda x: f'{cls.transformer.system_name}-{x}')

        # Write to backend database
        DbHandler.write_stations_statuses(df_status)
        DbHandler.upsert_stations_inventory(df_inventory, meta)
        DbHandler.upsert_systems_inventory(meta)


if __name__ == '__main__':
    IngestionEngine.ingest_system_data(VelibDataTransformer.system_name)
    IngestionEngine.ingest_system_data(CapitalBikeShareDataTransformer.system_name)
