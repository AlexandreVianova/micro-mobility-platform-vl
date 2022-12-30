WITH STATUS_HISTORY_LAST_24H AS (
    SELECT
        station_id,
        date_trunc('hour',timestamp) as hour,
        bikes,
        free,
        ebikes
    FROM stations.status_history
    WHERE
        timestamp BETWEEN
            (now() - INTERVAL '24 hours')
        	AND now()
),

STATIONS_INVENTORY AS (
    SELECT
        stations.inventory.id as station_id,
        name,
        system_name,
        city,
        country
	FROM stations.inventory
	INNER JOIN systems.inventory
	USING (system_name)
),

HOURLY_AGGS AS (
    SELECT
        station_id,
        hour,
        -- aggregate bike numbers per hour
        AVG(bikes) AS avg_bikes,
        MIN(bikes) AS min_bikes,
        MAX(bikes) AS max_bikes,
        AVG(free) AS avg_free,
        MIN(free) AS min_free,
        MAX(free) AS max_free,
        AVG(ebikes) AS avg_ebikes,
        MIN(ebikes) AS min_ebikes,
        MAX(ebikes) AS max_ebikes
    FROM STATUS_HISTORY_LAST_24H
    GROUP BY station_id, hour
),

HOURLY_STATS AS (
    SELECT
        system_name,
        country,
        city,
        name,
        hour,
        avg_bikes,
       	min_bikes,
        max_bikes,
        AVG(AVG_BIKES) OVER (
            PARTITION BY name ORDER BY hour ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_avg_bikes_3h,
        avg_free,
        min_free,
       	max_free,
       	AVG(AVG_FREE) OVER (
            PARTITION BY name ORDER BY hour ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_avg_free_3h,
        avg_ebikes,
        min_ebikes,
        max_ebikes,
        AVG(AVG_FREE) OVER (
            PARTITION BY name ORDER BY hour ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_avg_ebikes_3h
	FROM HOURLY_AGGS
	INNER JOIN STATIONS_INVENTORY
    USING (station_id)
)

SELECT * FROM HOURLY_STATS;