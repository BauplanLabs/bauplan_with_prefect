import bauplan


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.10', pip={'polars': '1.33'})
def my_parent(
    trips=bauplan.Model(
        'taxi_fhvhv',
        columns=[
            'pickup_datetime',
            'PULocationID',
            'DOLocationID',
            'trip_miles',
            ],
        filter="pickup_datetime >= '2023-03-15T00:00:00-05:00' AND pickup_datetime < '2023-04-01T00:00:00-05:00'"
    ),
    zones=bauplan.Model('taxi_metadata'),
):
    import polars as pl
    print("\n\n!!!! Bauplan <3 Prefect !!!!\n\n")
    trips_df = pl.from_arrow(trips)
    zones_df = pl.from_arrow(zones)
    joined = trips_df.join(zones_df, left_on='DOLocationID', right_on='LocationID')
    return joined.to_arrow()


# @bauplan.model(materialization_strategy='REPLACE')
# @bauplan.python('3.10', pip={'duckdb': '1.1.3'})
# def my_parent(
#     trips=bauplan.Model(
#         'taxi_fhvhv',
#         columns=[
#             'pickup_datetime',
#             'PULocationID',
#             'DOLocationID',
#             'trip_miles',
#             ],
#         filter="pickup_datetime >= '2023-03-01T00:00:00-05:00' AND pickup_datetime < '2023-04-01T00:00:00-05:00'"
#     ),
#     zones=bauplan.Model('taxi_metadata'),
# ):
#     import duckdb
#     print("\n\n!!!! Bauplan <3 Prefect !!!!\n\n")
#     con = duckdb.connect(':memory:')
#     con.register('trips', trips)
#     con.register('zones', zones)
#     joined = con.execute("""
#         SELECT trips.*, zones.*
#         FROM trips
#         JOIN zones ON trips.DOLocationID = zones.LocationID
#     """).fetch_arrow_table()
#     con.close()
#     return joined


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'polars': '1.35.2'})
def my_child(
    my_trips=bauplan.Model('my_parent'),
):
    import polars as pl
    df = pl.from_arrow(my_trips)
    df = df.filter(pl.col("trip_miles") > 0.0)
    print(f"\n\nGot {len(df)} rows after filtering\n\n")
    return df.to_arrow()