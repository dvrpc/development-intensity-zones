import pandas as pd


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


state_id_values = [
    item
    for sublist in db.query("SELECT DISTINCT state_id FROM _resources.county_key")
    for item in sublist
]  # Simultaneously gets county_key's unique state_id values, and flattens its list result

county_id_3dig_values = [
    [
        item
        for sublist in db.query(
            "SELECT DISTINCT county_id_3dig FROM _resources.county_key WHERE state_id = '" + i + "'"
        )
        for item in sublist
    ]
    for i in state_id_values
]  # For each state_id value, simultaneously gets its unique county_id_3dig values, and flattens its list result

county_id_3dig_values = dict(
    zip(state_id_values, county_id_3dig_values)
)  # Turns county_id_3dig_values into a dictionary, with each value being given its state_id for the key
