"""GADM Service"""
import logging
import os

import sqlalchemy


class GADMService:

    def __init__(self):
        self.engine = sqlalchemy.create_engine(os.getenv("POSTGRES_URL"))

    def points_outside_land(self, locations):
        """Return the subset of locations that fall outside any land polygon in gadm36_0."""
        if not locations:
            return []

        values = ", ".join(
            f"({loc['lat']}, {loc['lng']})" for loc in locations
        )

        sql = f"""
            SELECT p.lat, p.lng
            FROM (VALUES {values}) AS p(lat, lng)
            WHERE NOT EXISTS (
                SELECT 1 FROM gadm36_0 g
                WHERE ST_Contains(
                    g.the_geom,
                    ST_SetSRID(ST_MakePoint(p.lng, p.lat), 4326)
                )
            )
        """

        logging.debug(f"[SERVICE] [gadm_service] sql: {sql}")

        result = self.engine.execute(sql)
        return [{"lat": float(row[0]), "lng": float(row[1])} for row in result]
