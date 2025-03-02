from datetime import datetime
from typing import NamedTuple

from data_connectors.connector.snowflake_python.snowflake_python import SnowflakePython
from pydantic import BaseModel, Field, validator

from src.modules.pipelines.price_settings import (
    EXPECTED_NAN_EXPRESSION,
    HIGH_ATP_INCREASE_THRESHOLD,
    HIGH_OCCUPANCIES_THRESHOLD,
    HIGH_SOLD_OUT_DAYS_DIFF_THRESHOLD,
    LARGE_ATP_DIFFERENCE_THRESHOLD,
    LOW_OCCUPANCIES_THRESHOLD,
    LOW_SOLD_OUT_DAYS_DIFF_THRESHOLD,
    MED_SOLD_OUT_DAYS_DIFF_THRESHOLD,
    SMALL_ATP_DIFFERENCE_THRESHOLD,
    VENUE_ID_TYPE,
)


class VenueZone(BaseModel):
    ds_city_country: str = Field(alias="DS_CITY_COUNTRY")
    ds_venue: str = Field(alias="DS_VENUE")
    id_venue: int = Field(alias="ID_VENUE")
    seat_category: str = Field(alias="SEAT_CATEGORY")
    venue_last_date: datetime = Field(alias="VENUE_LAST_DATE")
    ds_label: str = Field(alias="DS_LABEL")
    n_labels: int = Field(alias="N_LABELS")
    all_labels: str = Field(alias="ALL_LABELS")
    n_concerts: int = Field(alias="N_CONCERTS")
    previous_occupancies_venue_zone: float = Field(
        alias="PREVIOUS_OCCUPANCIES_VENUE_ZONE"
    )
    previous_occupancies_venue: float = Field(
        alias="PREVIOUS_OCCUPANCIES_VENUE"
    )
    previous_sales_venue_zone: float = Field(alias="PREVIOUS_SALES_VENUE_ZONE")
    previous_capacities_venue_zone: float = Field(
        alias="PREVIOUS_CAPACITIES_VENUE_ZONE"
    )
    previous_first_price_venue_zone: float = Field(
        alias="PREVIOUS_FIRST_PRICE_VENUE_ZONE"
    )
    previous_atp_venue_zone: float = Field(alias="PREVIOUS_ATP_VENUE_ZONE")
    previous_atp_increase_venue_zone: float = Field(
        alias="PREVIOUS_ATP_INCREASE_VENUE_ZONE"
    )
    previous_atp_increase_venue: float = Field(
        alias="PREVIOUS_ATP_INCREASE_VENUE"
    )
    previous_avg_sold_out_days_venue_zone: float = Field(
        alias="PREVIOUS_AVG_SOLD_OUT_DAYS_VENUE_ZONE"
    )
    previous_avg_sold_out_days_venue: float = Field(
        alias="PREVIOUS_AVG_SOLD_OUT_DAYS_VENUE"
    )
    previous_concerts_venue_zone: int = Field(alias="PREVIOUS_CONCERTS_VENUE_ZONE")
    recurrent_venue: bool = Field(alias="RECURRENT_VENUE")
    avg_atp_increase_venue: float = Field(alias="AVG_ATP_INCREASE_VENUE")
    previous_atp_increase_zone_difference: float = Field(
        alias="PREVIOUS_ATP_INCREASE_ZONE_DIFFERENCE"
    )
    avg_sold_out_days_venue: float = Field(alias="AVG_SOLD_OUT_DAYS_VENUE")
    previous_avg_sold_out_days_zone_difference: float = Field(
        alias="PREVIOUS_AVG_SOLD_OUT_DAYS_ZONE_DIFFERENCE"
    )
    previous_sold_outs_zone: int = Field(alias="PREVIOUS_SOLD_OUTS_ZONE")
    order_price: float = Field(alias="ORDER_PRICE")
    previous_first_price_superior_venue_zone: float | None = Field(
        alias="PREVIOUS_FIRST_PRICE_SUPERIOR_VENUE_ZONE"
    )
    id_city: int = Field(alias="ID_CITY")
    cd_city: str = Field(alias="CD_CITY")
    ds_country: str = Field(alias="DS_COUNTRY")
    ds_city: str = Field(alias="DS_CITY")
    id_country: int = Field(alias="ID_COUNTRY")
    currency: str = Field(alias="CURRENCY")

    class Config:
        allow_population_by_field_name = True

    @validator("previous_first_price_superior_venue_zone", pre=True)
    def format_previous_first_price_superior_venue_zone(
            cls, value: str
    ) -> float | None:
        return float(value) if value and value != EXPECTED_NAN_EXPRESSION else None


class VenueZoneFeature(BaseModel):
    id_venue: int
    ds_venue: str
    ds_city_country: str
    id_city: int
    cd_city: str
    ds_country: str
    ds_city: str
    id_country: int
    currency: str
    ds_seat_category: str
    order_price: float
    previous_first_price_venue_zone: float
    previous_first_price_superior_venue_zone: float
    price_influence_range: float
    price_influence_range_multiplier: float
    price_influence_range_adjusted: float
    min_price: float
    total_range: float
    total_range_adjusted: float
    previous_occupancies_venue: float
    previous_atp_increase_venue: float
    previous_atp_increase_venue_zone: float
    previous_avg_sold_out_days_venue: float
    previous_avg_sold_out_days_venue_zone: float
    relative_price_multiplier: float
    price_influence_range_adjusted_cumsum: float
    price_influence_range_adjusted_cumsum_lag: float
    booking_fee: float | None
    sales_tax_outside_ticket_price: float | None


class AggregatedInternalFeatures(BaseModel):
    min_price: float
    total_range: float
    total_range_adjusted: float

    def aggregate_min_price(self, value: float) -> None:
        self.min_price = min(self.min_price, value)

    def aggregate_total_range(self, value: float) -> None:
        self.total_range += value

    def aggregate_total_range_adjusted(self, value: float) -> None:
        self.total_range_adjusted += value


class InternalPrecursorFeatures(NamedTuple):
    id_venue: int
    previous_first_price_venue_zone: float
    price_influence_range: float
    price_influence_range_adjusted: float
    price_influence_range_adjusted_cumsum: float
    price_influence_range_adjusted_cumsum_lag: float
    prev_first_price_superior_venue_zone: float
    price_influence_range_multiplier: float


class VenueZoneFeatures(BaseModel):
    features: list[VenueZoneFeature]
    venue_zone_data: list[VenueZone]

    @staticmethod
    def _price_intervention(previous_occupancies_venue: float, previous_atp_increase_venue: float,
                            previous_avg_sold_out_days_venue: float, recurrent_venue: bool) -> float:
        is_high_occupancy = previous_occupancies_venue > HIGH_OCCUPANCIES_THRESHOLD
        is_low_occupancy = previous_occupancies_venue < LOW_OCCUPANCIES_THRESHOLD
        is_high_atp_increase = (
                previous_atp_increase_venue > HIGH_ATP_INCREASE_THRESHOLD
        )
        fast_sold_out = (
                previous_avg_sold_out_days_venue > HIGH_SOLD_OUT_DAYS_DIFF_THRESHOLD
        )

        if recurrent_venue:
            if is_low_occupancy:
                return 0.9
            elif is_high_occupancy and (is_high_atp_increase or fast_sold_out):
                return 1.05
        return 1

    @staticmethod
    def _price_influence_range_increase(venue_zone: VenueZone) -> float:
        avg_sold_out_decreased_significantly = (
                venue_zone.previous_avg_sold_out_days_zone_difference
                < -MED_SOLD_OUT_DAYS_DIFF_THRESHOLD
        )
        avg_sold_out_decreased_moderately = (
                venue_zone.previous_avg_sold_out_days_zone_difference
                < -LOW_SOLD_OUT_DAYS_DIFF_THRESHOLD
        )
        atp_decreased_significantly = (
                venue_zone.previous_atp_increase_zone_difference
                < -LARGE_ATP_DIFFERENCE_THRESHOLD
        )
        atp_decreased_moderately = (
                venue_zone.previous_atp_increase_zone_difference
                < -SMALL_ATP_DIFFERENCE_THRESHOLD
        )

        avg_sold_out_increased_significantly = (
                venue_zone.previous_avg_sold_out_days_zone_difference
                > MED_SOLD_OUT_DAYS_DIFF_THRESHOLD
        )
        avg_sold_out_increased_moderately = (
                venue_zone.previous_avg_sold_out_days_zone_difference
                > LOW_SOLD_OUT_DAYS_DIFF_THRESHOLD
        )
        atp_increased_significantly = (
                venue_zone.previous_atp_increase_zone_difference
                > LARGE_ATP_DIFFERENCE_THRESHOLD
        )
        atp_increased_moderately = (
                venue_zone.previous_atp_increase_zone_difference
                > SMALL_ATP_DIFFERENCE_THRESHOLD
        )

        if venue_zone.recurrent_venue:
            if (
                    avg_sold_out_decreased_significantly
                    or atp_decreased_significantly
                    or (avg_sold_out_decreased_moderately and atp_decreased_moderately)
            ):
                return 0.1

            if (
                    avg_sold_out_increased_significantly
                    or atp_increased_significantly
                    or (avg_sold_out_increased_moderately and atp_increased_moderately)
            ):
                return -0.1

        return 0

    @staticmethod
    def _fill_non_affordable_price(row: VenueZone) -> float:
        if not row.previous_first_price_superior_venue_zone:
            return row.previous_first_price_venue_zone * 1.3
        return row.previous_first_price_superior_venue_zone

    @staticmethod
    def _compute_agg_stats_per_venue(
            precursors: list[InternalPrecursorFeatures],
    ) -> dict[VENUE_ID_TYPE, AggregatedInternalFeatures]:
        agg_stats_per_venue: dict[VENUE_ID_TYPE, AggregatedInternalFeatures] = {}

        for precursor in precursors:
            if agg_stats_per_venue.get(precursor.id_venue) is None:
                agg_stats_per_venue[precursor.id_venue] = AggregatedInternalFeatures(
                    min_price=precursor.previous_first_price_venue_zone,
                    total_range=precursor.price_influence_range,
                    total_range_adjusted=precursor.price_influence_range_adjusted,
                )
            else:
                agg_stats_per_venue[precursor.id_venue].aggregate_min_price(
                    precursor.previous_first_price_venue_zone
                )
                agg_stats_per_venue[precursor.id_venue].aggregate_total_range(
                    precursor.price_influence_range
                )
                agg_stats_per_venue[precursor.id_venue].aggregate_total_range_adjusted(
                    precursor.price_influence_range_adjusted
                )

        return agg_stats_per_venue

    @classmethod
    def compute(
            cls, zone_data: list[VenueZone]
    ) -> "VenueZoneFeatures":

        zone_features = []

        precursor_zone_features: list[InternalPrecursorFeatures] = []
        for venue_zone in zone_data:
            prev_first_price_superior_venue_zone = cls._fill_non_affordable_price(
                venue_zone
            )
            price_influence_range_multiplier = 1 + cls._price_influence_range_increase(
                venue_zone
            )
            price_influence_range = (
                    prev_first_price_superior_venue_zone
                    - venue_zone.previous_first_price_venue_zone
            )
            price_influence_range_adjusted = (
                    price_influence_range * price_influence_range_multiplier
            )

            is_first_zone = (
                    not precursor_zone_features
                    or precursor_zone_features[-1].id_venue != venue_zone.id_venue
            )

            precursor_zone_features.append(
                InternalPrecursorFeatures(
                    id_venue=venue_zone.id_venue,
                    previous_first_price_venue_zone=venue_zone.previous_first_price_venue_zone,
                    price_influence_range=price_influence_range,
                    price_influence_range_adjusted=price_influence_range_adjusted,
                    price_influence_range_adjusted_cumsum=price_influence_range_adjusted
                                                          + (
                                                              0
                                                              if is_first_zone
                                                              else precursor_zone_features[
                                                                  -1
                                                              ].price_influence_range_adjusted_cumsum
                                                          ),
                    price_influence_range_adjusted_cumsum_lag=0
                    if is_first_zone
                    else precursor_zone_features[
                        -1
                    ].price_influence_range_adjusted_cumsum,
                    prev_first_price_superior_venue_zone=prev_first_price_superior_venue_zone,
                    price_influence_range_multiplier=price_influence_range_multiplier,
                )
            )

        zone_agg_stats_by_venue = cls._compute_agg_stats_per_venue(
            precursors=precursor_zone_features
        )

        for venue_zone, precursor_features in zip(zone_data, precursor_zone_features):
            zone_features.append(
                VenueZoneFeature(
                    id_venue=venue_zone.id_venue,
                    ds_venue=venue_zone.ds_venue,
                    ds_city_country=venue_zone.ds_city_country,
                    id_city=venue_zone.id_city,
                    cd_city=venue_zone.cd_city,
                    ds_country=venue_zone.ds_country,
                    ds_city=venue_zone.ds_city,
                    id_country=venue_zone.id_country,
                    currency=venue_zone.currency,
                    ds_seat_category=venue_zone.seat_category,
                    order_price=venue_zone.order_price,
                    previous_first_price_venue_zone=venue_zone.previous_first_price_venue_zone,
                    previous_first_price_superior_venue_zone=precursor_features.prev_first_price_superior_venue_zone,
                    price_influence_range=precursor_features.price_influence_range,
                    price_influence_range_multiplier=precursor_features.price_influence_range_multiplier,
                    price_influence_range_adjusted=precursor_features.price_influence_range_adjusted,
                    min_price=zone_agg_stats_by_venue[venue_zone.id_venue].min_price,
                    total_range=zone_agg_stats_by_venue[
                        venue_zone.id_venue
                    ].total_range,
                    total_range_adjusted=zone_agg_stats_by_venue[
                        venue_zone.id_venue
                    ].total_range_adjusted,
                    previous_occupancies_venue=venue_zone.previous_occupancies_venue,
                    previous_atp_increase_venue=venue_zone.previous_atp_increase_venue,
                    previous_atp_increase_venue_zone=venue_zone.previous_atp_increase_venue_zone,
                    previous_avg_sold_out_days_venue=venue_zone.previous_avg_sold_out_days_venue,
                    previous_avg_sold_out_days_venue_zone=venue_zone.previous_avg_sold_out_days_venue_zone,
                    relative_price_multiplier=cls._price_intervention(
                        previous_occupancies_venue=venue_zone.previous_occupancies_venue,
                        previous_atp_increase_venue=venue_zone.previous_atp_increase_venue,
                        previous_avg_sold_out_days_venue=venue_zone.previous_avg_sold_out_days_venue,
                        recurrent_venue=venue_zone.recurrent_venue
                    ),
                    price_influence_range_adjusted_cumsum=precursor_features.price_influence_range_adjusted_cumsum,
                    price_influence_range_adjusted_cumsum_lag=precursor_features.price_influence_range_adjusted_cumsum_lag,
                    booking_fee=None,  # TODO: we do not have that data yet
                    sales_tax_outside_ticket_price=None,  # TODO: we do not have that data yet
                )
            )

        return cls(
            features=zone_features, venue_zone_data=zone_data
        )


class VenueZonePriceRecommendation(BaseModel):
    # TODO: From the pricing tool there are some values undefined. We should check if they are necessary
    id_venue: int
    ds_venue: str | None
    ds_city_country: str | None
    id_city: int | None
    cd_city: str | None
    ds_country: str | None
    ds_city: str | None
    id_country: int | None
    currency: str
    ds_seat_category: str
    nm_recommended_price: float
    nm_previous_first_price_venue_zone: float | None
    nm_unscaled_recommended_price: float | None
    nm_previous_occupancies_venue: float | None
    nm_previous_atp_increase_venue: float | None
    nm_previous_atp_increase_venue_zone: float | None
    nm_previous_avg_sold_out_days_venue: float | None
    nm_previous_avg_sold_out_days_venue_zone: float | None
    nm_relative_price_multiplier: float | None
    nm_price_influence_range: float | None
    nm_price_influence_range_multiplier: float | None
    nm_price_influence_range_adjusted_cumsum_lag: float | None
    nm_booking_fee: float | None
    nm_sales_tax_outside_ticket_price: float | None
    cd_version: str
    is_treatment: bool

    @staticmethod
    def _sql_str_formatter(string_value: str | None) -> str:
        return (
            "'" + string_value.replace("'", "''") + "'"
            if string_value is not None
            else "NULL"
        )

    @staticmethod
    def _sql_bool_formatter(bool_value: bool) -> str:
        return str(bool_value).upper()

    @staticmethod
    def _sql_number_formatter(int_value: int | float | None) -> str:
        return str(int_value) if int_value is not None else "NULL"

    def _format_field(self, value: int | float | bool | None) -> str:
        if isinstance(value, (int, float)):
            return self._sql_number_formatter(value)
        elif isinstance(value, str):
            return self._sql_str_formatter(value)
        elif isinstance(value, bool):
            return self._sql_bool_formatter(value)
        elif value is None:
            return "NULL"
        else:
            raise ValueError(f"Unsupported type for SQL formatting: {type(value)}")

    def to_sql_string(self, features_selection: list[str] | None = None) -> str:
        class_attrs = self.dict()
        if features_selection:
            field_values = [class_attrs[feature] for feature in features_selection]
        else:
            field_values = list(class_attrs.values())
        return ", ".join([self._format_field(field) for field in field_values])


class VenuePriceRecommendations(BaseModel):
    recommendations: list[VenueZonePriceRecommendation]

    def get_recommendations_by_venue_id(
            self, venue_id: int
    ) -> list[VenueZonePriceRecommendation]:
        return list(filter(lambda x: x.id_venue == venue_id, self.recommendations))

    def _get_insert_query(self) -> str:
        columns_to_export = [
            "id_venue",
            "ds_venue",
            "ds_city_country",
            "ds_city",
            "id_city",
            "cd_city",
            "ds_country",
            "id_country",
            "currency",
            "ds_seat_category",
            "nm_previous_first_price_venue_zone",
            "nm_unscaled_recommended_price",
            "nm_recommended_price",
            "nm_previous_occupancies_venue",
            "nm_previous_atp_increase_venue",
            "nm_previous_atp_increase_venue_zone",
            "nm_previous_avg_sold_out_days_venue",
            "nm_previous_avg_sold_out_days_venue_zone",
            "nm_relative_price_multiplier",
            "nm_price_influence_range",
            "nm_price_influence_range_multiplier",
            "nm_booking_fee",
            "nm_sales_tax_outside_ticket_price",
            "cd_version",
            "is_treatment",
        ]
        formatted_values = ", ".join(
            "(" + row.to_sql_string(features_selection=columns_to_export) + ")"
            for row in self.recommendations
        )

        reformat_columns = ", ".join(map(lambda x: x.upper(), columns_to_export))

        return f"""
                INSERT INTO PUBLIC.PRICE_SETTINGS_RECOMMENDATIONS (
                    {reformat_columns}
                    ) VALUES {formatted_values};
                """

    def bulk_on_dwh(self, snowflake_connector: SnowflakePython) -> None:
        snowflake_connector.execute_query(self._get_insert_query())
