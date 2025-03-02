from datetime import datetime

import numpy

from src.modules.pipelines.price_settings.price_settings_data_model import (
    VenuePriceRecommendations,
    VenueZoneFeatures,
    VenueZonePriceRecommendation,
)
from src.modules.pipelines.price_settings.selector_ab_test import Selector


class PriceSettingsModel:
    def __init__(self, version: str, ab_selector: Selector):
        self._version = version
        self.selector = ab_selector
        self._current_date = datetime.today()

    @staticmethod
    def _prettify_recommendations(price: float) -> float:
        granularity = 0.5
        if price > 10000:  # noqa: PLR2004
            granularity = 500
        elif price > 1000:  # noqa: PLR2004
            granularity = 50
        elif price > 100:  # noqa: PLR2004
            granularity = 5
        return float(numpy.round(price / granularity) * granularity)

    def generate_price_recommendations(
            self, data: VenueZoneFeatures
    ) -> VenuePriceRecommendations:
        recommendations = []

        for features in data.features:
            unscaled_recommended_price = (
                features.min_price
                + features.price_influence_range_adjusted_cumsum_lag
                * features.total_range
                / features.total_range_adjusted
                if features.total_range_adjusted != 0
                else 1
            )
            recommended_price = self._prettify_recommendations(
                unscaled_recommended_price * features.relative_price_multiplier
            )

            recommendations.append(
                VenueZonePriceRecommendation(
                    id_venue=features.id_venue,
                    ds_venue=features.ds_venue,
                    ds_city_country=features.ds_city_country,
                    ds_city=features.ds_city,
                    id_city=features.id_city,
                    cd_city=features.cd_city,
                    ds_country=features.ds_country,
                    id_country=features.id_country,
                    currency=features.currency,
                    ds_seat_category=features.ds_seat_category,
                    nm_unscaled_recommended_price=unscaled_recommended_price,
                    nm_recommended_price=recommended_price,
                    nm_previous_occupancies_venue=features.previous_occupancies_venue,
                    nm_previous_atp_increase_venue=features.previous_atp_increase_venue,
                    nm_previous_atp_increase_venue_zone=features.previous_atp_increase_venue_zone,
                    nm_previous_avg_sold_out_days_venue=features.previous_avg_sold_out_days_venue,
                    nm_previous_avg_sold_out_days_venue_zone=features.previous_avg_sold_out_days_venue_zone,
                    nm_relative_price_multiplier=features.relative_price_multiplier,
                    nm_price_influence_range=features.price_influence_range,
                    nm_price_influence_range_multiplier=features.price_influence_range_multiplier,
                    nm_previous_first_price_venue_zone=features.previous_first_price_venue_zone,
                    nm_price_influence_range_adjusted_cumsum_lag=features.price_influence_range_adjusted_cumsum_lag,
                    nm_booking_fee=features.booking_fee,
                    nm_sales_tax_outside_ticket_price=features.sales_tax_outside_ticket_price,
                    cd_version=self._version,
                    is_treatment=self.selector.is_treatment(features.id_venue),
                )
            )

        return VenuePriceRecommendations(recommendations=recommendations)
