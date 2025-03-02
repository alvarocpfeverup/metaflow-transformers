from datetime import datetime

from src.modules.pipelines.etl_jobs.price_monitoring.reversed_zone import ReversedZone


class ReversedZoneAlert:
    def __init__(self, reversed_zone: ReversedZone) -> None:
        self.reversed_zone = reversed_zone

    def __str__(self) -> str:
        zone_prices_str = "\n".join(
            [
                f"      - {zone}: {price}"
                for zone, price in self.reversed_zone.zone_price_map.items()
            ]
        )

        return f"""
⚠️ *Potential Reversed Zone Detected - Manual Review Recommended* ⚠️

We have detected a potential issue with zone pricing in one of your venues.
It appears that prices may be reversed, which could impact revenue and customer experience.
Please review the following information:

*   *Plan ID:* {self.reversed_zone.id_plan}
*   *City:* {self.reversed_zone.cd_city}
*   *Country:* {self.reversed_zone.ds_country}
*   *Start Time (Local):* {self.reversed_zone.dt_start_time_local.strftime('%Y-%m-%d %H:%M:%S')}
*   *Zone Prices:*
{zone_prices_str}

*Reason for Alert:* The pricing structure in the Zone Prices above suggests a possible reversal.
  Please manually verify the prices for each zone to ensure accuracy.


*Action Required:*  Please investigate this potential issue and take corrective action if necessary.
  This may involve updating the pricing configuration for the affected plan.

*Additional Information:*
 * Alert Created At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z%z')}

        """
