from datetime import datetime, timedelta, timezone

from data_connectors.connector.slack.slack_sender import SlackSender
from fever_ml_toolset.database.database_utils import (
    DatabaseUtils,
    SQLSentenceType,
)
from fever_ml_toolset.settings.default_settings import DefaultSettings

from src.modules.common.secrets.slack.slack_config import SlackSettings
from src.modules.common.utils.utils_paths import QUERIES_PATH
from src.modules.pipelines.etl_jobs.price_monitoring.reversed_zone import ReversedZone
from src.modules.pipelines.etl_jobs.price_monitoring.reversed_zone_alert import (
    ReversedZoneAlert,
)


class ReversedZonesMonitor:
    QUERIES_PREFIX = "prc_alerting_reversed_zones"

    def __init__(self) -> None:
        self.__insights_connector = (
            DefaultSettings.get_default().snowflake_insights.get_connection()
        )
        self.__db_utils = DatabaseUtils(
            directory=QUERIES_PATH.format(project_name="etl_jobs/price_monitoring")
        )
        self.__slack_sender = SlackSender(
            slack_webhook_url=SlackSettings.get_default().slack_webhook.value
        )
        self.__ensure_tables_exist()

    def run(self) -> None:
        self.__insights_connector.execute_query(
            self.__db_utils.get_sql(
                sql_sentence_type=SQLSentenceType.MERGE,
                prefix_filename=ReversedZonesMonitor.QUERIES_PREFIX,
            )
        )

        query_result = self.__insights_connector.execute_query(
            self.__db_utils.get_sql(
                sql_sentence_type=SQLSentenceType.SELECT,
                prefix_filename=ReversedZonesMonitor.QUERIES_PREFIX,
            ),
            return_dict=True,
        )

        reversed_zones = [
            ReversedZone(**reversed_zone) for reversed_zone in query_result
        ]
        notified_reversed_zones = self.__notify_on_reversed_zone(reversed_zones)
        if len(notified_reversed_zones) > 0:
            self.__update_last_time_warned(notified_reversed_zones)

    def __notify_on_reversed_zone(
        self, reversed_zones: list[ReversedZone]
    ) -> list[ReversedZone]:
        now_utc = datetime.now(timezone.utc)
        one_week_ago = now_utc - timedelta(weeks=1)

        notified_reversed_zones = []
        for reversed_zone in reversed_zones:
            if (
                reversed_zone.dt_last_time_warned is None
                or reversed_zone.dt_last_time_warned < one_week_ago
            ):
                self.__send_alert(reversed_zone)

            notified_reversed_zones.append(reversed_zone)

        return notified_reversed_zones

    def __send_alert(self, reversed_zone: ReversedZone) -> None:
        message = str(ReversedZoneAlert(reversed_zone))
        self.__slack_sender.send(message)

    def __ensure_tables_exist(self) -> None:
        self.__insights_connector.execute_query(
            self.__db_utils.get_sql(
                sql_sentence_type=SQLSentenceType.CREATE,
                prefix_filename=ReversedZonesMonitor.QUERIES_PREFIX,
            )
        )

    def __update_last_time_warned(
        self, notified_reversed_zones: list[ReversedZone]
    ) -> None:
        updates = []
        for reversed_zone in notified_reversed_zones:
            strat_time = reversed_zone.dt_start_time_local.strftime("%Y-%m-%d %H:%M:%S")

            query = f"""UPDATE PUBLIC.PRC_ALERTING_REVERSED_ZONES
                         SET DT_LAST_TIME_WARNED = CURRENT_TIMESTAMP
                         WHERE ID_PLAN = {reversed_zone.id_plan}
                           AND DT_START_TIME_LOCAL = '{strat_time}'::DATETIME;
            """

            updates.append(
                query.format(
                    main_plan_id=reversed_zone.id_plan,
                    starts_at=reversed_zone.dt_start_time_local,
                )
            )

        full_transaction = "\n".join(updates)
        self.__insights_connector.execute_string(full_transaction)
