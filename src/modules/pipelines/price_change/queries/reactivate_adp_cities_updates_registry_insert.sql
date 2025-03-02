INSERT INTO PUBLIC.ADP_UPDATES_REGISTRY (ID_SESSION,
                                         CURRENT_ANCHOR_BASE_OCCUPANCY,
                                         CURRENT_ANCHOR_BASE_PRICE,
                                         LAST_ADP_RUN_DATE,
                                         NEW_ANCHOR_BASE_OCCUPANCY,
                                         NEW_ANCHOR_BASE_PRICE,
                                         ID_RUN,
                                         DS_INTERVENTION_LABEL,
                                         ID_FLOW)
WITH PRICE_DROP_CITIES AS (SELECT DISTINCT ID_CITY
                           FROM DWH_FEVER.PUBLIC.PML_DIM_SESSION
                           WHERE ID_SESSION IN ({session_ids})),
     SESSIONS AS (SELECT ID_SESSION,
                         S.ID_CITY,
                         IS_ADDON,
                         ID_PLAN,

                         IFF(IS_ACTIVE = TRUE AND NM_AVAILABLE_TICKETS > 0
                                 AND (ID_CODE_POOL = -1 OR HAS_AVAILABLE_CODES = TRUE)
                                 AND IS_SOLD_OUT = FALSE,
                             TRUE, FALSE)                             AS HAS_BEEN_PURCHASABLE,
                         IFF(HAS_BEEN_PURCHASABLE = TRUE,
                             IFF(NM_MIN_TICKETS > NM_AVAILABLE_TICKETS,
                                 0, NM_AVAILABLE_TICKETS), 0)         AS _NM_LEFT_AVAILABLE_TICKETS,
                         IFF(HAS_BEEN_PURCHASABLE = TRUE,
                             IFF(NM_MIN_TICKETS > NM_CODE_POOL_AVAILABLE_CODES,
                                 0, NM_CODE_POOL_AVAILABLE_CODES), 0) AS _NM_LEFT_CODE_POOL_AVAILABLE_CODES,
                         NM_AVAILABLE_TICKETS,
                         NM_CODE_POOL_AVAILABLE_CODES,
                         CASE
                             -- Static case, get available tickets
                             WHEN ID_CODE_POOL < 0
                                 THEN _NM_LEFT_AVAILABLE_TICKETS
                             -- Shared Code Pool, we get the least amount of codes,
                             -- either tickets available for session or available codes for the code pool
                             ELSE LEAST(_NM_LEFT_AVAILABLE_TICKETS, _NM_LEFT_CODE_POOL_AVAILABLE_CODES)
                         END                                          AS NM_LEFT_AVAILABLE_TICKETS

                  FROM DWH_FEVER.PUBLIC.PML_DIM_SESSION S
                      INNER JOIN PRICE_DROP_CITIES CITIES
                          USING (ID_CITY)
                      LEFT JOIN DWH_FEVER.PUBLIC.PML_DIM_PLAN P
                          USING (ID_PLAN)
                  WHERE DT_START > CURRENT_DATE
                    AND P.ID_PLAN_GROUP = 1),

     DT_START_LOCAL_DATA AS (SELECT PDS.ID_PLAN,
                                    PDS.ID_SESSION,
                                    IFNULL(SUM(FT_TICKET_QTY), 0)                                             AS _FT_TICKET_QTY,
                                    IFNULL(SUM(CASE WHEN PDC.ID_COUPON_KIND != 20 THEN FT_TICKET_QTY END),
                                           0)                                                                 AS FT_TICKET_QTY_WO_INVITATIONS,
                                    IFNULL(SUM(FT_VALIDATED_TICKET_QTY), 0)                                   AS FT_VALIDATED_TICKET_QTY
                             FROM SESSIONS PDS
                                 LEFT JOIN DWH_FEVER.PUBLIC.PML_FCT_TICKET_SALES GFKPP
                                     ON PDS.ID_SESSION = GFKPP.ID_SESSION
                                 LEFT JOIN DWH_FEVER.PUBLIC.PML_DIM_ORDER PDO
                                     ON GFKPP.ID_ORDER = PDO.ID_ORDER
                                 JOIN DWH_FEVER.PUBLIC.PML_DIM_PLAN PDP
                                     ON PDP.ID_PLAN = PDS.ID_PLAN
                                 LEFT JOIN DWH_FEVER.PUBLIC.PML_DIM_COUPON PDC
                                     ON PDC.CD_COUPON = PDO.CD_COUPON
                             WHERE (GFKPP.ID_TICKET_STATUS = 1 OR GFKPP.ID_TICKET_STATUS IS NULL)
                               AND PDS.IS_ADDON = 0
                             GROUP BY 1, 2),
     SESSION_CAPACITIES AS (SELECT ID_PLAN,
                                   ID_SESSION,
                                   IFNULL(_FT_TICKET_QTY, 0) + IFNULL(NM_LEFT_AVAILABLE_TICKETS, 0)::INT AS CAPACITY,
                                   IFNULL(_FT_TICKET_QTY, 0)                                             AS TICKETS_SOLD,
                            FROM SESSIONS
                                LEFT JOIN DT_START_LOCAL_DATA
                                    USING (ID_PLAN, ID_SESSION)),
     SESSION_PRICES AS (SELECT PDS.MAIN_PLAN_ID AS ID_PLAN,
                               PLCS.SESSION_ID  AS ID_SESSION,
                               IFF(
                                       CMPMS.MUST_USE_TAX_BASE,
                                       IFF(PLCS.IS_ACTIVE, PLCS.TICKET_PRICE_TAX_BASE, PDS.TICKET_PRICE_TAX_BASE),
                                       IFF(PLCS.IS_ACTIVE, PLCS.TICKET_PRICE, PDS.TICKET_PRICE)
                               )                AS CURRENT_PRICE
                        FROM DWH_FEVER.LAKE.PREMILLER_LAKE_CORE_PLAN PDS
                            LEFT JOIN DWH_FEVER.LAKE.PREMILLER_LAKE_CORE_SESSIONPRICE PLCS
                                ON PDS.ID = PLCS.SESSION_ID
                            INNER JOIN DWH_FEVER.LAKE.PREMILLER_LAKE_CORE_MAINPLANMANAGEMENTSETTINGS CMPMS
                                ON CMPMS.MAIN_PLAN_ID = PDS.MAIN_PLAN_ID
                        WHERE PLCS.CHANNEL_ID = 'fever-marketplace'),
     CURRENT_VALUES AS (SELECT S.ID_SESSION,
                               S.ID_CITY,
                               COALESCE(CAPACITIES.CAPACITY, 0)                        AS CAPACITY,
                               COALESCE(CAPACITIES.TICKETS_SOLD, 0)                    AS TOTAL_TICKETS_SOLD,
                               COALESCE(DIV0NULL(COALESCE(CAPACITIES.TICKETS_SOLD, 0),
                                                 COALESCE(CAPACITIES.CAPACITY, 0)), 1) AS CURRENT_OCCUPANCY,
                               PRICES.CURRENT_PRICE                                    AS CURRENT_PRICE,
                               MODEL_TRACKER.RUN_DATE                                  AS ADP_RUN_DATE,
                               MODEL_TRACKER.ANCHOR_BASE_OCCUPANCY                     AS ANCHOR_BASE_OCCUPANCY,
                               MODEL_TRACKER.ANCHOR_BASE_PRICE                         AS ANCHOR_BASE_PRICE,
                               EF.BASE_PRICE                                           AS BASE_PRICE
                        FROM SESSIONS S
                            LEFT JOIN SESSION_CAPACITIES CAPACITIES
                                USING (ID_SESSION)
                            INNER JOIN PUBLIC.ADP_EXTRA_FEATURES EF
                                USING (ID_SESSION)
                            INNER JOIN PUBLIC.ADP_MODEL_TRACKER MODEL_TRACKER
                                USING (ID_SESSION)
                            INNER JOIN SESSION_PRICES PRICES
                                ON S.ID_SESSION = PRICES.ID_SESSION
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY S.ID_SESSION ORDER BY MODEL_TRACKER.RUN_DATE DESC) = 1
                        ORDER BY S.ID_SESSION, MODEL_TRACKER.RUN_DATE ASC)
SELECT ID_SESSION,
       ANCHOR_BASE_OCCUPANCY            AS CURRENT_ANCHOR_BASE_OCCUPANCY,
       ANCHOR_BASE_PRICE                AS CURRENT_ANCHOR_BASE_PRICE,
       ADP_RUN_DATE                     AS LAST_ADP_RUN_DATE,
       LEAST(1, CURRENT_OCCUPANCY)      AS NEW_ANCHOR_BASE_OCCUPANCY,
       CURRENT_PRICE                    AS NEW_ANCHOR_BASE_PRICE,
       '{id_run}'                       AS ID_RUN,
       '{intervention_label}'           AS DS_INTERVENTION_LABEL,
       'reactivate_adp_cdl_cities_flow' AS ID_FLOW
FROM CURRENT_VALUES;
