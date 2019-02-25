WITH all_profit AS (
  SELECT
    ao.user_id                                            AS user_id,
    count(ao.id)                                          AS deal_count,
    ao.client_platform_id                                 AS platform_id,
    ao.option_type_id                                     AS instrument_id,
    ao.active_id                                          AS asset_id,
    sum(ao.enrolled_amount / 1e6 - ao.win_enrolled / 1e6) AS binary_pnl,
    sum(ao.enrolled_amount / 1e6)                         AS binary_volume,
    NULL                                                  AS ni_pnl,
    NULL                                                  AS ni_volume,
    NULL                                                  AS commission,
    to_timestamp(ao.exp_time) :: DATE                     AS trade_date
  FROM archive_option ao
    JOIN user_balance ub ON ao.user_balance_id = ub.id
  WHERE ao.exp_time >= date_part('epoch', '2017-12-01' :: TIMESTAMP WITH TIME ZONE) :: INT
        AND ao.exp_time < date_part('epoch', '2018-02-01' :: TIMESTAMP WITH TIME ZONE) :: INT
        AND option_type_id IS NOT NULL
        AND ub.type = 1
  GROUP BY
    ao.user_id,
    ao.client_platform_id,
    ao.option_type_id,
    ao.active_id,
    to_timestamp(, ao.exp_time) :: DATE
  UNION ALL
  SELECT
    ap.user_id                       AS user_id,
    count(ap.id)                     AS deal_count,
    ap.client_platform_id            AS platform_id,
    CASE
    WHEN ap.instrument_type = 'digital-option'
      THEN 7
    WHEN ap.instrument_type = 'cfd'
      THEN 8
    WHEN ap.instrument_type = 'forex'
      THEN 9
    WHEN ap.instrument_type = 'crypto'
      THEN 10
    ELSE 0
    END                              AS instrument_id,
    ap.instrument_active_id          AS asset_id,
    NULL                             AS binary_pnl,
    NULL                             AS binary_volume,
    sum(ap.pnl_total_enrolled / 1e6) AS ni_pnl,
    SUM(
        CASE
        WHEN ap.position_type = 'short'
          THEN ap.sell_amount_enrolled / 1e6
        ELSE ap.buy_amount_enrolled / 1e6
        END
    )                                AS ni_volume,
    NULL                             AS commission,
    ap.close_at :: DATE              AS trade_date

  FROM archive_position ap
    JOIN user_balance ub ON ap.user_balance_id = ub.id
  WHERE
    ap.close_at >= '2017-12-01' AND ap.close_at < '2018-02-01'
    AND ub.type = 1
  GROUP BY
    ap.user_id,
    ap.client_platform_id,
    ap.instrument_type,
    ap.instrument_active_id,
    ap.close_at :: DATE
  UNION ALL
  SELECT
    tmp.user_id,
    NULL           AS   binary_pnl,
    NULL           AS   binary_volume,
    NULL           AS   ni_pnl,
    NULL           AS   ni_volume,
    sum(tmp.commission) commission,
    tmp.trade_date AS   trade_date
  FROM (
         SELECT
           user_id           AS user_id,
           create_at :: DATE AS trade_date,
           --            SUM(commission_amount_enrolled / 1e6) AS commission
           SUM(CASE WHEN (extra_data :: JSON ->> 'paid_for_commission_enrolled') IS NOT NULL
             THEN (extra_data :: JSON ->> 'paid_for_commission_enrolled') :: DOUBLE PRECISION
               ELSE commission_amount_enrolled / 1e6 END)
                             AS commission
         FROM
           orders
         WHERE
           create_at >= '2017-12-01' AND create_at < '2018-02-01'
           AND instrument_type IN ('crypto', 'cfd')
           AND status = 'filled'
           AND user_balance_type = 1
         GROUP BY
           user_id,
           create_at :: DATE
         UNION ALL
         SELECT
           user_id           AS user_id,
           create_at :: DATE AS trade_date,
           --            SUM(commission_amount_enrolled / 1e6) AS commission
           SUM(CASE WHEN (extra_data :: JSON ->> 'paid_for_commission_enrolled') IS NOT NULL
             THEN (extra_data :: JSON ->> 'paid_for_commission_enrolled') :: DOUBLE PRECISION
               ELSE commission_amount_enrolled / 1e6 END)
                             AS commission
         FROM orders_archive
         WHERE
           create_at >= '2017-12-01' AND create_at < '2018-02-01'
           AND instrument_type IN ('crypto', 'cfd')
           AND status = 'filled'
           AND user_balance_type = 1
           AND client_platform_id <> 0
         GROUP BY
           user_id,
           create_at :: DATE
       ) tmp
  GROUP BY tmp.user_id, tmp.trade_date
)

SELECT
  p.user_id,
  sum(p.binary_pnl :: FLOAT) AS binary_pnl_30,
  sum(p.ni_pnl :: FLOAT)     AS ni_pnl_30,
  sum(p.commission :: FLOAT) AS commission_30,
  u.created                  AS user_created
FROM all_profit p
  LEFT JOIN users u ON p.user_id = u.user_id
WHERE
  p.trade_date < (u.created + INTERVAL '30 days')
GROUP BY p.user_id, u.created
