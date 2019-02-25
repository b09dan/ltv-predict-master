CREATE TABLE data_science.pnl_2017_12_01_to_2018_01_01
  AS WITH all_profit AS (
    SELECT
      ao.user_id                                            AS user_id,
      sum(ao.enrolled_amount / 1e6 - ao.win_enrolled / 1e6) AS binary_pnl,
      NULL                                                  AS ni_pnl,
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
      to_timestamp(ao.exp_time) :: DATE
    UNION ALL
    SELECT
      ap.user_id          AS user_id,
      NULL                AS binary_pnl,
      SUM(
          CASE
          WHEN ap.position_type = 'short'
            THEN ap.sell_amount_enrolled / 1e6
          ELSE ap.buy_amount_enrolled / 1e6
          END
          -
          CASE
          WHEN ap.position_type = 'short'
            THEN (ap.sell_amount_enrolled + ap.pnl_total_enrolled) / 1e6
          ELSE (ap.buy_amount_enrolled + ap.pnl_total_enrolled) / 1e6
          END
      )                   AS ni_pnl,
      NULL                AS commission,
      ap.close_at :: DATE AS trade_date

    FROM archive_position ap
      JOIN user_balance ub ON ap.user_balance_id = ub.id
    WHERE
      ap.close_at >= '2017-12-01' AND ap.close_at < '2018-02-01'
      AND ub.type = 1
    GROUP BY
      ap.user_id,
      ap.close_at :: DATE
    UNION ALL
    SELECT
      tmp.user_id,
      NULL           AS   binary_pnl,
      NULL           AS   ni_pnl,
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

-- Features for 'commisions'
CREATE TABLE data_science.ltv_dataset_commissions AS
  SELECT
    user_id,
    count(*)                                 AS count,
    count(DISTINCT instrument_id)            AS instrument_id_count,
    sum((instrument_type = 'cfd') :: INT)    AS cfd_count,
    sum((instrument_type = 'crypto') :: INT) AS crypto_count,

    sum((user_balance_type = 1) :: INT)      AS real_count,
    sum((user_balance_type = 4) :: INT)      AS train_count,
    count(DISTINCT CASE WHEN user_balance_type = 1
      THEN instrument_active_id END)         AS actives_real_count,
    count(DISTINCT CASE WHEN user_balance_type = 4
      THEN instrument_active_id END)         AS actives_train_count,


    count(DISTINCT CASE WHEN instrument_type = 'cfd'
      THEN instrument_id END)                AS instrument_id_cfd_count,

    count(DISTINCT CASE WHEN instrument_type = 'crypto'
      THEN instrument_id END)                AS instrument_id_crypto_count,


    -- crypto
    sum(CASE WHEN user_balance_type = 1 AND instrument_type = 'crypto'
      THEN commission_amount_enrolled / 1e6
        ELSE 0 END)                          AS commission_real_crypto,
    sum(CASE WHEN user_balance_type = 4 AND instrument_type = 'crypto'
      THEN commission_amount_enrolled / 1e6
        ELSE 0 END)                          AS commission_train_crypto,

    --   cfd
    sum(CASE WHEN user_balance_type = 1 AND instrument_type = 'cfd'
      THEN commission_amount_enrolled / 1e6
        ELSE 0 END)                          AS commission_real_cfd,

    sum(CASE WHEN user_balance_type = 4 AND instrument_type = 'cfd'
      THEN commission_amount_enrolled / 1e6
        ELSE 0 END)                          AS commission_train_cfd
  FROM (
         SELECT
           o.user_id                  AS user_id,
           user_balance_type          AS user_balance_type,
           client_platform_id         AS platform_id,
           instrument_type            AS instrument_type,
           instrument_id              AS instrument_id,
           side                       AS side_id,
           create_at :: DATE          AS trade_date,
           commission_amount_enrolled AS commission_amount_enrolled,
           instrument_active_id       AS instrument_active_id
         FROM
           orders o
           JOIN data_science.pnl_2017_12_01_to_2018_01_01 u ON u.user_id = o.user_id
         WHERE
           o.create_at < u.user_created :: DATE + INTERVAL '1 day'
           AND instrument_type IN ('crypto', 'cfd')
           AND status = 'filled'
         UNION ALL
         SELECT
           oa.user_id                 AS user_id,
           user_balance_type          AS user_balance_type,
           client_platform_id         AS platform_id,
           instrument_type            AS instrument_type,
           instrument_id              AS instrument_id,
           side                       AS side_id,
           create_at :: DATE          AS trade_date,
           commission_amount_enrolled AS commission_amount_enrolled,
           instrument_active_id       AS instrument_active_id
         FROM orders_archive oa
           JOIN data_science.pnl_2017_12_01_to_2018_01_01 u ON u.user_id = oa.user_id
         WHERE
           oa.create_at < u.user_created :: DATE + INTERVAL '1 day'
           AND instrument_type IN ('crypto', 'cfd')
           AND status = 'filled'
           AND client_platform_id <> 0
       ) tmp
  GROUP BY user_id;

-- Features for 'New instruments'
CREATE TABLE data_science.ltv_dataset_ni AS
  SELECT
    --   digital-option
    ap.user_id,
    SUM(
        CASE WHEN ub.type = 1 AND ap.instrument_type = 'digital-option'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
        ELSE 0 END
    )                                                   AS volume_real_digital,
    SUM(
        CASE WHEN ub.type = 1 AND ap.instrument_type = 'digital-option'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
            -
            CASE
            WHEN ap.position_type = 'short'
              THEN (ap.sell_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            ELSE (ap.buy_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            END
        ELSE 0 END
    )                                                   AS pnl_real_digital,
    SUM(
        CASE WHEN ub.type = 4 AND ap.instrument_type = 'digital-option'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
        ELSE 0 END
    )                                                   AS volume_train_digital,
    SUM(
        CASE WHEN ub.type = 4 AND ap.instrument_type = 'digital-option'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
            -
            CASE
            WHEN ap.position_type = 'short'
              THEN (ap.sell_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            ELSE (ap.buy_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            END
        ELSE 0 END
    )                                                   AS pnl_train_digital,
    --   cfd
    SUM(
        CASE WHEN ub.type = 1 AND ap.instrument_type = 'cfd'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
        ELSE 0 END
    )                                                   AS volume_real_cfd,
    SUM(
        CASE WHEN ub.type = 1 AND ap.instrument_type = 'cfd'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
            -
            CASE
            WHEN ap.position_type = 'short'
              THEN (ap.sell_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            ELSE (ap.buy_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            END
        ELSE 0 END
    )                                                   AS pnl_real_cfd,
    SUM(
        CASE WHEN ub.type = 4 AND ap.instrument_type = 'cfd'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
        ELSE 0 END
    )                                                   AS volume_train_cfd,
    SUM(
        CASE WHEN ub.type = 4 AND ap.instrument_type = 'cfd'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
            -
            CASE
            WHEN ap.position_type = 'short'
              THEN (ap.sell_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            ELSE (ap.buy_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            END
        ELSE 0 END
    )                                                   AS pnl_train_cfd,
    --   forex
    SUM(
        CASE WHEN ub.type = 1 AND ap.instrument_type = 'forex'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
        ELSE 0 END
    )                                                   AS volume_real_forex,
    SUM(
        CASE WHEN ub.type = 1 AND ap.instrument_type = 'forex'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
            -
            CASE
            WHEN ap.position_type = 'short'
              THEN (ap.sell_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            ELSE (ap.buy_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            END
        ELSE 0 END
    )                                                   AS pnl_real_forex,
    SUM(
        CASE WHEN ub.type = 4 AND ap.instrument_type = 'forex'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
        ELSE 0 END
    )                                                   AS volume_train_forex,
    SUM(
        CASE WHEN ub.type = 4 AND ap.instrument_type = 'forex'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
            -
            CASE
            WHEN ap.position_type = 'short'
              THEN (ap.sell_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            ELSE (ap.buy_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            END
        ELSE 0 END
    )                                                   AS pnl_train_forex,
    --   crypto
    SUM(
        CASE WHEN ub.type = 1 AND ap.instrument_type = 'crypto'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
        ELSE 0 END
    )                                                   AS volume_real_crypto,
    SUM(
        CASE WHEN ub.type = 1 AND ap.instrument_type = 'crypto'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
            -
            CASE
            WHEN ap.position_type = 'short'
              THEN (ap.sell_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            ELSE (ap.buy_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            END
        ELSE 0 END
    )                                                   AS pnl_real_crypto,
    SUM(
        CASE WHEN ub.type = 4 AND ap.instrument_type = 'crypto'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
        ELSE 0 END
    )                                                   AS volume_train_crypto,
    SUM(
        CASE WHEN ub.type = 4 AND ap.instrument_type = 'crypto'
          THEN
            CASE
            WHEN ap.position_type = 'short'
              THEN ap.sell_amount_enrolled / 1000000.0
            ELSE ap.buy_amount_enrolled / 1000000.0
            END
            -
            CASE
            WHEN ap.position_type = 'short'
              THEN (ap.sell_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            ELSE (ap.buy_amount_enrolled + ap.pnl_total_enrolled) / 1000000.0
            END
        ELSE 0 END
    )                                                   AS pnl_train_crypto,
    count(ap.id)                                        AS deal_count,
    sum((ap.status = 'closed') :: INT)                  AS closed_count,
    count(DISTINCT instrument_active_id)                AS instrument_actives_count,

    count(DISTINCT CASE WHEN ap.instrument_type = 'digital-option'
      THEN instrument_active_id END)                    AS instrument_actives_digital_count,
    count(DISTINCT CASE WHEN ap.instrument_type = 'cfd'
      THEN instrument_active_id END)                    AS instrument_actives_cfd_count,
    count(DISTINCT CASE WHEN ap.instrument_type = 'forex'
      THEN instrument_active_id END)                    AS instrument_actives_forex_count,
    count(DISTINCT CASE WHEN ap.instrument_type = 'crypto'
      THEN instrument_active_id END)                    AS instrument_actives_crypto_count,


    sum((ap.instrument_type = 'digital-option') :: INT) AS digital_count,
    sum((ap.instrument_type = 'cfd') :: INT)            AS cfd_count,
    sum((ap.instrument_type = 'forex') :: INT)          AS forex_count,
    sum((ap.instrument_type = 'crypto') :: INT)         AS crypto_count
  FROM archive_position ap
    JOIN data_science.pnl_2017_12_01_to_2018_01_01 u ON u.user_id = ap.user_id
    JOIN user_balance ub ON ap.user_balance_id = ub.id
  WHERE ap.create_at < u.user_created :: DATE + INTERVAL '1 day'
  GROUP BY ap.user_id;

-- Features for 'Binary Options'
CREATE TABLE data_science.ltv_dataset_binary AS
  SELECT
    ao.user_id,
    --u.created as registration_date,
    count(*)                  AS deals,
    sum((ub.type = 1) :: INT) AS real_count,
    sum((ub.type = 4) :: INT) AS train_count,

    count(DISTINCT CASE WHEN ub.type = 1
      THEN active_id END)     AS actives_real_count,
    count(DISTINCT CASE WHEN ub.type = 4
      THEN active_id END)     AS actives_train_count,

    sum(CASE WHEN ub.type = 1
      THEN ao.enrolled_amount / 1000000.0
        ELSE 0 END)           AS volume_real,
    sum(CASE WHEN ub.type = 4
      THEN ao.enrolled_amount / 1000000.0
        ELSE 0 END)           AS volume_train,


    sum(CASE WHEN ub.type = 1
      THEN ao.enrolled_amount / 1000000.0 - ao.win_enrolled / 1000000.0
        ELSE 0 END)           AS pnl_real,
    sum(CASE WHEN ub.type = 4
      THEN ao.enrolled_amount / 1000000.0 - ao.win_enrolled / 1000000.0
        ELSE 0 END)           AS pnl_train

  FROM archive_option ao
    JOIN data_science.pnl_2017_12_01_to_2018_01_01 u ON u.user_id = ao.user_id
    JOIN user_balance ub ON ao.user_balance_id = ub.id
  WHERE
    ao.created < u.user_created :: DATE + INTERVAL '1 day'
  GROUP BY ao.user_id;