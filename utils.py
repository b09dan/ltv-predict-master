FEATURE_COLUMNS = ['b_actives_real_count',
                   'b_actives_train_count',
                   'b_deals',
                   'b_pnl_real',
                   'b_pnl_train',
                   'b_real_count',
                   'b_train_count',
                   'b_volume_real',
                   'b_volume_train',
                   'c_actives_real_count',
                   'c_actives_train_count',
                   'c_cfd_count',
                   'c_commission_real_cfd',
                   'c_commission_real_crypto',
                   'c_commission_train_cfd',
                   'c_commission_train_crypto',
                   'c_count',
                   'c_crypto_count',
                   'c_instrument_id_cfd_count',
                   'c_instrument_id_count',
                   'c_instrument_id_crypto_count',
                   'c_real_count',
                   'c_train_count',
                   'n_cfd_count',
                   'n_closed_count',
                   'n_crypto_count',
                   'n_deal_count',
                   'n_digital_count',
                   'n_forex_count',
                   'n_instrument_actives_cfd_count',
                   'n_instrument_actives_count',
                   'n_instrument_actives_crypto_count',
                   'n_instrument_actives_digital_count',
                   'n_instrument_actives_forex_count',
                   'n_pnl_real_cfd',
                   'n_pnl_real_crypto',
                   'n_pnl_real_digital',
                   'n_pnl_real_forex',
                   'n_pnl_train_cfd',
                   'n_pnl_train_crypto',
                   'n_pnl_train_digital',
                   'n_pnl_train_forex',
                   'n_volume_real_cfd',
                   'n_volume_real_crypto',
                   'n_volume_real_digital',
                   'n_volume_real_forex',
                   'n_volume_train_cfd',
                   'n_volume_train_crypto',
                   'n_volume_train_digital',
                   'n_volume_train_forex',
                   'age_18_24',
                   'age_24_30',
                   'age_30_40',
                   'age_40_50',
                   'age_50_80',
                   'age_trash',
                   'locale_en_US',
                   'locale_de_DE',
                   'locale_id_ID',
                   'locale_it_IT',
                   'locale_zh_CN',
                   'locale_fr_FR',
                   'locale_es_ES',
                   'locale_th_TH',
                   'gender_1',
                   'gender_2',
                   'is_public_1',
                   'is_public_0',
                   'has_nik',
                   'is_regulated',
                   'is_trial']


def rename_columns(prefix, df):
    column_mapper = {x: f"{prefix}{x}" for x in df.columns.values if x != 'user_id'}
    return df.rename(index=str, columns=column_mapper)


def sql_copy_user_dataset():
    return f"""
    WITH transactions AS (
    SELECT
      user_id,
      count(user_id) AS deposits
    FROM stat_transactions_data
    WHERE
      registration_date > '2017-12-01'
      AND registration_date < '2018-02-01'
      AND transaction_date < registration_date + INTERVAL '30 days'
      AND balance_type = 1
    GROUP BY user_id
    )
     SELECT
      u.user_id,
      u.locale,
      date_part('year', age(u.birthdate)) AS age,
      u.country_id,
      u.gender,
      u.currency_id,
      u.client_platform_id,
      u.is_trial,
      u.is_regulated,
      u.is_public,
      (u.nickname IS NOT NULL)            AS has_nik,
      coalesce(t.deposits, 0)             AS deposits
    FROM users u
      LEFT JOIN transactions t ON u.user_id = t.user_id
    WHERE u.created > '2017-12-01' AND u.created < '2018-02-01'
    """


def sql_stat_tags_dataset(days_after_reg):
    return f"""
    SELECT
      t.user_id,
      sum(CASE WHEN name = 'used historical prices'
        THEN 1
          ELSE 0 END) AS used_historical_prices,
      sum(CASE WHEN name = 'tried to change asset'
        THEN 1
          ELSE 0 END) AS tried_to_change_asset,
      sum(CASE WHEN name = 'changed deal amount manualy'
        THEN 1
          ELSE 0 END) AS changed_deal_amount_manualy,
      sum(CASE WHEN name = 'visit_traderoom'
        THEN 1
          ELSE 0 END) AS visit_traderoom,
      sum(CASE WHEN name = 'button deposit page'
        THEN 1
          ELSE 0 END) AS button_deposit_pag,
      sum(CASE WHEN name = 'visited withdrawal page'
        THEN 1
          ELSE 0 END) AS visited_withdrawal_page,
      sum(CASE WHEN name = 'added technical analysis'
        THEN 1
          ELSE 0 END) AS added_technical_analysis,
      sum(CASE WHEN name = 'changed chart type'
        THEN 1
          ELSE 0 END) AS changed_chart_type,
      sum(CASE WHEN name = 'open video tutorial'
        THEN 1
          ELSE 0 END) AS open_video_tutorial,
      sum(CASE WHEN name = 'sell option used'
        THEN 1
          ELSE 0 END) AS sell_option_used,
      sum(CASE WHEN name = 'refreshed demo'
        THEN 1
          ELSE 0 END) AS refreshed_demo,
      sum(CASE WHEN name = 'phone confirmed'
        THEN 1
          ELSE 0 END) AS phone_confirmed,
      sum(CASE WHEN name = 'user use buyback'
        THEN 1
          ELSE 0 END) AS user_use_buyback,
      sum(CASE WHEN name LIKE 'trading indicator added%'
        THEN 1
          ELSE 0 END) AS trading_indicator_added
    
    FROM stat_tags t LEFT JOIN users u ON t.user_id = u.user_id
      WHERE
      u.created > '2017-12-01' AND u.created < '2018-02-01' AND t.tag_time < (u.created + INTERVAL '{days_after_reg} day')
    GROUP BY t.user_id
    """


def features_engineering(df):
    # Features
    # age
    df["age_18_24"] = ((df["age"] >= 18) & (df["age"] < 24))
    df["age_24_30"] = ((df["age"] >= 24) & (df["age"] < 30))
    df["age_30_40"] = ((df["age"] >= 30) & (df["age"] < 40))
    df["age_40_50"] = ((df["age"] >= 40) & (df["age"] < 50))
    df["age_50_80"] = ((df["age"] >= 50) & (df["age"] <= 80))
    df["age_trash"] = ((df["age"] < 18) | (df["age"] > 80))
    # locle

    df["locale_en_US"] = df["locale"] == "en_US"
    df["locale_pt_PT"] = df["locale"] == "pt_PT"
    df["locale_id_ID"] = df["locale"] == "id_ID"
    df["locale_es_ES"] = df["locale"] == "es_ES"
    df["locale_de_DE"] = df["locale"] == "de_DE"
    df["locale_ru_RU"] = df["locale"] == "ru_RU"
    df["locale_fr_FR"] = df["locale"] == "fr_FR"
    df["locale_it_IT"] = df["locale"] == "it_IT"
    df["locale_th_TH"] = df["locale"] == "th_TH"
    df["locale_ko_KO"] = df["locale"] == "ko_KO"
    df["locale_zh_CN"] = df["locale"] == "zh_CN"
    df["locale_tr_TR"] = df["locale"] == "tr_TR"
    df["locale_ar_KW"] = df["locale"] == "ar_KW"
    df["locale_sv_SE"] = df["locale"] == "sv_SE"
    df["locale_no_NO"] = df["locale"] == "no_NO"

    # country
    df['country_id_225'] = df['country_id'] == 225
    df['country_id_94'] = df['country_id'] == 94
    df['country_id_30'] = df['country_id'] == 30
    df['country_id_194'] = df['country_id'] == 194
    df['country_id_151'] = df['country_id'] == 151
    df['country_id_119'] = df['country_id'] == 119
    df['country_id_162'] = df['country_id'] == 162
    df['country_id_128'] = df['country_id'] == 128
    df['country_id_78'] = df['country_id'] == 78
    df['country_id_206'] = df['country_id'] == 206
    df['country_id_200'] = df['country_id'] == 200
    df['country_id_180'] = df['country_id'] == 180
    df['country_id_205'] = df['country_id'] == 205
    df['country_id_157'] = df['country_id'] == 157
    df['country_id_97'] = df['country_id'] == 97
    df['country_id_72'] = df['country_id'] == 72
    df['country_id_181'] = df['country_id'] == 181
    df['country_id_175'] = df['country_id'] == 175
    df['country_id_164'] = df['country_id'] == 164
    df['country_id_212'] = df['country_id'] == 212
    df['country_id_91'] = df['country_id'] == 91
    df['country_id_182'] = df['country_id'] == 182
    df['country_id_140'] = df['country_id'] == 140
    df['country_id_46'] = df['country_id'] == 46
    df['country_id_204'] = df['country_id'] == 204
    df['country_id_18'] = df['country_id'] == 18
    df['country_id_134'] = df['country_id'] == 134
    df['country_id_183'] = df['country_id'] == 183
    df['country_id_146'] = df['country_id'] == 146
    df['country_id_191'] = df['country_id'] == 191
    df['country_id_189'] = df['country_id'] == 189
    df['country_id_171'] = df['country_id'] == 171
    df['country_id_10'] = df['country_id'] == 10
    df['country_id_62'] = df['country_id'] == 62
    df['country_id_220'] = df['country_id'] == 220
    df['country_id_2'] = df['country_id'] == 2
    df['country_id_211'] = df['country_id'] == 211
    df['country_id_14'] = df['country_id'] == 14
    df['country_id_159'] = df['country_id'] == 159
    df['country_id_156'] = df['country_id'] == 156
    df['country_id_101'] = df['country_id'] == 101
    df['country_id_160'] = df['country_id'] == 160
    df['country_id_108'] = df['country_id'] == 108
    df['country_id_3'] = df['country_id'] == 3
    df['country_id_55'] = df['country_id'] == 55
    df['country_id_0'] = df['country_id'] == 0
    df['country_id_95'] = df['country_id'] == 95
    df['country_id_42'] = df['country_id'] == 42
    df['country_id_61'] = df['country_id'] == 61
    df['country_id_59'] = df['country_id'] == 59
    df['country_id_188'] = df['country_id'] == 188
    df['country_id_77'] = df['country_id'] == 77
    df['country_id_113'] = df['country_id'] == 113
    df['country_id_92'] = df['country_id'] == 92
    df['country_id_79'] = df['country_id'] == 79
    df['country_id_102'] = df['country_id'] == 102
    df['country_id_100'] = df['country_id'] == 100
    df['country_id_143'] = df['country_id'] == 143
    df['country_id_32'] = df['country_id'] == 32
    df['country_id_130'] = df['country_id'] == 130
    df['country_id_139'] = df['country_id'] == 139
    df['country_id_104'] = df['country_id'] == 104
    df['country_id_15'] = df['country_id'] == 15
    df['country_id_81'] = df['country_id'] == 81
    df['country_id_20'] = df['country_id'] == 20
    df['country_id_176'] = df['country_id'] == 176

    # gender
    df["gender_1"] = df["gender"] == 1
    df["gender_2"] = df["gender"] == 2

    # currency_id
    df['currency_id_5'] = df['currency_id'] == 5
    df['currency_id_1'] = df['currency_id'] == 1
    df['currency_id_2'] = df['currency_id'] == 2
    df['currency_id_6'] = df['currency_id'] == 6
    df['currency_id_7'] = df['currency_id'] == 7
    df['currency_id_4'] = df['currency_id'] == 4
    df['currency_id_8'] = df['currency_id'] == 8
    df['currency_id_9'] = df['currency_id'] == 9
    df['currency_id_43'] = df['currency_id'] == 43
    df['currency_id_10'] = df['currency_id'] == 10
    df['currency_id_3'] = df['currency_id'] == 3
    df['currency_id_5'] = df['currency_id'] == 5

    # client_platform_id
    df['client_platform_id_2'] = df['client_platform_id'] == 2
    df['client_platform_id_9'] = df['client_platform_id'] == 9
    df['client_platform_id_3'] = df['client_platform_id'] == 3
    df['client_platform_id_12'] = df['client_platform_id'] == 12
    df['client_platform_id_1000'] = df['client_platform_id'] == 1000
    df['client_platform_id_14'] = df['client_platform_id'] == 14
    df['client_platform_id_13'] = df['client_platform_id'] == 13

    # logical
    df["is_trial"] = df["is_trial"].fillna(False)
    df["is_regulated"] = df["is_regulated"].fillna(False)
    df["is_public"] = df["is_public"].fillna(False)
    df["is_public_1"] = df["is_public"] == True
    df["is_public_0"] = df["is_public"] == False

    # fill na by prefix
    def fill_na_by_prefix(df, prefixs, def_na=0):
        cols_for_fill = []
        for col_name in df.columns.values:
            for pref in prefixs:
                if col_name.startswith(pref):
                    cols_for_fill.append(col_name)
        df[cols_for_fill] = df[cols_for_fill].fillna(def_na)

    fill_na_by_prefix(df, prefixs=["b_", "n_", "c_"])
    return df


def df_to_feature_matrix(df):
    return df[FEATURE_COLUMNS].as_matrix()


def sql_commissions_dataset_for_users(user_ids, days_after_reg=1):
    return f"""
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
           o.client_platform_id         AS platform_id,
           instrument_type            AS instrument_type,
           instrument_id              AS instrument_id,
           side                       AS side_id,
           create_at :: DATE          AS trade_date,
           commission_amount_enrolled AS commission_amount_enrolled,
           instrument_active_id       AS instrument_active_id
         FROM
           orders o
           JOIN users u ON u.user_id = o.user_id
         WHERE
           o.user_id in ({",".join([str(x) for x in user_ids])}) 
           AND o.create_at < u.created :: DATE + INTERVAL '{days_after_reg} day'
           AND instrument_type IN ('crypto', 'cfd')
           AND status = 'filled'
         UNION ALL
         SELECT
           oa.user_id                 AS user_id,
           user_balance_type          AS user_balance_type,
           oa.client_platform_id         AS platform_id,
           instrument_type            AS instrument_type,
           instrument_id              AS instrument_id,
           side                       AS side_id,
           create_at :: DATE          AS trade_date,
           commission_amount_enrolled AS commission_amount_enrolled,
           instrument_active_id       AS instrument_active_id
         FROM orders_archive oa
           JOIN users u ON u.user_id = oa.user_id
         WHERE
           oa.user_id in ({",".join([str(x) for x in user_ids])}) 
           AND oa.create_at < u.created :: DATE + INTERVAL '{days_after_reg} day'
           AND instrument_type IN ('crypto', 'cfd')
           AND status = 'filled'
           AND oa.client_platform_id <> 0
       ) tmp
  GROUP BY user_id
    """


def sql_new_instruments_dataset_for_users(user_ids, days_after_reg=1):
    return f"""
     SELECT
      --   digital-option
      ap.user_id,
      SUM(
          CASE WHEN ub.type = 1 AND ap.instrument_type = 'digital-option'
            THEN
              CASE
              WHEN ap.position_type = 'short'
                THEN ap.sell_amount_enrolled / 1e6
              ELSE ap.buy_amount_enrolled / 1e6
              END
          ELSE 0 END
      )                                                   AS volume_real_digital,
      SUM(
          CASE WHEN ub.type = 1 AND ap.instrument_type = 'digital-option'
            THEN
              ap.pnl_total_enrolled / 1e6
          ELSE 0 END
      )                                                   AS pnl_real_digital,
      SUM(
          CASE WHEN ub.type = 4 AND ap.instrument_type = 'digital-option'
            THEN
              CASE
              WHEN ap.position_type = 'short'
                THEN ap.sell_amount_enrolled / 1e6
              ELSE ap.buy_amount_enrolled / 1e6
              END
          ELSE 0 END
      )                                                   AS volume_train_digital,
      SUM(
          CASE WHEN ub.type = 4 AND ap.instrument_type = 'digital-option'
            THEN
              ap.pnl_total_enrolled / 1e6
          ELSE 0 END
      )                                                   AS pnl_train_digital,
      --   cfd
      SUM(
          CASE WHEN ub.type = 1 AND ap.instrument_type = 'cfd'
            THEN
              CASE
              WHEN ap.position_type = 'short'
                THEN ap.sell_amount_enrolled / 1e6
              ELSE ap.buy_amount_enrolled / 1e6
              END
          ELSE 0 END
      )                                                   AS volume_real_cfd,
      SUM(
          CASE WHEN ub.type = 1 AND ap.instrument_type = 'cfd'
            THEN
              ap.pnl_total_enrolled / 1e6
          ELSE 0 END
      )                                                   AS pnl_real_cfd,
      SUM(
          CASE WHEN ub.type = 4 AND ap.instrument_type = 'cfd'
            THEN
              CASE
              WHEN ap.position_type = 'short'
                THEN ap.sell_amount_enrolled / 1e6
              ELSE ap.buy_amount_enrolled / 1e6
              END
          ELSE 0 END
      )                                                   AS volume_train_cfd,
      SUM(
          CASE WHEN ub.type = 4 AND ap.instrument_type = 'cfd'
            THEN
              ap.pnl_total_enrolled / 1e6
          ELSE 0 END
      )                                                   AS pnl_train_cfd,
      --   forex
      SUM(
          CASE WHEN ub.type = 1 AND ap.instrument_type = 'forex'
            THEN
              CASE
              WHEN ap.position_type = 'short'
                THEN ap.sell_amount_enrolled / 1e6
              ELSE ap.buy_amount_enrolled / 1e6
              END
          ELSE 0 END
      )                                                   AS volume_real_forex,
      SUM(
          CASE WHEN ub.type = 1 AND ap.instrument_type = 'forex'
            THEN
              ap.pnl_total_enrolled / 1e6
          ELSE 0 END
      )                                                   AS pnl_real_forex,
      SUM(
          CASE WHEN ub.type = 4 AND ap.instrument_type = 'forex'
            THEN
              CASE
              WHEN ap.position_type = 'short'
                THEN ap.sell_amount_enrolled / 1e6
              ELSE ap.buy_amount_enrolled / 1e6
              END
          ELSE 0 END
      )                                                   AS volume_train_forex,
      SUM(
          CASE WHEN ub.type = 4 AND ap.instrument_type = 'forex'
            THEN
              ap.pnl_total_enrolled / 1e6
          ELSE 0 END
      )                                                   AS pnl_train_forex,
      --   crypto
      SUM(
          CASE WHEN ub.type = 1 AND ap.instrument_type = 'crypto'
            THEN
              CASE
              WHEN ap.position_type = 'short'
                THEN ap.sell_amount_enrolled / 1e6
              ELSE ap.buy_amount_enrolled / 1e6
              END
          ELSE 0 END
      )                                                   AS volume_real_crypto,
      SUM(
          CASE WHEN ub.type = 1 AND ap.instrument_type = 'crypto'
            THEN
              ap.pnl_total_enrolled / 1e6
          ELSE 0 END
      )                                                   AS pnl_real_crypto,
      SUM(
          CASE WHEN ub.type = 4 AND ap.instrument_type = 'crypto'
            THEN
              CASE
              WHEN ap.position_type = 'short'
                THEN ap.sell_amount_enrolled / 1e6
              ELSE ap.buy_amount_enrolled / 1e6
              END
          ELSE 0 END
      )                                                   AS volume_train_crypto,
      SUM(
          CASE WHEN ub.type = 4 AND ap.instrument_type = 'crypto'
            THEN
              ap.pnl_total_enrolled / 1e6
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
      JOIN users u ON u.user_id = ap.user_id
      JOIN user_balance ub ON ap.user_balance_id = ub.id
    WHERE
      ap.user_id IN ({",".join([str(x) for x in user_ids])})
      AND ap.create_at < u.created :: DATE + INTERVAL '{days_after_reg} day'
    GROUP BY ap.user_id
    """


def sql_binary_dataset_for_users(user_ids, days_after_reg=1):
    return f"""
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
    JOIN users u ON u.user_id = ao.user_id
    JOIN user_balance ub ON ao.user_balance_id = ub.id
  WHERE
    ao.user_id in ({",".join([str(x) for x in user_ids])}) 
    AND ao.created < u.created :: DATE + INTERVAL '{days_after_reg} day'
  GROUP BY ao.user_id
    """


def sql_user_data_dataset_for_users(user_ids):
    return f"""
    SELECT
      user_id,
      tz,
      locale,
      date_part('year', age(birthdate)) AS age,
      country_id,
      gender,
      currency_id,
      client_platform_id,
      is_trial,
      is_regulated,
      is_public,
      (nickname IS NOT NULL)            AS has_nik
    FROM users u
    WHERE u.user_id in ({",".join([str(x) for x in user_ids])})
    """


#
#
def sql_get_unhandled_mobile_users(apps_flyer_tname, adwords_mob_queue_tname):
    return f"""
    WITH queue_mql_backend AS (SELECT *
               FROM {adwords_mob_queue_tname}
               WHERE conversion_name = 'mql_backend'
    )
    SELECT first_connected_user AS user_id
    FROM {apps_flyer_tname} a
      LEFT JOIN queue_mql_backend q
        ON a.first_connected_user = q.user_id
      INNER JOIN users u
      ON a.first_connected_user = u.user_id
    WHERE
      a.install_time >= now() - INTERVAL '10 days'
      AND u.created <= now() + INTERVAL '1 days'
      AND a.aff_id IN (166, 162)
      AND a.first_connected_user IS NOT NULL
      AND q.user_id IS NULL
    """


def sql_insert_mobile_mql_for_users(adwords_mob_queue_tname, user_ids):
    return f"""
    WITH T_apps_flyer AS
(
    SELECT
      first_connected_user                                                      AS user_id,
      android_advertising_id,
      ios_idfa,
      os_version,
      client_platform_id,
      aff_id,
      aff_track,
      now()                                                                     AS transaction_date,
      (regexp_matches(extra :: TEXT, '"app_version":"(.*?)"' :: TEXT, 'g')) [1] AS app_version,
      (regexp_matches(extra :: TEXT, '"sdk_version":"(.*?)"' :: TEXT, 'g')) [1] AS sdk_version,
      ip_adress,
      device
    FROM
      (
        SELECT
          *,
          (CASE WHEN android_advertising_id IS NOT NULL OR ios_idfa IS NOT NULL AND aff_id IN (166, 162)
            THEN -- we need to have installs from specific affs, because otherwise reinstalling the app
              -- user may have different device info
              -- thus, it may result into successfull, but no attributed conversion
              max(install_time)
              OVER (
                PARTITION BY first_connected_user ) END) last_install
        FROM apps_flyer
        WHERE first_connected_user IN ({",".join([str(x) for x in user_ids])})
      ) AS t
    WHERE install_time = last_install
)
INSERT INTO {adwords_mob_queue_tname} 
  SELECT
    a.user_id,
    a.client_platform_id,
    --t.country,
    --t.registration_date,
    a.aff_id,
    a.aff_track,
    a.transaction_date                       AS conversion_time,
    'custom' :: TEXT                         AS conversion_type,
    'mql_backend' :: TEXT                    AS conversion_name,
    (CASE WHEN a.client_platform_id = 2
      THEN android_advertising_id
     WHEN a.client_platform_id IN (3, 12)
       THEN ios_idfa END)                    AS uid,
    (CASE WHEN a.client_platform_id = 2
      THEN 'advertisingid'
     WHEN a.client_platform_id IN (3, 12)
       THEN 'idfa' END)                      AS uid_type,
    0                                        AS lat,
    -- 0 or 1   https://developer.apple.com/documentation/adsupport/asidentifiermanager/1614148-isadvertisingtrackingenabled
    a.app_version                            AS app_version,
    a.os_version,
    sdk_version                              AS sdk_version,
    extract('epoch' FROM a.transaction_date) AS timestamp,
    1                                        AS value,
    --'USD' as currency_code, -- always USD, beacuse at present time we store money in USD value

    a.ip_adress,
    a.device,
    (CASE WHEN a.client_platform_id = 2
      THEN 'Android ' || a.os_version
     WHEN a.client_platform_id IN (3, 12)
       THEN 'iOS ' || a.os_version END)      AS platform_version,
    u.locale,
    NULL :: TIMESTAMP                           send_date,
    NULL :: TEXT                             AS error,
    now()                                    AS insertion_time,
    0                                        AS send_counter

  FROM T_apps_flyer a
    INNER JOIN users u ON (u.user_id = a.user_id)
    """


def sql_get_unhandled_web_users(click_history_tname, adwords_queue_tname):
    return f"""
    WITH queue_mql_backend AS (SELECT *
                                   FROM {adwords_queue_tname}
                                   WHERE conversion_name = 'mql_backend'
        ), google_web_u AS (SELECT *
                            FROM users u
                            WHERE aff_id IN (168, 1)),
            last_registrations AS (SELECT *
                                   FROM {click_history_tname} h
                                   WHERE h.operation_type = 'register' AND h.created >= (now() - INTERVAL '10 days'))
        SELECT r.user_id
        FROM last_registrations r
          LEFT JOIN queue_mql_backend q ON r.user_id = q.user_id
          INNER JOIN google_web_u u ON r.user_id = u.user_id
        WHERE q.user_id IS NULL
        """


def sql_insert_web_mql_for_users(click_history_tname, adwords_queue_tname, user_ids):
    return f"""
    INSERT INTO {adwords_queue_tname}
    WITH mql_users AS (SELECT *
                       FROM users
                       WHERE user_id IN
                             ({",".join([str(x) for x in user_ids])}))
    SELECT
      h.user_id             AS user_id,
      gclid                 AS gclid,
      u.created             AS conversion_time,
      'mql_backend' :: TEXT AS conversion_name,
      0 :: INTEGER          AS transaction_sum,
      u.aff_id              AS aff_id,
      u.aff_track           AS aff_track,
      c.name             AS country,
      NULL :: TIMESTAMP        send_date,
      NULL :: TEXT          AS error,
      8                     AS reg_platform
    FROM {click_history_tname} h
      INNER JOIN mql_users u ON h.user_id = u.user_id
      LEFT JOIN country c ON u.country_id = c.id
    """


def sql_insert_web_non_predicted_deponators(adwords_queue_tname, hours_after_reg, days_before_now):
    return f"""
    WITH last_deponators AS (SELECT
                           user_id,
                           registration_date,
                           sum(transaction_sum)  AS transaction_sum,
                           min(transaction_date) AS transaction_date
                         FROM stat_transactions_data
                         WHERE
                           transaction_type = 'deposit'
                           AND (balance_type = 1 OR balance_type IS NULL)
                           AND registration_date > now() - INTERVAL '{days_before_now} days'
                           AND transaction_date < registration_date + INTERVAL '{hours_after_reg} hours'
                         GROUP BY user_id, registration_date),
    queue_mql_backend AS (SELECT *
                          FROM {adwords_queue_tname}
                          WHERE conversion_name = 'mql_backend')
    INSERT INTO {adwords_queue_tname}
    SELECT
      h.user_id                    AS user_id,
      h.gclid                      AS gclid,
      d.transaction_date           AS conversion_time,
      'mql_backend' :: TEXT        AS conversion_name,
      d.transaction_sum :: INTEGER AS transaction_sum,
      u.aff_id                     AS aff_id,
      u.aff_track                  AS aff_track,
      c.name                       AS country,
      NULL :: TIMESTAMP            AS send_date,
      NULL :: TEXT                 AS error,
      8                            AS reg_platform
    FROM last_deponators d LEFT JOIN queue_mql_backend q ON d.user_id = q.user_id
      INNER JOIN user_adwords_click_history h ON d.user_id = h.user_id
      INNER JOIN users u ON d.user_id = u.user_id
      LEFT JOIN country c ON u.country_id = c.id
    WHERE q.user_id IS NULL AND h.operation_type = 'register' AND u.aff_id IN (168, 1)
    """


def sql_insert_mobile_predicted_deponators(adwords_queue_tname, hours_after_reg, days_before_now):
    return f"""
    WITH last_deponators AS (SELECT
                           user_id,
                           registration_date,
                           sum(transaction_sum)  AS transaction_sum,
                           min(transaction_date) AS transaction_date
                         FROM stat_transactions_data
                         WHERE
                           transaction_type = 'deposit'
                           AND (balance_type = 1 OR balance_type IS NULL)
                           AND registration_date > now() - INTERVAL '{days_before_now} days'
                           AND transaction_date < registration_date + INTERVAL '{hours_after_reg} hours'
                         GROUP BY user_id, registration_date),
    mobile_queue_mql_backend AS (SELECT *
                                 FROM {adwords_queue_tname}
                                 WHERE conversion_name = 'mql_backend'),
    T_apps_flyer AS (SELECT
                       first_connected_user                                                      AS user_id,
                       android_advertising_id,
                       ios_idfa,
                       os_version,
                       client_platform_id,
                       aff_id,
                       aff_track,
                       transaction_sum,
                       transaction_date,
                       (regexp_matches(extra :: TEXT, '"app_version":"(.*?)"' :: TEXT, 'g')) [1] AS app_version,
                       (regexp_matches(extra :: TEXT, '"sdk_version":"(.*?)"' :: TEXT, 'g')) [1] AS sdk_version,
                       ip_adress,
                       device
                     FROM
                       (
                         SELECT
                           a.*,
                           d.transaction_sum  AS                          transaction_sum,
                           d.transaction_date AS                          transaction_date,
                           (CASE WHEN (android_advertising_id IS NOT NULL OR ios_idfa IS NOT NULL) AND
                                      aff_id IN (166, 162)
                             THEN -- we need to have installs from specific affs, because otherwise reinstalling the app
                               -- user may have different device info
                               -- thus, it may result into successfull, but no attributed conversion
                               max(install_time)
                               OVER (
                                 PARTITION BY first_connected_user ) END) last_install
                         FROM apps_flyer a INNER JOIN last_deponators d ON a.first_connected_user = d.user_id
                       ) AS t
                     WHERE install_time = last_install
      )
    INSERT INTO {adwords_queue_tname}
      SELECT
        a.user_id,
        a.client_platform_id,
        --t.country,
        --t.registration_date,
        a.aff_id,
        a.aff_track,
        a.transaction_date                       AS conversion_time,
        'custom' :: TEXT                         AS conversion_type,
        'mql_backend' :: TEXT                    AS conversion_name,
        (CASE WHEN a.client_platform_id = 2
          THEN android_advertising_id
         WHEN a.client_platform_id IN (3, 12)
           THEN ios_idfa END)                    AS uid,
        (CASE WHEN a.client_platform_id = 2
          THEN 'advertisingid'
         WHEN a.client_platform_id IN (3, 12)
           THEN 'idfa' END)                      AS uid_type,
        0                                        AS lat,
        -- 0 or 1   https://developer.apple.com/documentation/adsupport/asidentifiermanager/1614148-isadvertisingtrackingenabled
        a.app_version                            AS app_version,
        a.os_version,
        a.sdk_version                            AS sdk_version,
        extract('epoch' FROM a.transaction_date) AS timestamp,
        transaction_sum                          AS value,
        --'USD' as currency_code, -- always USD, beacuse at present time we store money in USD value
    
        a.ip_adress,
        a.device,
        (CASE WHEN a.client_platform_id = 2
          THEN 'Android ' || a.os_version
         WHEN a.client_platform_id IN (3, 12)
           THEN 'iOS ' || a.os_version END)      AS platform_version,
        u.locale,
        NULL :: TIMESTAMP                           send_date,
        NULL :: TEXT                             AS error,
        now()                                    AS insertion_time,
        0                                        AS send_counter
      FROM T_apps_flyer a LEFT JOIN mobile_queue_mql_backend q ON a.user_id = q.user_id
        INNER JOIN users u ON a.user_id = u.user_id
      WHERE q.user_id IS NULL AND u.aff_id IN (166, 162)
    """
