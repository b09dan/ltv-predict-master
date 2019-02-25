import argparse
import configparser
import hashlib
import json
import logging
import os
import sys
from collections import namedtuple
from pathlib import Path
from typing import List

import pandas as pd
import psycopg2
import sklearn
from sklearn.externals import joblib

from utils import sql_get_unhandled_mobile_users, \
    sql_insert_mobile_mql_for_users, sql_insert_web_mql_for_users, sql_get_unhandled_web_users, \
    sql_insert_web_non_predicted_deponators, sql_insert_mobile_predicted_deponators

CHUNK_SIZE = 10000
MODEL_NAME = 'random_forest_04'
MQL_MOBILE_TARGET_TABLE = 'google_mobile_adwords_queue'
MQL_WEB_TARGET_TABLE = 'google_adwords_queue'
APPS_FLYER_TABLE = 'apps_flyer'
ADWORDS_CLICK_HISTORY = 'user_adwords_click_history'
HOURS_AFTER_REG = 24 * 7
DAYS_BEFORE_NOW = 3

LOG_LEVEL = "INFO"
# LOG_LEVEL = "INFO"
LOG_FILE = '/var/log/ltv-predict/main.log'

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
ch = logging.StreamHandler()
ch.setLevel(LOG_LEVEL)
formatter = logging.Formatter(
    '%(asctime)s [%(filename)s.%(lineno)d] %(processName)s %(levelname)-1s %(name)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
fh = logging.FileHandler(LOG_FILE)
fh.setLevel(LOG_LEVEL)
fh.setFormatter(formatter)
logger.addHandler(fh)

AppConfig = namedtuple("AppConfig", "gp_user, gp_pass, wp_user, wp_pass, ver_ignore, model_path")


def sql_user_data(user_ids):
    return f"""
    WITH 
        required_users AS (
          SELECT
            u.user_id                           AS user_id,
            u.locale                            AS locale,
            date_part('year', age(u.birthdate)) AS age,
            u.country_id                        AS country_id,
            u.gender                            AS gender,
            u.currency_id                       AS currency_id,
            u.client_platform_id                AS client_platform_id,
            u.is_trial                          AS is_trial,
            u.is_regulated                      AS is_regulated,
            u.is_public                         AS is_public,
            (u.nickname IS NOT NULL)            AS has_nik,
            u.created                           AS created
          FROM users u
          WHERE u.user_id in ({",".join([str(x) for x in user_ids])})),
        ni_data AS (SELECT
                      ap.user_id,
                      --                   extract(EPOCH FROM (min(ap.create_at) - u.created)) AS first_deal_interval,
                      --   digital-option
                      SUM(
                          CASE WHEN ap.instrument_type = 'digital-option'
                            THEN
                              CASE
                              WHEN ap.position_type = 'short'
                                THEN ap.sell_amount_enrolled / 1e6
                              ELSE ap.buy_amount_enrolled / 1e6
                              END
                          ELSE 0 END
                      )                                                   AS volume_train_digital,
                      SUM(
                          CASE WHEN ap.instrument_type = 'digital-option'
                            THEN
                              ap.pnl_total_enrolled / 1e6
                          ELSE 0 END
                      )                                                   AS pnl_train_digital,
                      --   cfd
                      SUM(
                          CASE WHEN ap.instrument_type = 'cfd'
                            THEN
                              CASE
                              WHEN ap.position_type = 'short'
                                THEN ap.sell_amount_enrolled / 1e6
                              ELSE ap.buy_amount_enrolled / 1e6
                              END
                          ELSE 0 END
                      )                                                   AS volume_train_cfd,
                      SUM(
                          CASE WHEN ap.instrument_type = 'cfd'
                            THEN
                              ap.pnl_total_enrolled / 1e6
                          ELSE 0 END
                      )                                                   AS pnl_train_cfd,
                      --   forex
                      SUM(
                          CASE WHEN ap.instrument_type = 'forex'
                            THEN
                              CASE
                              WHEN ap.position_type = 'short'
                                THEN ap.sell_amount_enrolled / 1e6
                              ELSE ap.buy_amount_enrolled / 1e6
                              END
                          ELSE 0 END
                      )                                                   AS volume_train_forex,
                      SUM(
                          CASE WHEN ap.instrument_type = 'forex'
                            THEN
                              ap.pnl_total_enrolled / 1e6
                          ELSE 0 END
                      )                                                   AS pnl_train_forex,
                      --   crypto
                      SUM(
                          CASE WHEN ap.instrument_type = 'crypto'
                            THEN
                              CASE
                              WHEN ap.position_type = 'short'
                                THEN ap.sell_amount_enrolled / 1e6
                              ELSE ap.buy_amount_enrolled / 1e6
                              END
                          ELSE 0 END
                      )                                                   AS volume_train_crypto,
                      SUM(
                          CASE WHEN ap.instrument_type = 'crypto'
                            THEN
                              ap.pnl_total_enrolled / 1e6
                          ELSE 0 END
                      )                                                   AS pnl_train_crypto,
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
                      JOIN required_users u ON u.user_id = ap.user_id
                      JOIN user_balance ub ON ap.user_balance_id = ub.id
                    WHERE
                      --   only train
                      ub.type = 4
                      AND ap.create_at < u.created :: DATE + INTERVAL '1 day'
                    GROUP BY ap.user_id),
        b_data AS (SELECT
                     ao.user_id,
                     count(DISTINCT active_id)                                         AS bin_count,
                     sum(ao.enrolled_amount / 1000000.0)                               AS volume_train_bin,
                     sum(ao.enrolled_amount / 1000000.0 - ao.win_enrolled / 1000000.0) AS pnl_train_bin,
                     count(DISTINCT ao.active_id)                                      AS instrument_actives_bin_count
                   FROM archive_option ao
                     JOIN required_users u ON u.user_id = ao.user_id
                     JOIN user_balance ub ON ao.user_balance_id = ub.id
                   WHERE
                     ub.type = 4
                     AND ao.created < u.created :: DATE + INTERVAL '1 day'
                   GROUP BY ao.user_id)
    SELECT
      u.user_id,
      u.locale,
      u.age,
      u.country_id,
      u.gender,
      u.currency_id,
      u.client_platform_id,
      u.is_trial,
      u.is_regulated,
      u.is_public,
      u.has_nik,
      u.created,
      coalesce(n.volume_train_digital, 0) AS volume_train_digital,
      coalesce(n.pnl_train_digital, 0) AS pnl_train_digital,
      coalesce(n.volume_train_cfd, 0) AS volume_train_cfd,
      coalesce(n.pnl_train_cfd, 0) AS pnl_train_cfd,
      coalesce(n.volume_train_forex, 0) AS volume_train_forex,
      coalesce(n.pnl_train_forex, 0) AS pnl_train_forex,
      coalesce(n.volume_train_crypto, 0) AS volume_train_crypto,
      coalesce(n.pnl_train_crypto, 0) AS pnl_train_crypto,
      coalesce(n.closed_count, 0) AS closed_count,
      coalesce(n.instrument_actives_count, 0) AS instrument_actives_count,
      coalesce(n.instrument_actives_digital_count, 0) AS instrument_actives_digital_count,
      coalesce(n.instrument_actives_cfd_count, 0) AS instrument_actives_cfd_count,
      coalesce(n.instrument_actives_forex_count, 0) AS instrument_actives_forex_count,
      coalesce(n.instrument_actives_crypto_count, 0) AS instrument_actives_crypto_count,
      coalesce(n.digital_count, 0) AS digital_count,
      coalesce(n.cfd_count, 0) AS cfd_count,
      coalesce(n.forex_count, 0) AS forex_count,
      coalesce(n.crypto_count, 0) AS crypto_count,
      coalesce(b.bin_count, 0) AS bin_count,
      coalesce(b.volume_train_bin, 0) AS volume_train_bin,
      coalesce(b.pnl_train_bin, 0) AS pnl_train_bin,
      coalesce(b.instrument_actives_bin_count, 0) AS instrument_actives_bin_count
    FROM required_users u LEFT JOIN ni_data n ON u.user_id = n.user_id
      LEFT JOIN b_data b ON u.user_id = b.user_id
    """


def sql_user_stat_tags_wpad(user_ids, days_after_reg=1):
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
      WHERE t.user_id in ({",".join([str(x) for x in user_ids])}) AND t.tag_time < (u.created + INTERVAL '{days_after_reg} day')
    GROUP BY t.user_id
    """
def sql_user_stat_tags_gp(user_ids, days_after_reg=1):
    return f"""
    SELECT
      ut.user_id,
      sum(CASE WHEN t.name = 'used historical prices'
        THEN 1
          ELSE 0 END) AS used_historical_prices,
      sum(CASE WHEN t.name = 'tried to change asset'
        THEN 1
          ELSE 0 END) AS tried_to_change_asset,
      sum(CASE WHEN t.name = 'changed deal amount manualy'
        THEN 1
          ELSE 0 END) AS changed_deal_amount_manualy,
      sum(CASE WHEN t.name = 'visit_traderoom'
        THEN 1
          ELSE 0 END) AS visit_traderoom,
      sum(CASE WHEN t.name = 'button deposit page'
        THEN 1
          ELSE 0 END) AS button_deposit_pag,
      sum(CASE WHEN t.name = 'visited withdrawal page'
        THEN 1
          ELSE 0 END) AS visited_withdrawal_page,
      sum(CASE WHEN t.name = 'added technical analysis'
        THEN 1
          ELSE 0 END) AS added_technical_analysis,
      sum(CASE WHEN t.name = 'changed chart type'
        THEN 1
          ELSE 0 END) AS changed_chart_type,
      sum(CASE WHEN t.name = 'open video tutorial'
        THEN 1
          ELSE 0 END) AS open_video_tutorial,
      sum(CASE WHEN t.name = 'sell option used'
        THEN 1
          ELSE 0 END) AS sell_option_used,
      sum(CASE WHEN t.name = 'refreshed demo'
        THEN 1
          ELSE 0 END) AS refreshed_demo,
      sum(CASE WHEN t.name = 'phone confirmed'
        THEN 1
          ELSE 0 END) AS phone_confirmed,
      sum(CASE WHEN t.name = 'user use buyback'
        THEN 1
          ELSE 0 END) AS user_use_buyback,
      sum(CASE WHEN t.name LIKE 'trading indicator added%'
        THEN 1
          ELSE 0 END) AS trading_indicator_added
    
    FROM user_tags ut LEFT JOIN users u ON ut.user_id = u.user_id
      INNER JOIN tags t ON ut.tag_id = t.id
    WHERE ut.user_id IN ({",".join([str(x) for x in user_ids])})
          AND ut.created < (u.created + INTERVAL '{days_after_reg} day')
    GROUP BY ut.user_id
    """


def check_path(path):
    if not path.exists() or not path.is_file():
        logger.error(f"Can't find file: {path}")
        sys.exit(1)
    return path


def get_field_from_cfg(cfg, field):
    if cfg is None or field not in cfg:
        logger.error(f"Can't find field '{field}' in cfg")
        sys.exit(1)
    return cfg[field]


def features_engineering(df):
    result = pd.DataFrame()
    # Features
    # age
    result["age_18_24"] = ((df["age"] >= 18) & (df["age"] < 24))
    result["age_24_30"] = ((df["age"] >= 24) & (df["age"] < 30))
    result["age_30_40"] = ((df["age"] >= 30) & (df["age"] < 40))
    result["age_40_50"] = ((df["age"] >= 40) & (df["age"] < 50))
    result["age_50_80"] = ((df["age"] >= 50) & (df["age"] <= 80))
    result["age_trash"] = ((df["age"] < 18) | (df["age"] > 80))

    # gender
    result["gender_1"] = df["gender"] == 1
    result["gender_2"] = df["gender"] == 2

    # currency_id
    result['currency_id_5'] = df['currency_id'] == 5
    result['currency_id_1'] = df['currency_id'] == 1
    result['currency_id_2'] = df['currency_id'] == 2
    result['currency_id_6'] = df['currency_id'] == 6
    result['currency_id_7'] = df['currency_id'] == 7
    result['currency_id_4'] = df['currency_id'] == 4
    result['currency_id_8'] = df['currency_id'] == 8

    # client_platform_id
    result['client_platform_id_2'] = df['client_platform_id'] == 2
    result['client_platform_id_9'] = df['client_platform_id'] == 9
    result['client_platform_id_3'] = df['client_platform_id'] == 3
    result['client_platform_id_12'] = df['client_platform_id'] == 12

    result['used_historical_prices'] = df['used_historical_prices']
    result['tried_to_change_asset'] = df['tried_to_change_asset']
    result['changed_deal_amount_manualy'] = df['changed_deal_amount_manualy']
    result['visit_traderoom'] = df['visit_traderoom']
    result['button_deposit_pag'] = df['button_deposit_pag']
    result['visited_withdrawal_page'] = df['visited_withdrawal_page']
    result['added_technical_analysis'] = df['added_technical_analysis']
    result['changed_chart_type'] = df['changed_chart_type']
    result['open_video_tutorial'] = df['open_video_tutorial']
    result['sell_option_used'] = df['sell_option_used']
    result['refreshed_demo'] = df['refreshed_demo']
    result['phone_confirmed'] = df['phone_confirmed']
    result['user_use_buyback'] = df['user_use_buyback']
    result['trading_indicator_added'] = df['trading_indicator_added']

    result['volume_train_digital'] = df['volume_train_digital']
    result['pnl_train_digital'] = df['pnl_train_digital']
    result['volume_train_cfd'] = df['volume_train_cfd']
    result['pnl_train_cfd'] = df['pnl_train_cfd']
    result['volume_train_forex'] = df['volume_train_forex']
    result['pnl_train_forex'] = df['pnl_train_forex']
    result['volume_train_crypto'] = df['volume_train_crypto']
    result['pnl_train_crypto'] = df['pnl_train_crypto']
    result['closed_count'] = df['closed_count']
    result['instrument_actives_count'] = df['instrument_actives_count']
    result['instrument_actives_digital_count'] = df['instrument_actives_digital_count']
    result['instrument_actives_cfd_count'] = df['instrument_actives_cfd_count']
    result['instrument_actives_forex_count'] = df['instrument_actives_forex_count']
    result['instrument_actives_crypto_count'] = df['instrument_actives_crypto_count']
    result['digital_count'] = df['digital_count']
    result['cfd_count'] = df['cfd_count']
    result['forex_count'] = df['forex_count']
    result['crypto_count'] = df['crypto_count']
    result['bin_count'] = df['bin_count']
    result['volume_train_bin'] = df['volume_train_bin']
    result['pnl_train_bin'] = df['pnl_train_bin']
    result['instrument_actives_bin_count'] = df['instrument_actives_bin_count']

    result = result.fillna(0)
    return result


def read_model(model_name, model_path, ignore_wrong_version=False):
    model_file_path = model_path + "/" + model_name + '.pkl'
    model_cfg_path = model_path + "/" + model_name + '.json'
    check_path(Path(model_file_path))
    check_path(Path(model_cfg_path))
    cfg_json = None
    logger.info(f"Read model cfg {model_cfg_path}")
    with open(model_cfg_path) as cfg_json_file:
        cfg_json = json.load(cfg_json_file)
    logger.info("Cfg loaded")
    logger.debug("Cfg:%s", cfg_json)
    logger.info(f"Read classifier {model_file_path}")
    main_threshold = get_field_from_cfg(cfg_json, 'main_threshold')
    sklearn_v = get_field_from_cfg(cfg_json, 'sklearn_v')
    if sklearn_v != sklearn.__version__:
        if not ignore_wrong_version:
            logger.error(f"Wrong sklearn version! Expected:{sklearn_v} Actual:{sklearn.__version__}")
            sys.exit(1)
    clf = joblib.load(model_file_path)
    logger.info("Classifier loaded")
    return clf, main_threshold, cfg_json


def make_class_prediction(clf, X, threshold):
    proba_xs = clf.predict_proba(X)[:, 1]
    return proba_xs > threshold, proba_xs


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def get_mobile_users_for_prediction(wpad_connect) -> List[int]:
    sql = sql_get_unhandled_mobile_users(APPS_FLYER_TABLE, MQL_MOBILE_TARGET_TABLE)
    return __get_users_for_prediction(wpad_connect, sql)


def get_web_users_for_prediction(wpad_connect) -> List[int]:
    sql = sql_get_unhandled_web_users(ADWORDS_CLICK_HISTORY, MQL_WEB_TARGET_TABLE)
    return __get_users_for_prediction(wpad_connect, sql)


def __get_users_for_prediction(wpad_connect, sql) -> List[int]:
    logger.info("Get user for prediction")
    result = []
    try:
        with wpad_connect.cursor() as cur:
            logger.debug("Execute query 'get unhandled users'. SQL:%s", repr(sql))
            cur.execute(sql)
            result = [x[0] for x in cur.fetchall()]
    finally:
        wpad_connect.commit()
    logger.info("Unhandled users count: %s", len(result))
    return result


def sql_query_to_dataframe(sql, connection):
    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER DELIMITER ','".format(sql)
    output_file_path = "/tmp/" + str(hashlib.sha1(outputquery.encode()).hexdigest()) + ".csv"
    try:
        connection.autocommit = True
        with connection.cursor() as cur, open(output_file_path, "w") as file:
            logger.info(f"Copy data from sql to {output_file_path} ...")
            cur.copy_expert(outputquery, file)
            logger.info(f"Copy successfully complete ...")
    finally:
        connection.autocommit = False
        connection.commit()
    return pd.read_csv(output_file_path)


def get_dataset_for_users(gp_connect, user_ids):
    # 'c_actives_train_count' 10317
    logger.info("Get dataset for users")
    sql_for_user_data = sql_user_data(user_ids)
    sql_for_stat_tags = sql_user_stat_tags_gp(user_ids)
    logger.debug("Query user data. SQL:%s", repr(sql_for_user_data))
    logger.debug("Query stat tags data. SQL:%s", repr(sql_for_stat_tags))
        # user_data = sqlio.read_sql_query(sql_for_user_data, gp_connect)
    # user_tagas_data = sqlio.read_sql_query(sql_for_stat_tags, wpad_connect)
    logger.info("Get user data from GP")
    user_data = sql_query_to_dataframe(sql_for_user_data, gp_connect)
    logger.info("Get tags data from GP")
    user_tagas_data = sql_query_to_dataframe(sql_for_stat_tags, gp_connect)

    logger.info("Combine to one dataset")
    ds_users_i = user_data.set_index('user_id')
    ds_stat_tags_i = user_tagas_data.set_index('user_id')

    data_i = ds_users_i.combine_first(ds_stat_tags_i)
    data = data_i.reset_index()
    data[ds_stat_tags_i.columns.values] = data[ds_stat_tags_i.columns.values].fillna(0)
    return data


def execute_and_fill_df(conn, sql):
    local_cursor = conn.cursor()
    local_cursor.execute(sql)
    local_cursor.fetchall()


MQLData = namedtuple("MQLData", "user_id is_mql proba")


def find_mqls(gp_connect, user_ids, chunk_size) -> List[MQLData]:
    logger.info("Find mqls")
    result = []
    for chunk_num, user_ids_batch in enumerate(chunker(user_ids, chunk_size)):
        logger.info(f"Handle chunk_num: {chunk_num}")
        df = get_dataset_for_users(gp_connect, user_ids_batch)
        if len(df) == 0:
            logger.warning("No MQLs found: %s", result)
        else:
            logger.debug("Dataframe:\n%s", df)
            X_df = features_engineering(df)
            X = X_df.as_matrix()
            y, proba_xs = make_class_prediction(clf, X, main_threshold)
            for i, user_id in enumerate(df['user_id']):
                result.append(MQLData(user_id=user_id, is_mql=y[i], proba=proba_xs[i]))
            logger.debug("Mqls found: %s", result)
    return result


def save_mobile_mgl_data(wpad_connect, users_with_mql: List[MQLData]):
    __save_mgl_data(wpad_connect, MQL_MOBILE_TARGET_TABLE, users_with_mql,
                    lambda user_ids: sql_insert_mobile_mql_for_users(MQL_MOBILE_TARGET_TABLE, user_ids))


def save_web_mgl_data(wpad_connect, users_with_mql: List[MQLData]):
    __save_mgl_data(wpad_connect, MQL_WEB_TARGET_TABLE, users_with_mql,
                    lambda user_ids: sql_insert_web_mql_for_users(ADWORDS_CLICK_HISTORY, MQL_WEB_TARGET_TABLE,
                                                                  user_ids))


def __save_mgl_data(wpad_connect, taret_table_name, users_with_mql: List[MQLData], userids_to_sql_fun):
    only_mql_user_ids = [data.user_id for data in users_with_mql if data.is_mql]
    if len(only_mql_user_ids) > 0:
        sql = userids_to_sql_fun(only_mql_user_ids)
        try:
            with wpad_connect.cursor() as cur:
                logger.info(f"Start inserting MQL data to {taret_table_name}.")
                logger.debug("Inserting MQL data SQL:%s", repr(sql))
                cur.execute(sql)
                rowcount = cur.rowcount
                logger.info(f"Complete inserting MQL data to {taret_table_name}. Row count:({rowcount}).")
        finally:
            wpad_connect.commit()

    else:
        logger.warning("No MQL data")


def execute_common_insert_sql(sql):
    try:
        with wpad_connect.cursor() as cur:
            logger.info(f"Start common inserting.")
            logger.debug("Commn inserting SQL:%s", repr(sql))
            cur.execute(sql)
            rowcount = cur.rowcount
            logger.info(f"Complete common Row count:({rowcount}).")
    finally:
        wpad_connect.commit()


def connect_to_gp(gp_user, gp_pass):
    logger.info("Connect to GP")
    return psycopg2.connect(database="reporting",
                            user=gp_user,
                            password=gp_pass,
                            host="node01.prod.analytics.wz-ams.lo.mobbtech.com",
                            port=6432)


def connect_to_wpad(wp_user, wp_pass):
    logger.info("Connect to WPAD02")
    return psycopg2.connect(database="options_analytics",
                            user=wp_user,
                            password=wp_pass,
                            host="pgsql01.prod.analyze.wz-ams.lo.mobbtech.com",
                            port=5432)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, help="App config", required=True)
    parser.add_argument("--model_path", type=str, help="Path to model")
    args = parser.parse_args()
    return args


def get_config() -> AppConfig:
    args = parse_args()
    if hasattr(args, 'config'):
        if hasattr(args, 'model_path') and args.model_path is not None:
            model_path = args.model_path
        else:
            model_path = os.getcwd()

        config = configparser.ConfigParser()
        config.read(args.config)
        gp_user = config['main']['gp_user']
        gp_pass = config['main']['gp_pass']
        wp_user = config['main']['wp_user']
        wp_pass = config['main']['wp_pass']
        ver_ignore = config['main']['ver_ignore'].strip().lower() == 'true'
        return AppConfig(gp_user=gp_user, gp_pass=gp_pass, wp_user=wp_user, wp_pass=wp_pass, ver_ignore=ver_ignore,
                         model_path=model_path)
    else:
        logger.error("Can't find config arguments. Use -c or --config")
        sys.exit(1)


if __name__ == "__main__":
    logger.info("Launch")
    config = get_config()
    try:
        with connect_to_gp(config.gp_user, config.gp_pass) as gp_connect, \
                connect_to_wpad(config.wp_user, config.wp_pass) as wpad_connect:
            clf, main_threshold, _ = read_model(model_name=MODEL_NAME, model_path=config.model_path,
                                                ignore_wrong_version=config.ver_ignore)
            wpad_connect.autocommit = True
            logger.info("Handle mobile users")
            mobile_user_ids = get_mobile_users_for_prediction(wpad_connect)
            mobile_users_with_mql: List[MQLData] = find_mqls(gp_connect, mobile_user_ids, CHUNK_SIZE)
            logger.debug(mobile_users_with_mql)
            save_mobile_mgl_data(wpad_connect, mobile_users_with_mql)

            logger.info("Handle web users")
            web_user_ids = get_web_users_for_prediction(wpad_connect)
            web_users_with_mql: List[MQLData] = find_mqls(gp_connect, web_user_ids, CHUNK_SIZE)
            save_web_mgl_data(wpad_connect, web_users_with_mql)

            logger.info("Handle non predicted web deponators")

            web_i_sql = sql_insert_web_non_predicted_deponators(MQL_WEB_TARGET_TABLE, hours_after_reg=HOURS_AFTER_REG,
                                                                days_before_now=DAYS_BEFORE_NOW)
            execute_common_insert_sql(web_i_sql)

            logger.info("Handle non predicted mobile deponators")
            mob_i_sql = sql_insert_mobile_predicted_deponators(MQL_MOBILE_TARGET_TABLE, hours_after_reg=HOURS_AFTER_REG,
                                                               days_before_now=DAYS_BEFORE_NOW)
            execute_common_insert_sql(mob_i_sql)


    except Exception as e:
        logger.exception("Unexpected error.")

    logger.info("Complete")
