import argparse
import configparser
import gzip
import logging
import os
import sys
from collections import namedtuple
from pathlib import Path

import pandas as pd
import psycopg2

from utils import sql_stat_tags_dataset

LOG_LEVEL = "DEBUG"

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
ch = logging.StreamHandler()
ch.setLevel(LOG_LEVEL)
formatter = logging.Formatter(
    '%(asctime)s [%(filename)s.%(lineno)d] %(processName)s %(levelname)-1s %(name)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

AppConfig = namedtuple("AppConfig", "gp_user gp_pass wp_user wp_pass ver_ignore model_path reload gzip")


def connect_to_gp(gp_user, gp_pass):
    logger.info("Connect to GP")
    return psycopg2.connect(database="reporting",
                            user=gp_user,
                            password=gp_pass,
                            host="node01.prod.analytics.wz-ams.lo.mobbtech.com",
                            port=5432)


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
    parser.add_argument("-z", "--gzip", help="Use gzip compression", required=False, action="store_true")
    parser.add_argument("-r", "--reload", help="Reload existed files", required=False, action="store_true")
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
                         model_path=model_path, reload=args.reload, gzip=args.gzip)
    else:
        logger.error("Can't find config arguments. Use -c or --config")
        sys.exit(1)


def sizeof_fmt(num, suffix='b'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def load_sql_result_to_file(connect, sql, file_path, config):
    if config.gzip:
        file_path = file_path + ".gz"

    path = Path(file_path)
    if not config.reload and path.exists() and path.is_file():
        logger.info(f"No reload for:{file_path}")
    else:
        outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER DELIMITER ','".format(sql)
        file = None
        try:
            if config.gzip:
                file = gzip.open(file_path, "wb")
            else:
                file = open(file_path, "w")

            with connect.cursor() as cur:
                logger.info(f"Collect data for {file_path} ...")
                cur.copy_expert(outputquery, file)
        finally:
            if file:
                file.close()
            connect.commit()
    logger.info(f"Final file size: {sizeof_fmt(os.path.getsize(path))}")


def read_user_ids(user_file):
    logger.info("Read user_id list")
    if config.gzip:
        user_df = pd.read_csv(user_file + ".gz", compression='gzip')
    else:
        user_df = pd.read_csv(user_file)
    user_ids = list(user_df['user_id'])
    return user_ids


def rename(prefix, df):
    column_mapper = {x: f"{prefix}{x}" for x in df.columns.values if x != 'user_id'}
    return df.rename(index=str, columns=column_mapper)


user_data_sql = """
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
      WHERE u.created > '2017-12-01' AND u.created < '2018-02-01' AND client_platform_id IN (1,2,3,4,5,6,7,8,9,11,12)),
    ni_data AS (SELECT
                  ap.user_id,
                  --   extract(EPOCH FROM (min(ap.create_at) - u.created)) AS first_deal_interval,
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

transactions_sql = """SELECT
      user_id,
      count(user_id) AS deposits
    FROM stat_transactions_data
    WHERE
      registration_date > '2017-12-01'
      AND registration_date < '2018-02-01'
      AND transaction_date < registration_date + INTERVAL '30 days'
      AND balance_type = 1
    GROUP BY user_id
    """

if __name__ == "__main__":
    logger.info("Launch")
    config = get_config()
    try:
        with connect_to_wpad(config.wp_user, config.wp_pass) as wpad_connect, \
                connect_to_gp(config.gp_user, config.gp_pass) as gp_conect:
            days_after_reg = 1
            user_file = f"mql_data/user_data.csv"
            load_sql_result_to_file(gp_conect, user_data_sql, user_file, config)
            transactions_file = f"mql_data/transactions.csv"
            load_sql_result_to_file(wpad_connect, transactions_sql, transactions_file, config)
            tags_file = f"mql_data/stat_tags_data_d{days_after_reg}.csv"
            load_sql_result_to_file(wpad_connect, sql_stat_tags_dataset(days_after_reg), tags_file, config)
            # user_ids = read_user_ids(user_file)
            # logger.info(f"User ids found: {len(user_ids)}")
            # ni_file = f"mql_data/ni_data_d{days_after_reg}.csv"
            # load_sql_result_to_file(gp_conect, sql_new_instruments_dataset_for_users(user_ids, days_after_reg), ni_file,
            #                         config)
            # binary_file = f"mql_data/binary_data_d{days_after_reg}.csv"
            # load_sql_result_to_file(gp_conect, sql_binary_dataset_for_users(user_ids, days_after_reg), binary_file,
            #                         config)

            logger.info("Read files ...")
            ds_users = pd.read_csv('mql_data/user_data.csv')
            ds_transactions = pd.read_csv('mql_data/transactions.csv')
            ds_stat_tags = pd.read_csv('mql_data/stat_tags_data_d1.csv')
            # ds_binary_d1 = rename("b_", pd.read_csv('mql_data/binary_data_d1.csv'))
            # ds_newinstr_d1 = rename("n_", pd.read_csv('mql_data/ni_data_d1.csv'))

            logger.info("Join file to one dataset ...")
            ds_users_i = ds_users.set_index('user_id')
            ds_transactions_i = ds_transactions.set_index('user_id')
            ds_stat_tags_i = ds_stat_tags.set_index('user_id')

            data_i = ds_users_i.combine_first(ds_stat_tags_i).combine_first(ds_transactions_i)
            data = data_i.reset_index()
            data[ds_stat_tags_i.columns.values] = data[ds_stat_tags_i.columns.values].fillna(0)
            data[ds_transactions_i.columns.values] = data[ds_transactions_i.columns.values].fillna(0)

            logger.info("Find NA")
            for col_name in data.columns.values:
                null_count = data[col_name].isnull().sum()
                if null_count > 0:
                    logger.info(f"{col_name}, {null_count}")

            dataset_path = "mql_data/mql_dataset.gzip"
            logger.info(f"Save data ({dataset_path})...")
            data.to_pickle(dataset_path, protocol=4, compression='gzip')
            logger.info("File saved")


    except Exception as e:
        logger.exception("Unexpected error.")

    logger.info("Complete")
