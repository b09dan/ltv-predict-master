import json
import logging

import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import f1_score
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import roc_curve, auc
from sklearn.model_selection import train_test_split

LOG_LEVEL = "DEBUG"

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
ch = logging.StreamHandler()
ch.setLevel(LOG_LEVEL)
formatter = logging.Formatter(
    '%(asctime)s [%(filename)s.%(lineno)d] %(processName)s %(levelname)-1s %(name)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


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


def to_clf_data(df):
    X_df = (features_engineering(df))
    X = X_df.as_matrix()
    y = (df['deposits'] > 0).as_matrix()
    return X, y, X_df


def cutoff_youdens_j(fpr, tpr, thresholds):
    j_scores = tpr - fpr
    j_ordered = sorted(zip(j_scores, thresholds))
    return j_ordered[-1][1]


if __name__ == "__main__":
    logger.info("Launch")
    try:
        logger.info("Read data...")
        data = pd.read_pickle("mql_data/mql_dataset.gzip", compression="gzip")
        X, y, X_df = to_clf_data(data)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=13)
        n_estimator = 300
        random_state = 0
        clf = RandomForestClassifier(max_depth=4,
                                     n_jobs=4,
                                     n_estimators=n_estimator,
                                     random_state=random_state,
                                     verbose=1,
                                     class_weight="balanced_subsample")
        logger.info("Fit classifier...")
        clf.fit(X_train, y_train)
        logger.info("Test classifier")
        y_predict_prob = clf.predict_proba(X_test)[:, 1]
        fpr, tpr, thresholds = roc_curve(y_test, y_predict_prob)
        roc_auc = auc(fpr, tpr)
        logger.info(f"Roc_auc:{roc_auc}")
        main_threshold = cutoff_youdens_j(fpr, tpr, thresholds)
        logger.info(f"Threshold :{main_threshold}")
        cnf_matrix = confusion_matrix(y_test, y_predict_prob > main_threshold)
        logger.info(f"Confusion matrix:\n{cnf_matrix}")
        ncnf_matrix = cnf_matrix.astype('float') / cnf_matrix.sum(axis=1)[:, np.newaxis]
        logger.info(f"Confusion matrix normalized:\n{ncnf_matrix}")
        y_predict = y_predict_prob > main_threshold
        acc = accuracy_score(y_test, y_predict)
        prec = precision_score(y_test, y_predict)
        rec = recall_score(y_test, y_predict)
        f1_s = f1_score(y_test, y_predict)
        logger.info(f"Accuracy:{acc}")
        logger.info(f"Precision:{prec}")
        logger.info(f"Recall:{rec}")
        logger.info(f"F1_score:{f1_s}")
        feature_columns = list(X_df.columns.values)
        imp_f = sorted(list(zip(feature_columns, clf.feature_importances_)), key=lambda x: -x[1])
        logger.info("Feature importance:")
        for f, i in imp_f:
            logger.info(f"{f},{i}")

        model_name = 'random_forest_04'
        model_filename = model_name + '.pkl'
        joblib.dump(clf, model_filename)
        json_data = {"sklearn_v": sklearn.__version__,
                     "roc_auc": roc_auc,
                     "main_threshold": main_threshold,
                     "accuracy": acc, "precision": prec,
                     "recall": rec, "feature_columns": feature_columns}
        with open(model_name + '.json', 'w') as outfile:
            json.dump(json_data, outfile)
            logger.info(f"Save model to {model_filename}")


    except Exception as e:
        logger.exception("Unexpected error.")

    logger.info("Complete")
