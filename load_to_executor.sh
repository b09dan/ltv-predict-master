#!/usr/bin/env bash

rsync -avz ./*.py apshenichniy@executer01.prod.analytics.wz-ams.lo.mobbtech.com:~/ltv-predict
rsync -avz ./random_forest* apshenichniy@executer01.prod.analytics.wz-ams.lo.mobbtech.com:~/ltv-predict