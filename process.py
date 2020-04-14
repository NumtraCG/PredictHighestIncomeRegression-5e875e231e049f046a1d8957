import json
import Connectors
import Transformations
import AutoML
try:
    PredictHighestIncomeRegression_DBFS = Connectors.DBFSConnector.fetch(
        [], {}, "5e875e231e049f046a1d8958", spark, "{'url': '/Demo/PredictHighestIncomeTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi0ef076722999cf4cd8859e9aafdb7b76', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

except Exception as ex:
    print(ex)
try:
    PredictHighestIncomeRegression_AutoFE = Transformations.TransformationMain.run(["5e875e231e049f046a1d8958"], {"5e875e231e049f046a1d8958": PredictHighestIncomeRegression_DBFS}, "5e875e231e049f046a1d8959", spark, json.dumps({"FE": [{"transformationsData": {"feature_label": "Occupation"}, "feature": "Occupation", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "518", "mean": "", "stddev": "", "min": "AGRICULTURAL", "max": "Writers and authors", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "M_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "518", "mean": "331.95", "stddev": "2744.98", "min": "0", "max": "60746", "missing": "0"}}, {"transformationsData": {"feature_label": "M_weekly"}, "feature": "M_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "518", "mean": "1005.59", "stddev": "396.25", "min": "1001", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "F_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "518", "mean": "267.17", "stddev": "2215.85", "min": "0", "max": "48334", "missing": "0"}}, {"transformationsData": {"feature_label": "F_weekly"}, "feature": "F_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "518", "mean": "806.1", "stddev": "301.72", "min": "1002", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {
                                                                                   "transformationsData": {}, "feature": "All_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "518", "mean": "599.17", "stddev": "4921.28", "min": "0", "max": "109080", "missing": "0"}}, {"transformationsData": {"feature_label": "All_weekly"}, "feature": "All_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "518", "mean": "911.86", "stddev": "351.11", "min": "1000", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "Occupation_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "518", "mean": "258.5", "stddev": "149.68", "min": "0.0", "max": "517.0", "missing": "0"}}, {"feature": "M_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "518", "mean": "39.91", "stddev": "61.33", "min": "0.0", "max": "202.0", "missing": "0"}}, {"feature": "F_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "518", "mean": "28.79", "stddev": "49.79", "min": "0.0", "max": "172.0", "missing": "0"}}, {"feature": "All_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "518", "mean": "68.83", "stddev": "84.94", "min": "0.0", "max": "264.0", "missing": "0"}}]}))

except Exception as ex:
    print(ex)
try:
    AutoML.functionRegression(PredictHighestIncomeRegression_AutoFE, [
                              "Occupation", "M_workers", "M_weekly", "F_workers", "F_weekly", "All_workers"], "All_weekly")

except Exception as ex:
    print(ex)
