import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e875e231e049f046a1d8958','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	PredictHighestIncomeRegression_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e875e231e049f046a1d8958", spark, "{'url': '/Demo/PredictHighestIncomeTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi0ef076722999cf4cd8859e9aafdb7b76', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e875e231e049f046a1d8958','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e875e231e049f046a1d8958','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e875e231e049f046a1d8959','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	PredictHighestIncomeRegression_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e875e231e049f046a1d8958"],{"5e875e231e049f046a1d8958": PredictHighestIncomeRegression_DBFS}, "5e875e231e049f046a1d8959", spark,json.dumps( {"FE": [{"transformationsData": {"feature_label": "Occupation"}, "feature": "Occupation", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "215", "mean": "", "stddev": "", "min": "Accountants and auditors", "max": "Writers and authors", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "M_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "215", "mean": "176.37", "stddev": "493.1", "min": "0", "max": "5548", "missing": "0"}}, {"transformationsData": {"feature_label": "M_weekly"}, "feature": "M_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "215", "mean": "1008.11", "stddev": "365.43", "min": "1001", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "F_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "215", "mean": "133.25", "stddev": "343.07", "min": "0", "max": "2104", "missing": "0"}}, {"transformationsData": {"feature_label": "F_weekly"}, "feature": "F_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "215", "mean": "797.96", "stddev": "286.9", "min": "1025", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "All_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "215", "mean": "309.68", "stddev": "746.03", "min": "0", "max": "7551", "missing": "0"}}, {"transformationsData": {"feature_label": "All_weekly"}, "feature": "All_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "215", "mean": "914.07", "stddev": "329.22", "min": "1001", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "Occupation_transform", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "215", "mean": "107.0", "stddev": "62.21", "min": "0.0", "max": "214.0", "missing": "0"}}, {"feature": "M_weekly_transform", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "215", "mean": "22.59", "stddev": "31.24", "min": "0.0", "max": "98.0", "missing": "0"}}, {"feature": "F_weekly_transform", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "215", "mean": "11.56", "stddev": "20.28", "min": "0.0", "max": "70.0", "missing": "0"}}, {"feature": "All_weekly_transform", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "215", "mean": "33.94", "stddev": "39.61", "min": "0.0", "max": "120.0", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e875e231e049f046a1d8959','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e875e231e049f046a1d8959','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e875e231e049f046a1d895a','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	PredictHighestIncomeRegression_AutoML = tpot_execution.Tpot_execution.run(["5e875e231e049f046a1d8959"],{"5e875e231e049f046a1d8959": PredictHighestIncomeRegression_AutoFE}, "5e875e231e049f046a1d895a", spark,json.dumps( {"model_type": "regression", "label": "All_weekly", "features": ["Occupation", "M_workers", "M_weekly", "F_workers", "F_weekly", "All_workers"], "percentage": "40", "executionTime": "5", "sampling": "0", "sampling_value": "", "run_id": "", "model_id": "5e875fec1e049f046a1d89e0", "ProjectName": "ML Sample Problems", "PipelineName": "PredictHighestIncomeRegression", "pipelineId": "5e875e231e049f046a1d8957", "userid": "5df78f4be2f2eff24740bbd7", "runid": "", "url_ResultView": "http://13.68.212.36:3200", "experiment_id": "480623611921769"}))

	PipelineNotification.PipelineNotification().completed_notification('5e875e231e049f046a1d895a','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e875e231e049f046a1d895a','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)

