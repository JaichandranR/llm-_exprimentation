import sys
import types
import mock  # or from unittest import mock if not already

# Mock SparkContext and GlueContext to avoid real Spark dependencies
pyspark_mock = types.ModuleType("pyspark")
pyspark_mock.SparkContext = mock.MagicMock()
sys.modules["pyspark"] = pyspark_mock

awsglue_mock = types.ModuleType("awsglue")
awsglue_context_mock = types.ModuleType("awsglue.context")
awsglue_context_mock.GlueContext = mock.MagicMock()
awsglue_utils_mock = types.ModuleType("awsglue.utils")
awsglue_utils_mock.getResolvedOptions = mock.MagicMock()

awsglue_mock.context = awsglue_context_mock
awsglue_mock.utils = awsglue_utils_mock
sys.modules["awsglue"] = awsglue_mock
sys.modules["awsglue.context"] = awsglue_context_mock
sys.modules["awsglue.utils"] = awsglue_utils_mock
