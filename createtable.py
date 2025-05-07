import sys
import types
from unittest import mock  # ✅ Correctly import mock from unittest

# --- Full mock of awsglue and pyspark module tree ---
awsglue_mock = types.ModuleType("awsglue")
context_mock = types.ModuleType("awsglue.context")
utils_mock = types.ModuleType("awsglue.utils")
pyspark_context_mock = types.ModuleType("pyspark.context")

context_mock.GlueContext = mock.MagicMock()
utils_mock.getResolvedOptions = mock.MagicMock()
pyspark_context_mock.SparkContext = mock.MagicMock()

sys.modules["awsglue"] = awsglue_mock
sys.modules["awsglue.context"] = context_mock
sys.modules["awsglue.utils"] = utils_mock
sys.modules["pyspark.context"] = pyspark_context_mock
sys.modules["pyspark"] = types.SimpleNamespace(SparkContext=mock.MagicMock())  # Mock pyspark root module
