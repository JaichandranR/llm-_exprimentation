import sys
import types
from unittest import mock

# --- Full mock of awsglue and pyspark module tree ---
awsglue_mock = types.ModuleType("awsglue")
context_mock = types.ModuleType("awsglue.context")
utils_mock = types.ModuleType("awsglue.utils")
pyspark_mock = types.ModuleType("pyspark")
pyspark_context_mock = types.ModuleType("pyspark.context")

# Add dummy classes/functions if your code uses them
context_mock.GlueContext = mock.MagicMock()
utils_mock.getResolvedOptions = mock.MagicMock()
pyspark_context_mock.SparkContext = mock.MagicMock()

# Register the fake modules in sys.modules
sys.modules["awsglue"] = awsglue_mock
sys.modules["awsglue.context"] = context_mock
sys.modules["awsglue.utils"] = utils_mock
sys.modules["pyspark"] = pyspark_mock
sys.modules["pyspark.context"] = pyspark_context_mock

# Now you can safely import your target module
from src.main.python import common_data_sync
