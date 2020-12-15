
import sys

from great_expectations import DataContext

# checkpoint configuration
context = DataContext("/home/demilsonfayika/staging/results")
suite = context.get_expectation_suite("staging.validation")
# You can modify your BatchKwargs to select different data
batch_kwargs = {"table": "staging", "schema": "public", "datasource": "stagingtable"}

# checkpoint validation process
batch = context.get_batch(batch_kwargs, suite)
results = context.run_validation_operator("action_list_operator", [batch])

if not results["success"]:
    print('{"result":"fail"}')
    sys.exit(0)

print('{"result":"pass"}')
sys.exit(0)
