import countTravelTime
import pytest
import pyspark

# create necessary test framework
@pytest.fixture(scope="session")
def spark_context(request):
    conf = (pyspark.SparkConf()
            .setMaster("local[2]")
            .setAppName("pytest-pyspark-local-testing")
            )
    sc = pyspark.SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())
    
    return sc

# add test cases
def test_that_requires_sc(spark_context):
  assert true
  #assert PythonWordCount.doJob(spark_context.parallelize(["12123 123123","3423"])).count() == 3