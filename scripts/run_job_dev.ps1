$commands = @'
cd /opt/spark/
export INPUT_PATH=/opt/spark/sample_data/bronze
export OUTPUT_PATH=/opt/spark/sample_data/silver
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/work-dir/aws_pipeline-0.0.1-py3-none-any.whl \
  /opt/spark/jobs/bronze_to_silver_job.py
'@ -replace "`r", ""

docker exec -i spark-master bash -c $commands