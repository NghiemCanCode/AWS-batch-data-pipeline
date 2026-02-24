docker exec -i spark-master bash -c "
cd /opt/spark/

export INPUT_PATH=/opt/spark/sample_data
export OUTPUT_PATH=/opt/spark/sample_data

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/work-dir/aws_pipeline-0.0.1-py3-none-any.whl \
  /opt/spark/jobs/bronze_to_silver_job.py
"