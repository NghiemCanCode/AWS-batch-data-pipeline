# Helm values for the Airbyte chart installed by abctl. Written for Helm chart V2 (Airbyte 2.x; V1 is unsupported
# since 2.1.0). Rendered by 071-airbyte-install.sh: __STORAGE_BUCKET__ and __REGION__ are replaced with real values.
#
# Helm silently ignores keys it does not know, and values are case-sensitive.
# A wrong key gives no error, only a missing setting: verify after the first sync (deploy.md, Appendix 4).
global:
  # Airbyte's own storage (job logs, state, workload output) in S3 instead of the in-cluster MinIO.
  # Credentials come from the instance profile through IMDS (hop limit 3, step 3.2).
  storage:
    # Must be lowercase. With "S3" the chart does not pass the region to the pods, while the app still builds an
    # S3 client and every pod crashes with "region must not be blank or empty".
    type: "s3"
    bucket:
      log: "__STORAGE_BUCKET__"
      state: "__STORAGE_BUCKET__"
      workloadOutput: "__STORAGE_BUCKET__"
      activityPayload: "__STORAGE_BUCKET__"
    s3:
      region: "__REGION__"
      authenticationType: "instanceProfile"

  # Every sync starts pods for the source and destination connectors. Without limits, one CDC initial snapshot
  # can take all 16 GB and evict the server and database pods.
  jobs:
    resources:
      requests:
        cpu: "250m"
        memory: "1Gi"
      limits:
        cpu: "2"
        memory: "4Gi"

# In V2, configuring external storage already disables the internal MinIO. Kept explicit; ignored if unknown.
minio:
  enabled: false
