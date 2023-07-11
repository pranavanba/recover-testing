library(synapser)
library(arrow)
library(dplyr)

synapser::synLogin(authToken = daemon_pat)
PARQUET_FOLDER <- "syn51406699"

# Get STS credentials
token <- synapser::synGetStsStorageToken(
  entity = PARQUET_FOLDER,
  permission = "read_only",
  output_format = "json")

# Pass STS credentials to Arrow filesystem interface
s3 <- arrow::S3FileSystem$create(
  access_key = token$accessKeyId,
  secret_key = token$secretAccessKey,
  session_token = token$sessionToken,
  region="us-east-1")

# List Parquet datasets
base_s3_uri <- paste0(token$bucket, "/", token$baseKey)
parquet_datasets <- s3$GetFileInfo(arrow::FileSelector$create(base_s3_uri, recursive=T))
for (dataset in parquet_datasets) {
  print(dataset$path)
}

# Serialize Parquet dataset as data frame
# metadata <- arrow::open_dataset(s3$path(paste0(base_s3_uri, "/", "dataset_archivemetadata_v1")))
# metadata_df <- dplyr::collect(metadata)
