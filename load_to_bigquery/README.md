## Step processing with Step Functions
The following commands can be executed in Cloud Shell.

### Bucket creation
```
$> gsutil mb -c regional -l europe-west1 gs://meetup-step-processing-input
$> gsutil mb -c regional -l europe-west1 gs://meetup-step-processing-output
```

### Bigquery tables creation
```
$> bq --location=EU mk --dataset meetup-hands-on-gcp-2019:googleplaystore_step
$> bq mk --table meetup-hands-on-gcp-2019:googleplaystore_step.play_store play-store-schema.json
```

## Execute step function
In order to execute step functions the source file needs to be loaded to Cloud Storage bucket.

```
gsutil cp dataset/input_googleplaystore.csv gs://meetup-step-processing-input/input/
```