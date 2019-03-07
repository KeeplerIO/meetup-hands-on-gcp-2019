## Batch processing with Dataflow
The following commands can be executed in Cloud Shell.

### Bucket creation
```
$> gsutil mb -c regional -l europe-west1 gs://meetup-batch-processing
```
### Dataset copy from other bucket
```
gsutil cp gs://meetup-hands-on-gcp-2019/google_play_store/googleplaystore.csv gs://meetup-batch-processing/input/
```
### Bigquery table creation
```
$> bq mk --table meetup-hands-on-gcp-2019:googleplaystore_batch_dataflow.play_store play-store-schema.json
```
### Sample project generation (not required)
```
$> mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=2.10.0 \
      -DgroupId=io.keepler \
      -DartifactId=batch-processing-beam \
      -Dversion="0.1" \
      -Dpackage=io.keepler.beam.batch \
      -DinteractiveMode=false
```
### Execution
```
$> mvn compile exec:java \
       -Dexec.mainClass=io.keepler.beam.batch.KeeplerSample \
       -Dexec.args="--runner=DataflowRunner --inputFile=gs://meetup-batch-processing/input/googleplaystore.csv --project=meetup-hands-on-gcp-2019 --tempLocation=gs://meetup-batch-processing/tmp/" \
       -Pdataflow-runner
```
This commands should create a new Dataflow job that populates the play_store BigQuery table.
