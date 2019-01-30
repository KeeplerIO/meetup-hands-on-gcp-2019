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
### Dependencies installation (in virtualenv)
```
$> virtualenv venv
$> source venv/bin/activate
$> pip install apache-beam
$> pip install apache-beam[gcp]
```
### Check dependencies installation (optional)
```
$> python -m apache_beam.examples.wordcount_minimal --input LICENSE --output counts
```
This commands should generate a file with the word count of the LICENSE file of this repo.

### Run a sample Apache Beam pipeline in Dataflow
