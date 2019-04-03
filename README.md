# Meetup hands on gcp 2019

This repository contains source code for Keepler's meetup "Google Cloud Platform for Big Data".

The repository contains thre different use cases.

* **Step processing**: A process deployed in Cloud Functions loads the dataset and transform it. Other step loads the transformed CSV and write it to BigQuery.
* **Batch processing**: Dataflow loads data from Cloud Storage and applies some transformations. Transformed data are loaded in BigQuery.
* **Streaming processing**: An evolution of batch processing. Dataflow ingest streaming data from Pub/Sub service. After some transformations, Dataflow loads data to BigQuery.

Note: *The [Google Play Store](https://www.kaggle.com/lava18/google-play-store-apps#googleplaystore.csv) dataset is used for all scenarios.*
