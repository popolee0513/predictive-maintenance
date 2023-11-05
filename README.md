# Real-time Machinery Fault Detection

![Pipeline Image](https://github.com/popolee0513/predictive-maintenance/blob/main/pipeline.jpg)

## Introduction

This project is focused on real-time machinery fault detection using machine learning. The primary data source for this project is the [MetroPT2 dataset](https://zenodo.org/records/7766691), a benchmark dataset for predictive maintenance.The main goal of this project is to develop a real-time machinery fault detection pipeline.

## Data Source

The project relies on the [MetroPT2 dataset](https://zenodo.org/records/7766691) for training and testing. This dataset provides sensor data, which is essential for our machinery fault detection model.

## Methodology

I use the following techniques:

- **Feature Engineering:** I create rolling window features and lag features to capture patterns in the sensor data. These features provide valuable insights into the machinery's behavior over time.

- **Machine Learning Model:** The predictive model is based on XGBoost, a popular machine learning algorithm. It achieves a recall rate of approximately 80% and an F1 score of around 70%.

## Real-time Pipeline

To create a real-time machinery fault detection pipeline, I use the following technologies:

- **Confluent Cloud Kafka (Free Trial Version):** Kafka serves as the messaging backbone to enable real-time data ingestion and processing.

- **PySpark Streaming:** I utilize PySpark's streaming capabilities for data processing. Window-based aggregation and streaming join operations are used to create real-time features for the machine learning model.

- **Real-time Prediction:** My trained XGBoost model is employed to make predictions in real time. If a data point is classified as an abnormality, it is logged in BigQuery for further analysis and historical record-keeping.
