# Project 1 — Real-Time Personalization Pipeline Architecture

## Purpose

This project is a production-style real-time personalization pipeline.

It ingests synthetic clickstream events through Kafka, processes them with Spark Structured Streaming, separates invalid records into a dead-letter queue, stores valid data through Bronze, Silver, and Gold lakehouse layers, publishes user-level features to PostgreSQL, and exposes those features through a FastAPI serving endpoint.

The goal is not just to move data.

The goal is to show an end-to-end data platform pattern:

```text
events → streaming validation → lakehouse layers → feature table → API serving
