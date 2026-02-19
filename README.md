# Violent Crime Data Pipeline – End to End

End-to-end data engineering project for historical crime analytics and simulated real-time crime event streaming across U.S. counties.

---

## Overview

This project implements a complete end-to-end data pipeline following an ETL methodology.  
Historical crime data is cleaned and transformed using Python, then used to simulate real-time crime events published to Apache Kafka. These events are consumed and loaded into Google BigQuery for analytical purposes and visualization in Tableau and Looker Studio.

---

## Architecture

1. **ETL Process (Python)**
   - Ingests historical crime data (CSV)
   - Cleans, normalizes, and transforms data
   - Generates analytical features (rates per 100k, severity, timestamps)

2. **Event Streaming (Apache Kafka)**
   - Simulated crime events produced to Kafka topics
   - Consumers read streaming data in real time

3. **Data Warehouse (BigQuery)**
   - Streaming events loaded into BigQuery
   - Optimized for analytical queries

4. **Analytics & Visualization**
   - Dashboards built using Tableau and Looker Studio

---

## Tech Stack

- **Python** (ETL, data transformation)
- **Apache Kafka** (event streaming)
- **Docker & Docker Compose** (infrastructure orchestration)
- **Google BigQuery** (data warehouse)
- **Looker Studio** (data visualization)
- **Tableau** (data visualization)

---

## Project Structure

## Author

**Víctor Eduardo**  
B.Sc. in Mathematics | B.Sc. in Physics  
Data Engineering & Analytics
