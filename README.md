his repository hosts the data, code, and notebooks developed as part of the HealthRadar Capstone Project. The project aims to build an early warning system for healthcare inequalities across England, with a focus on detecting underserved areas in primary care (GP practices).

📌 Project Overview

Access to General Practitioners (GPs) remains highly uneven across England, especially in areas of socioeconomic deprivation. Traditional planning approaches rely on static datasets such as the Index of Multiple Deprivation (IMD), which do not capture real-time changes in population, demand, or workforce supply.

HealthRadar addresses this gap by:

Combining multi-source datasets: GP patient registers, GP workforce data, ONS population data, income & health deprivation scores, and QOF disease prevalence (2023/24).

Harmonising data at fine geographic levels (LSOA, MSOA, ICB).

Applying machine learning and statistical models to:

Forecast patient demand.

Classify underserved communities.

Cluster persistent deprivation hotspots.

Delivering outputs as maps, metrics, and dashboards to support NHS England planners, public health analysts, and policymakers.

📊 Data Sources

GP Patient Registers (Sept 2023 – Aug 2025) – NHS Digital

GP Workforce Data (2025 snapshot) – NHS England

ONS Population & Demographics (2022) – Office for National Statistics

Income & Health Deprivation Scores (IMD 2019) – MHCLG

QOF Disease Registers (2023/24) – NHS England

ONS Postcode ↔ Region Mapping & Shapefiles – ONS Geography

All data has been cleaned, harmonised, and normalised for consistent analysis.
📌 Raw and processed datasets (CSV) are provided in the processed/ folder.
