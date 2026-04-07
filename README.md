# 📊 BA-COE KPI Automation Suite

## 📝 Overview
The **BA-COE KPI Automation Suite** is a centralized Python framework designed to streamline the extraction, processing, and reporting of Key Performance Indicators (KPIs). Instead of manual data pulls, this tool automates the connection to data sources, applies business logic, and generates timestamped audit-ready reports.

---

## ✨ Key Features
* **Interactive CLI Menu**: A user-friendly command-line interface for selecting specific KPI sets.
* **Persistent Connection Management**: Optimized database/API handling via `reusable_connection.py`.
* **Automated Logging**: Full execution traceability using a custom `logger.py`.
* **Dynamic Exporting**: Automated CSV generation with standardized naming conventions.

---

## 🏗 Technical Architecture

| File | Responsibility |
| :--- | :--- |
| **`main.py`** | The "Orchestrator." It initializes the logger and triggers the CLI menu. |
| **`cli_menu.py`** | Handles user inputs and navigation logic. |
| **`fetch_KPIs.py`** | Contains the logic/queries required to pull raw data. |
| **`reusable_connection.py`** | Manages the lifecycle of the data source connection. |
| **`export_helper.py`** | Processes raw data into the final CSV format. |
| **`config.py`** | Stores credentials and environment-specific variables. |
| **`logger.py`** | Standardizes error and activity tracking. |

---

## 🚀 How to Run the Project

### 1. Environment Setup
Ensure you have Python installed. It is recommended to use a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Mac/Linux
.\venv\Scripts\activate   # Windows