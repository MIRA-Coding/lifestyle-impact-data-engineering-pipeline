# lifestyle-impact-data-engineering-pipeline
using: https://www.kaggle.com/datasets/charlottebennett1234/lifestyle-factors-and-their-impact-on-students
## Project Overview

The **Student Lifestyle pipeline** is a comprehensive data processing and visualization pipeline designed to analyze and present student-related data. It focuses on key aspects such as academic performance, daily activities, and stress levels to provide actionable insights. The system is implemented in Python and leverages various database and processing technologies across four distinct phases, culminating in an interactive Streamlit front-end dashboard.

**University:** Umm Al-Qura University

## Team Members

* Amirah Mohsen Al-Ghamwy 
* Amjad Ahmed Hawsaoui 
* Noura Mohammed Al-Kabkabi 
* Wi'am Rashed 

## Design and Implementation Phases

The pipeline is structured into four phases, each utilizing different technologies to efficiently handle data:

### Phase 1: Relational Database (SQLite)

This phase focuses on loading and managing the `student_lifestyle_dataset.csv` into a SQLite database.

* **Data Loading & Normalization:** The CSV data is read into a Pandas DataFrame and then normalized into four distinct tables within SQLite: `Genders`, `Students`, `Grades`, and `Daily_Activities`.
* **Schema Design:** Tables are created with appropriate primary and foreign key constraints to ensure data integrity and a well-structured relational model.
* **CRUD Operations:** Basic Create, Read, Update, and Delete (CRUD) operations are implemented and tested to demonstrate database interaction capabilities.
* **Query Optimization:**
    * **Indexing:** Indexes are created (e.g., on `study_hours`) to significantly enhance query performance.
    * **JOIN Queries:** Optimized JOIN queries are used to retrieve combined data efficiently.
    * **Aggregation (GROUP BY):** Aggregation queries with `GROUP BY` are implemented to summarize data, such as calculating total study hours and average grades per student.

### Phase 2: NoSQL Database (TinyDB)

This phase demonstrates the use of a lightweight NoSQL database, TinyDB, for flexible, document-based storage.

* **Data Insertion:** The dataset is transformed into JSON-like documents, each representing a student's comprehensive profile (including grades, stress levels, and daily activities), and inserted into TinyDB.
* **Queries:**
    * Identifies students eligible for scholarships (GPA $\ge 7$).
    * Recommends students for counseling based on moderate or high stress levels.
    * Identifies students eligible for sports scholarships (physical activity $\ge 5$ hours/day).
    * Includes functionality to remove low-performing students (GPA $\le 3$).
* **Advantages:** TinyDB's simplicity allows for rapid prototyping and flexible querying, making it suitable for small-scale, dynamic datasets.

### Phase 3: Stream Processing (PySpark)

This phase leverages PySpark for distributed data processing, simulating a streaming pipeline for larger datasets.

* **Data Processing:** The dataset is processed to:
    * Filter high-performing students (grades $\ge 9.75$).
    * Select specific columns (`student_id`, `grades`).
    * Sort students by grades in descending order.
* **Output:** Processed results are saved as CSV files in separate output directories (`output_students`, `output_students2`, `output_students3`).
* **Scalability:** PySpark ensures the pipeline's ability to handle larger datasets in a distributed environment, with local execution for development and testing.

### Phase 4: Integration and Reporting

The final phase consolidates and visualizes the processed data from all previous phases.

* **Data Integration:** Outputs from SQLite, TinyDB, and PySpark are merged into a single comprehensive Pandas DataFrame. This involves handling schema differences and ensuring data consistency across disparate sources.
* **Output:** A combined CSV file (`combined_dataset.csv`) is generated, which includes a `top_performer` flag for high-achieving students.
* **Reporting & Visualization:**
    * Aggregate insights, such as average grades grouped by stress level, are computed.
    * A Streamlit dashboard is used as the front-end to present these insights interactively, providing a user-friendly interface for data exploration.

## How to Run

To run this project locally, follow these steps:

1.  **Prerequisites:**
    * Python 3.x
    * Anaconda (recommended for environment management)
    * `student_lifestyle_dataset.csv` file (ensure it's in the same directory as `main.py` or provide the correct path).

2.  **Setup Environment (using Anaconda):**
    ```bash
    conda create -n student_dashboard python=3.9
    conda activate student_dashboard
    pip install pandas tinydb pyspark streamlit
    ```

3.  **Run the Data Engineering Pipeline:**
    Execute the `main.py` script to process the data through all phases and generate the necessary database files and Spark outputs.
    ```bash
    python main.py
    ```
    This script will:
    * Populate the SQLite database (`student_lifestyle.db`).
    * Create the TinyDB database (`students.json`).
    * Generate PySpark output CSVs in `output_students`, `output_students2`, and `output_students3` directories.

4.  **Launch the Streamlit Dashboard:**
    After the pipeline has run successfully, launch the Streamlit application to view the dashboard:
    ```bash
    streamlit run main.py
    ```
    This will open the dashboard in your web browser, displaying the integrated insights and visualizations.

## Project Structure

├── main.py                             # It for the Streamlit dashboard (it use the generated dataset from ipynb file)
├── DataEngineering final project.ipynb # The file contain the process of the first 3 phases
├── student_lifestyle_dataset.csv       # Input dataset **Original**
└── README.md                       # This README file
