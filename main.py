# Usage: streamlit run main.py

import argparse
import glob
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from tinydb import TinyDB, Query
from pyspark.sql import SparkSession
import streamlit as st  # local import keeps dependency optional

st.set_page_config(page_title="Student Lifestyle Dashboard", page_icon="🎓", layout="wide")

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def banner(title: str) -> str:
    return f"\n{title}\n" + "⁃" * len(title)

# ---------------------------------------------------------------------------
# Phase 1 – Relational Database (SQLite)
# ---------------------------------------------------------------------------

def phase1_sqlite(csv_path: Path, db_path: Path) -> pd.DataFrame:
    print(banner("Phase 1 – Relational DB (SQLite)"))

    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df):,} rows from {csv_path.name}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.executescript(
        """
        DROP TABLE IF EXISTS Daily_Activities;
        DROP TABLE IF EXISTS Grades;
        DROP TABLE IF EXISTS Students;
        DROP TABLE IF EXISTS Genders;
        """
    )

    cur.execute("""CREATE TABLE Genders (
                    gender_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                    gender_name TEXT UNIQUE)
                """)
    cur.execute("""CREATE TABLE Students (
                    student_id  INTEGER PRIMARY KEY,
                    gender_id   INTEGER,
                    stress_level TEXT,
                    FOREIGN KEY (gender_id) REFERENCES Genders(gender_id))""")
    cur.execute("""CREATE TABLE Grades (
                    grade_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                    student_id  INTEGER,
                    grade_value REAL,
                    FOREIGN KEY (student_id) REFERENCES Students(student_id))""")
    cur.execute("""CREATE TABLE Daily_Activities (
                    activity_id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    student_id               INTEGER,
                    study_hours              REAL,
                    extracurricular_hours    REAL,
                    sleep_hours              REAL,
                    social_hours             REAL,
                    physical_activity_hours  REAL,
                    FOREIGN KEY (student_id) REFERENCES Students(student_id))""")

    gender_df = df[["Gender"]].drop_duplicates().reset_index(drop=True)
    gender_df["gender_id"] = gender_df.index + 1
    df = df.merge(gender_df, on="Gender")
    gender_df.rename(columns={"Gender": "gender_name"}, inplace=True)
    gender_df.to_sql("Genders", conn, if_exists="append", index=False)

    students_df = df[["Student_ID", "gender_id", "Stress_Level"]]
    students_df.to_sql("Students", conn, if_exists="append", index=False)

    grades_df = df[["Student_ID", "Grades"]].rename(columns={"Grades": "grade_value"})
    grades_df.to_sql("Grades", conn, if_exists="append", index=False)

    activities_df = df[[
        "Student_ID",
        "Study_Hours_Per_Day",
        "Extracurricular_Hours_Per_Day",
        "Sleep_Hours_Per_Day",
        "Social_Hours_Per_Day",
        "Physical_Activity_Hours_Per_Day",
    ]].rename(columns={
        "Study_Hours_Per_Day": "study_hours",
        "Extracurricular_Hours_Per_Day": "extracurricular_hours",
        "Sleep_Hours_Per_Day": "sleep_hours",
        "Social_Hours_Per_Day": "social_hours",
        "Physical_Activity_Hours_Per_Day": "physical_activity_hours",
    })
    activities_df.to_sql("Daily_Activities", conn, if_exists="append", index=False)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_study_hours ON Daily_Activities(study_hours)")
    conn.commit()

    # ----------------------------- CRUD OPERATIONS -----------------------------
    print("\nCRUD Operations")
    print("-" * 40)

    # READ
    cur.execute("SELECT * FROM Students WHERE Student_ID = 1")
    print("The row 1 in student:")
    print(cur.fetchall())

    # UPDATE
    cur.execute("UPDATE Students SET Stress_Level = 'Medium' WHERE Student_ID = 1")
    conn.commit()
    cur.execute("SELECT * FROM Students WHERE Student_ID = 1")
    print("Updated row 1 in student:")
    print(cur.fetchall())

    # CREATE
    cur.execute("DELETE FROM Students WHERE Student_ID = 2001")
    conn.commit()
    cur.execute("INSERT INTO Students (Student_ID, Gender_ID, Stress_Level) VALUES (2001, 2, 'Medium')")
    conn.commit()
    cur.execute("SELECT * FROM Students WHERE Student_ID = 2001")
    print("Created row 2001 in student table:")
    print(cur.fetchall())

    # DELETE
    cur.execute("DELETE FROM Students WHERE Student_ID = 2001")
    conn.commit()
    cur.execute("SELECT * FROM Students WHERE Student_ID = 2001")
    print("Deleted row 2001 from the student:")
    print(cur.fetchall())

    # -------------------------- QUERY OPTIMIZATION ----------------------------
    import time

    print("\nQuery Optimization BY INDEXING")
    start = time.time()
    cur.execute("SELECT * FROM Daily_Activities WHERE Study_Hours > 5")
    print("Time without re-creating index:", time.time() - start)

    cur.execute("DROP INDEX IF EXISTS idx_study_hours")
    cur.execute("CREATE INDEX idx_study_hours ON Daily_Activities(Study_Hours)")
    conn.commit()

    start = time.time()
    cur.execute("SELECT * FROM Daily_Activities WHERE Study_Hours > 5")
    print("Time with index:               ", time.time() - start)

    print("\nQuery Optimization Using JOIN Queries")
    start = time.time()
    query = """
    SELECT s.Student_ID, d.Study_Hours, s.Stress_Level
    FROM Students s
    JOIN Daily_Activities d ON s.Student_ID = d.Student_ID
    WHERE d.Study_Hours > 5
    """
    df_join = pd.read_sql_query(query, conn)
    print(df_join)
    print("Time to process:", time.time() - start)

    print("\nQuery Optimization Using Aggregation GROUP BY")
    start = time.time()
    query = """
    SELECT
        s.Student_ID,
        g.Gender_Name,
        s.Stress_Level,
        SUM(d.Study_Hours) AS Total_Study_Hours,
        AVG(gr.Grade_Value) AS Average_Grade
    FROM
        Students s
    JOIN
        Daily_Activities d ON s.Student_ID = d.Student_ID
    JOIN
        Genders g ON s.Gender_ID = g.Gender_ID
    LEFT JOIN
        Grades gr ON s.Student_ID = gr.Student_ID
    GROUP BY
        s.Student_ID, g.Gender_Name, s.Stress_Level
    ORDER BY
        Total_Study_Hours DESC;
    """
    df_group = pd.read_sql_query(query, conn)
    print(df_group)
    print("Time to process:", time.time() - start)

    conn.close()
    print("✔️  SQLite populated and tested →", db_path.name)
    return df

# ---------------------------------------------------------------------------
# Phase 2 – NoSQL (TinyDB)
# ---------------------------------------------------------------------------

def phase2_tinydb(df: pd.DataFrame, tiny_path: Path) -> None:
    print(banner("Phase 2 – NoSQL (TinyDB)"))

    db = TinyDB(tiny_path)
    db.truncate()

    sample = df
    docs = [{
        "student_id": int(r.Student_ID),
        "study_hours_per_day": float(r.Study_Hours_Per_Day),
        "extracurricular_hours_per_day": float(r.Extracurricular_Hours_Per_Day),
        "sleep_hours_per_day": float(r.Sleep_Hours_Per_Day),
        "social_hours_per_day": float(r.Social_Hours_Per_Day),
        "physical_activity_hours_per_day": float(r.Physical_Activity_Hours_Per_Day),
        "stress_level": r.Stress_Level,
        "gender": r.Gender,
        "grades": float(r.Grades),
    } for r in sample.itertuples(index=False)]
    db.insert_multiple(docs)
    print("Inserted 100 student records!")

    # Students with GPA >= 7 for scholarships
    Student = Query()
    print("\nStudents eligible for scholarships (GPA >= 7):")
    scholarship_students = db.search(Student.grades >= 7)
    for student in scholarship_students:
        print(f"Student ID: {student['student_id']}, GPA: {student['grades']}")

    # Students with Moderate or High stress level for counseling
    print("\nStudents recommended for counseling (Moderate or High stress):")
    counseling_students = db.search(Student.stress_level.one_of(['Moderate', 'High']))
    for student in counseling_students:
        print(f"Student ID: {student['student_id']}, Stress Level: {student['stress_level']}")

    # Students with high physical activity (>= 5 hours/day) for sports scholarships
    print("\nStudents eligible for sports scholarships (Physical Activity >= 5 hours/day):")
    sports_students = db.search(Student.physical_activity_hours_per_day >= 5)
    for student in sports_students:
        print(f"Student ID: {student['student_id']}, Physical Activity: {student['physical_activity_hours_per_day']} hours")

    # Delete students with GPA <= 2
    db.remove(Student.grades <= 2)
    print("\nDeleted students with GPA <= 2!")

    # Step 10: Count Documents
    print("\nTotal number of students:", len(db))

# ---------------------------------------------------------------------------
# Phase 3 – Stream Processing (PySpark)
# ---------------------------------------------------------------------------

def phase3_spark(clean_csv: Path, out_dir: Path) -> list[Path]:
    print(banner("Phase 3 – Stream Processing (PySpark)"))

    spark = SparkSession.builder.appName("StudentsLifeStyle").master("local[*]").getOrCreate()
    sdf = spark.read.csv(str(clean_csv), header=True, inferSchema=True)

    sdf = sdf.withColumnRenamed("Grades", "grades")
    filtered = sdf.filter(sdf["grades"] >= 9.75)
    selected = sdf.select("student_id", "grades")
    sorted_df = sdf.orderBy(sdf["grades"].desc())

    dirs = [
        out_dir / "output_students",
        out_dir / "output_students2",
        out_dir / "output_students3",
    ]
    filtered.write.mode("overwrite").option("header", "true").csv(str(dirs[0]))
    selected.write.mode("overwrite").option("header", "true").csv(str(dirs[1]))
    sorted_df.write.mode("overwrite").option("header", "true").csv(str(dirs[2]))

    spark.stop()
    print("Spark outputs →", ", ".join(d.name for d in dirs))
    return dirs

# ---------------------------------------------------------------------------
# Phase 4 – Integration & Reporting
# ---------------------------------------------------------------------------

def phase4_integration(db_path: Path, tiny_path: Path, spark_dirs: list[Path], out_csv: Path) -> pd.DataFrame:
    print(banner("Phase 4 – Integration & Reporting"))

    # Read phase 1 data
    with sqlite3.connect(db_path) as conn:
        students = pd.read_sql("SELECT * FROM Students", conn)
        grades   = pd.read_sql("SELECT * FROM Grades", conn)
        acts     = pd.read_sql("SELECT * FROM Daily_Activities", conn)

    # Read phase 2 data
    nosql_df = pd.DataFrame(TinyDB(tiny_path).all())

    # Read phase 3 data
    spark_frames: dict[str, pd.DataFrame] = {}
    for d in spark_dirs:
        files = sorted(glob.glob(str(d / "*.csv")))
        if files:
            spark_frames[d.name] = pd.read_csv(files[0])

    relational = (
        students
        .merge(grades, on="student_id", how="left")
        .merge(acts,   on="student_id", how="left")
    )
    relational.columns = map(str.lower, relational.columns)
    nosql_df["student_id"] = nosql_df["student_id"].astype(int)

    combined = relational.merge(nosql_df, on="student_id", how="outer", suffixes=("", "_nosql"))

    if "output_students" in spark_frames:
        perf = spark_frames["output_students"][["student_id"]].astype(int)
        perf["top_performer"] = True
        combined = combined.merge(perf, on="student_id", how="left")
        combined["top_performer"].fillna(False, inplace=True)

    combined.to_csv(out_csv, index=False)
    print(f"Combined CSV saved → {out_csv.name} ({len(combined):,} rows)")

    print(banner("Average Grade by Stress Level"))
    if "grade_value" in combined.columns:
        print(combined.groupby("stress_level")["grade_value"].mean().round(2))

    return combined

# ---------------------------------------------------------------------------
# Dashboard (Streamlit)
# ---------------------------------------------------------------------------

def launch_dashboard(db_path: Path, tiny_path: Path, spark_dirs: list[Path], combined_csv: Path) -> None:

    # ---- Phase 1 (SQLite)
    conn = sqlite3.connect(db_path)
    students_df = pd.read_sql_query("SELECT * FROM Students", conn)
    conn.close()

    # ---- Phase 2 (TinyDB)
    records = TinyDB(tiny_path).all()
    tiny_df = pd.DataFrame(records)

    # ---- Phase 3 (Spark outputs)
    streamed = []
    for d in spark_dirs:
        for f in sorted(Path(d).glob("*.csv")):
            streamed.append(pd.read_csv(f))
    stream_df = pd.concat(streamed, ignore_index=True) if streamed else pd.DataFrame()
    
    # ---- Phase 4 (Combined CSV)
    combined = pd.read_csv(combined_csv)

    # ---- UI ------------------------------------------------------------
    st.title("🎓 Student Data Dashboard")

    with st.expander("ℹ️ Instructions", expanded=False):
        st.markdown("This dashboard illustrates the outputs of all four pipeline phases …")

    st.subheader("📋 Phase 1: Students (Relational DB)")
    st.dataframe(students_df, use_container_width=True)

    st.subheader("🗃️ Phase 2: Lifestyle Documents (TinyDB)")
    st.dataframe(tiny_df, use_container_width=True)

    st.subheader("📡 Phase 3: Streamed Grades (Spark CSVs)")
    if not stream_df.empty:
        st.dataframe(stream_df, use_container_width=True)
    else:
        st.info("No stream data found – run Phase 3 first.")

    st.subheader("🌸 Phase 4: Combined Dataset")
    st.dataframe(combined, use_container_width=True)

    st.subheader("📊 Aggregate Insights")
    if "grades" in tiny_df.columns:
        st.bar_chart(tiny_df.groupby("stress_level")["grades"].mean())

    st.success("Dashboard ready!  Reload after rerunning the pipeline to refresh data.")

# ---------------------------------------------------------------------------
# Pipeline driver
# ---------------------------------------------------------------------------

def main() -> None:
    data_dir = Path.cwd()
    csv_path = Path("student_lifestyle_dataset.csv").expanduser().resolve()
    if not csv_path.exists():
        sys.exit(f"❌ CSV not found: {csv_path}")

    db_path = Path("student_lifestyle.db").expanduser().resolve()
    tiny_path = data_dir / "students.json"

    # Phase 1: SQLite
    df = phase1_sqlite(csv_path, db_path)

    # Phase 2: TinyDB
    phase2_tinydb(df, tiny_path)

    # Phase 3: PySpark
    cleaned_csv = data_dir / "student_lifestyle_cleaned.csv"
    clean_df = df.copy()
    clean_df.columns = [c.lower() for c in clean_df.columns]
    clean_df.to_csv(cleaned_csv, index=False)
    spark_dirs = phase3_spark(cleaned_csv, data_dir)

    # Phase 4: Integration & Reporting
    combined_csv = data_dir / "combined_dataset.csv"
    combined = phase4_integration(db_path, tiny_path, spark_dirs, combined_csv)

    print("🚀 Pipeline complete!")

    # Launch dashboard if requested
    launch_dashboard(db_path, tiny_path, spark_dirs, combined_csv)


if __name__ == "__main__":
    main()
