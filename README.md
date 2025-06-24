# ETL_Midterm_Calvin_035
A short project to demonstrate mastery of the ETL process.

---

## 1. Project Overview

This project demonstrates a complete **ETL (Extract, Transform, Load)** pipeline for sales order data. 
The primary goal is to gather raw sales data from various sources (CSV files), cleanse and enrich it, and then load the refined data into an efficient storage format (Parquet files) ready for further analysis and reporting. 

The pipeline involves:

- Extracting raw and incremental datasets from CSV files.
- Cleaning, enriching, filtering, and structuring data.
- Categorizing and transforming categorical variables.
- Saving the transformed data into **Parquet** format for efficient storage and querying.

The final output includes two optimized datasets:  
- `full_data.parquet` – based on historical full data  
- `incremental_data.parquet` – based on newly added data

---

##  2. ETL Phases

### A) Extraction

Located in: `etl_extraction.ipynb` file

- Loaded `raw_data.csv` and `incremental_data.csv` using **Pandas**.
- Inspected the data using `.info()`, `.head()`, and `.isnull()`.
- Identified and reported missing values, duplicates, and unusual placeholders like `'?'`, `'N/A'`, or `'None'`.

### B) Transformation

Located in: `etl_transformation.ipynb` file

#### For `raw_data`:
- **Cleaning:**
  - Filled missing `customer_name` with `'Unknown'`.
  - Used `median` for missing `quantity` and `unit_price`.
  - Forward-filled missing `order_date`.
  - Filled missing `region` with `'Unknown'`.
  - Removed duplicate rows.
- **Enrichment:** Added `total_spend = quantity * unit_price`.
- **Structuring:** Converted `order_date` to `datetime`.
- **Categorization:** Added `customer_tier` based on spending:
  - `Bronze` <= 100  
  - `Silver` <= 500  
  - `Gold` <= 1000  
  - `Platinum` > 1000

#### For `incremental_data`:
- Applied similar cleaning rules.
- Dropped `customer_name` (anonymous analysis).
- Filtered out rows with region `'Unknown'`.
- Used **one-hot encoding** for `product` and `region`.
- Added `total_spend`.

### C) Loading

Located in: `etl_loading.ipynb` file

- Transformed datasets were saved in efficient **Parquet** format using `pandas.to_parquet()`.
- Saved into a structured directory: `/loaded/`
  - `loaded/full_data.parquet`
  - `loaded/incremental_data.parquet`
- Previewed data using:
  ```python
  pd.read_parquet("loaded/full_data.parquet").head()
  pd.read_parquet("loaded/incremental_data.parquet").head()
    ```

---

## 3. Tools Used

| Tool/Library                   | Purpose                              |
| ------------------------------ | ------------------------------------ |
| **Python 3.13+**               | Core programming language            |
| **Pandas**                     | Data manipulation & analysis         |
| **NumPy**                      | Numerical operations                 |
| **PyArrow**                    | Parquet file support in Pandas       |
| **Jupyter Notebook / VS Code** | Development environment              |
| **Parquet**                    | Efficient storage and querying       |


---



## 4. How to Run the Project
Follow these steps to set up and run the ETL project:

Clone the Repository (if applicable):

```bash
git clone https://github.com/Campeon254/ETL_Midterm_Calvin_035
cd ETL_Midterm_Calvin_035
```

Make sure you have Python installed. Then, install the required libraries:

```bash
pip install pandas pyarrow numpy
```

**Prepare Data Files:**

Place your raw data CSV files in the specified paths. Based on the provided code, you'll need:

raw_data.csv in `C:\Users\admin\Downloads\` (or update the path in the script to where your raw_data.csv is located).

incremental_data.csv in `C:\Users\admin\Downloads\` (or update the path in the script to where your incremental_data.csv is located).

The project will create the following directories and files:

`ETL_Midterm_Calvin_035\Data\` (will contain copies of raw/incremental data after initial load)

`ETL_Midterm_Calvin_035\Transformed\` (will contain transformed_full.csv and transformed_incremental.csv)

`ETL_Midterm_Calvin_035\loaded\` (will contain full_data.parquet and incremental_data.parquet)

**Execute the ETL Script:**

If the provided code snippets are combined into a single Python file (e.g., etl_pipeline.ipynb), you can run it from your terminal:

```bash
jupyter nbconvert --to script etl_pipeline.ipynb
```

Alternatively, if you are using a Jupyter Notebook or similar interactive environment, execute the cells sequentially as demonstrated in the project description.

The script will perform the extraction, transformation, and loading steps, printing intermediate outputs and saving the processed files to their respective directories.

---

## 📁 Project Structure

```
ETL_Midterm_Calvin_035/
├── Data/
│   ├── raw_data.csv
│   └── incremental_data.csv
├── Transformed/
│   ├── transformed_full.csv
│   └── transformed_incremental.csv
├── loaded/
│   ├── full_data.parquet
│   └── incremental_data.parquet
├── etl_extraction.py
├── etl_transformation.py
├── etl_loading.py
└── README.md
```

---