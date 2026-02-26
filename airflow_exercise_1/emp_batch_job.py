from pyspark.sql import SparkSession

def process_data():
    spark = SparkSession.builder.appName("GCPDataprocJob").getOrCreate()
    
    # Define your GCS bucket and paths
    bucket = "test-demo-ododah"
    employee_path = f"gs://{bucket}/airflow-project-1/data/employee.csv"
    department_path = f"gs://{bucket}/airflow-project-1/data/department.csv"
    output_path = f"gs://{bucket}/airflow-project-1/output"
    
    # Read datasets 
    employee = spark.read.csv(employee_path, header=True, inferSchema=True)
    department = spark.read.csv(department_path, header=True, inferSchema=True)
    
    # Filter employee data 
    filtered_employee = employee.filter(employee.salary > 55000)
    
    # Join datasets
    joined_data = filtered_employee.join(department, "dept_id", "inner")
    
    # Write output 
    joined_data.write.csv(output_path, header=True, mode="overwrite")
    
    spark.stop()
    

if __name__ == "__main__":
    process_data()