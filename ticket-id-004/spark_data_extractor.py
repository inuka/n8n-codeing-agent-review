from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkDataExtractor:
    def __init__(self, app_name="DataExtractor", master="local[*]"):
        """Initialize Spark session"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Set log level to reduce verbose output
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized")

    def create_sample_data(self):
        """Create sample data for demonstration"""
        sample_data = [
            (1, "John Doe", 28, "Engineering", 75000),
            (2, "Jane Smith", 32, "Marketing", 65000),
            (3, "Mike Johnson", 45, "Engineering", 95000),
            (4, "Sarah Wilson", 29, "Sales", 55000),
            (5, "David Brown", 38, "Engineering", 85000),
            (6, "Lisa Davis", 26, "Marketing", 60000),
            (7, "Tom Miller", 41, "Sales", 70000),
            (8, "Amy Garcia", 33, "Engineering", 80000),
            (9, "Chris Lee", 31, "Marketing", 62000),
            (10, "Maria Rodriguez", 27, "Sales", 58000)
        ]
        
        columns = ["id", "name", "age", "department", "salary"]
        df = self.spark.createDataFrame(sample_data, columns)
        
        # Cache the DataFrame for better performance
        df.cache()
        logger.info("Sample data created and cached")
        return df

    def extract_basic_stats(self, df):
        """Extract basic statistics from the DataFrame"""
        logger.info("Extracting basic statistics...")
        
        # Basic info
        total_records = df.count()
        logger.info(f"Total records: {total_records}")
        
        # Show schema
        print("\n=== DataFrame Schema ===")
        df.printSchema()
        
        # Show sample data
        print("\n=== Sample Data (First 5 rows) ===")
        df.show(5)
        
        # Basic statistics
        print("\n=== Basic Statistics ===")
        df.describe().show()
        
        return total_records

    def extract_department_analysis(self, df):
        """Extract department-wise analysis"""
        logger.info("Performing department analysis...")
        
        print("\n=== Department Analysis ===")
        dept_stats = df.groupBy("department") \
            .agg(
                count("*").alias("employee_count"),
                avg("salary").alias("avg_salary"),
                min("salary").alias("min_salary"),
                max("salary").alias("max_salary"),
                avg("age").alias("avg_age")
            ) \
            .orderBy("employee_count", ascending=False)
        
        dept_stats.show()
        return dept_stats

    def extract_filtered_data(self, df):
        """Extract filtered data based on conditions"""
        logger.info("Extracting filtered data...")
        
        # High earners (salary > 70000)
        print("\n=== High Earners (Salary > $70,000) ===")
        high_earners = df.filter(col("salary") > 70000) \
                        .select("name", "department", "salary") \
                        .orderBy("salary", ascending=False)
        high_earners.show()
        
        # Young professionals (age < 30)
        print("\n=== Young Professionals (Age < 30) ===")
        young_professionals = df.filter(col("age") < 30) \
                               .select("name", "age", "department", "salary")
        young_professionals.show()
        
        return high_earners, young_professionals

    def save_data_sample(self, df, output_path="output"):
        """Save sample data to different formats"""
        logger.info(f"Saving data to {output_path}...")
        
        try:
            # Save as Parquet (recommended for Spark)
            df.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/employees.parquet")
            logger.info("Data saved as Parquet")
            
            # Save as CSV
            df.coalesce(1).write.mode("overwrite") \
              .option("header", "true") \
              .csv(f"{output_path}/employees.csv")
            logger.info("Data saved as CSV")
            
            # Save as JSON
            df.coalesce(1).write.mode("overwrite").json(f"{output_path}/employees.json")
            logger.info("Data saved as JSON")
            
        except Exception as e:
            logger.error(f"Error saving data: {e}")

    def extract_data_from_source(self, source_type="parquet", source_path=None):
        """Extract data from external sources"""
        logger.info(f"Extracting data from {source_type} source...")
        
        try:
            if source_type == "parquet" and source_path:
                df = self.spark.read.parquet(source_path)
            elif source_type == "csv" and source_path:
                df = self.spark.read.option("header", "true").csv(source_path)
            elif source_type == "json" and source_path:
                df = self.spark.read.json(source_path)
            else:
                logger.warning("No valid source provided, using sample data")
                df = self.create_sample_data()
            
            return df
        except Exception as e:
            logger.error(f"Error reading from source: {e}")
            logger.info("Falling back to sample data")
            return self.create_sample_data()

    def run_extraction_pipeline(self, source_path=None):
        """Run complete data extraction pipeline"""
        logger.info("Starting data extraction pipeline...")
        
        try:
            # Extract or create data
            if source_path:
                df = self.extract_data_from_source("parquet", source_path)
            else:
                df = self.create_sample_data()
            
            # Perform analysis
            self.extract_basic_stats(df)
            self.extract_department_analysis(df)
            self.extract_filtered_data(df)
            
            # Save sample output
            self.save_data_sample(df)
            
            logger.info("Data extraction pipeline completed successfully")
            return df
            
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            raise

    def close(self):
        """Close Spark session"""
        self.spark.stop()
        logger.info("Spark session closed")

def main():
    """Main function to run the data extractor"""
    extractor = None
    
    try:
        # Initialize extractor
        extractor = SparkDataExtractor(app_name="SampleDataExtractor")
        
        # Run extraction pipeline
        df = extractor.run_extraction_pipeline()
        
        # Optional: Show final summary
        print("\n=== Extraction Summary ===")
        print(f"Total records processed: {df.count()}")
        print(f"Columns: {len(df.columns)}")
        print(f"Column names: {df.columns}")
        
    except Exception as e:
        logger.error(f"Main execution error: {e}")
    
    finally:
        if extractor:
            extractor.close()

if __name__ == "__main__":
    main()
