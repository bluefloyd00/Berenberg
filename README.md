# Berenber tech test
This project contains a Python application with the tech test for Beremberg. The application's functionality is defined in pipeline.py, and the Behave tests are located in the features directory.

## Getting Started
These instructions will cover how to get your application up and running on your local machine for development and testing purposes.

### Prerequisites
Before you begin, ensure you have Docker installed on your system.

### Running the Application

To run the application inside a Docker container, follow these steps:

1. **Clone the Repository:**
   If you haven't already cloned the repository, do so with:
   ```bash
   git clone https://github.com/bluefloyd00/Berenberg.git
   cd Beremberg
   ```


2. **Build the application:**
   This command builds a Docker application 
   ```bash
   docker build -t berember-tech-test .
   ```

3. **Run the application:**
   This command builds a Docker application and mounts a volume
   ```bash
   docker run -v /absolute/path/to/project/data:/app/data berember-tech-test
   ```


## Additional Information
- Data Output: After performing the calculations, the application writes the resulting DataFrame to a file named "result.parquet". This file is saved locally in the mounted data folder. When running the Docker container, this folder corresponds to a specified directory on your host machine, allowing easy access to the output file.

- Logging Execution Time: The application logs the execution time for each step of the pipeline. These logs provide insights into the time taken for individual operations, aiding in performance analysis.

- Total Execution Time: The log entry labeled "Execution time of main" represents the total execution time of the pipeline. This metric includes the cumulative time taken for all steps from data loading to the final calculation.

- Limited Testing: Due to time constraints, only a few tests have been implemented. 

- Efficient Data Manipulation: The application utilizes Pandas, a powerful data manipulation and analysis library for Python. By leveraging Pandas' vectorized operations, the code is designed for high efficiency, particularly in handling large datasets. This approach significantly improves performance compared to traditional loop-based data processing.

## Assumptions

- In the executions dataset, the trading_date is expressed in milliseconds.
- In the marketdata dataset, the event_timestamp is expressed in nanoseconds.

In cases where multiple marketdata transactions occur within the same millisecond for a given execution, we will consider only the latest transaction.
