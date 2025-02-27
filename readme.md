# DriveWealth Open Library Data Pipeline

## 1. Overview & Subject Rationale

**Objective:**  
Develop a mini data pipeline that extracts data from the Open Library API for a user-specified subject, transforms the raw JSON into clean, structured datasets for authors and books, and outputs CSV files as a mock persistent storage solution. This solution is designed with production-readiness in mind, featuring robust error handling, structured JSON logging, caching, comprehensive testing, and scalability considerations.

**Subject Selection & Dynamic Configuration:**  
- **Default Subject:**  
  The default subject is set to `"science_fiction"` in `config.yaml`.  
- **Override Capability:**  
  This can be overridden at runtime via command-line parameters (using Python’s `argparse`).
- **Rationale:**  
  The `"science_fiction"` subject provides a rich and diverse dataset—with multiple authors, varied publication years, and numerous works—making it ideal for demonstrating robust extraction, transformation, and error handling.

---

## 2. Architecture Overview

The pipeline is divided into four main modules:

- **Extraction (extract.py):**  
  Retrieves raw JSON data from the Open Library API using HTTP GET requests with retries (via Tenacity) and caching (via Cachetools). It validates that the response contains the required `"works"` key and logs all key events in JSON format for CloudWatch compatibility.

- **Transformation (transform.py):**  
  Processes the raw JSON data to extract and normalize details for both authors and books. It converts data types (e.g., publication year to integer), logs warnings for missing fields, collects error records (persisted to `error_records.csv`), and maintains a schema version constant.

- **Loading (load.py):**  
  Writes the transformed data to CSV files (`authors.csv` and `books.csv`), which serve as a mock database. In production, these CSVs would be replaced with a persistent database (e.g., PostgreSQL on AWS RDS).

- **Aggregation (aggregate.py):**  
  Uses Pandas to group and aggregate the book data, computing metrics such as the count of books per author per publication year and the average number of books per author per year.

Additionally, a detailed architecture diagram is provided in `diagram.drawio` (or `diagram.pdf`), visually outlining the data flow and AWS integration.

---

## 3. Configuration Options

All configuration settings are centralized in `config.yaml`. Key settings include:

- **API Configuration:**
  - `subject`: Default subject ("science_fiction").
  - `base_url`: `"https://openlibrary.org/subjects"`.
  - `endpoint_format`: `"{base_url}/{subject}.json"` – a template to dynamically construct the API endpoint.
  
- **AWS Configuration:**
  - `s3_bucket`: `"drivewealth-openlibrary-data"` – the S3 bucket used for storing CSV outputs.
  - `region`: `"us-east-1"`.
  
- **Schema Version:**
  - `schema_version`: `1.0` – used by the transformation module to manage schema updates.

**Sensitive Data Management:**  
Sensitive information (e.g., API keys) should be managed using environment variables or a secrets manager rather than being stored in this file.

*Sample config.yaml:*
```yaml
api:
  subject: "science_fiction"
  base_url: "https://openlibrary.org/subjects"
  endpoint_format: "{base_url}/{subject}.json"

aws:
  s3_bucket: "drivewealth-openlibrary-data"
  region: "us-east-1"

schema_version: 1.0
```
4. Logging and Monitoring
	•	Structured JSON Logging:
All modules emit log messages in JSON format, ensuring compatibility with AWS CloudWatch. Key events such as pipeline start, API call attempts, retries, and phase completions are logged.
	•	Runtime Metrics:
The pipeline measures the duration of extraction, transformation, and loading phases, logging these performance metrics to help identify bottlenecks.
	•	Monitoring:
In production, these logs and metrics would be forwarded to CloudWatch for real-time monitoring and alerting.

5. Mock Database vs. Production Database
	•	Current Approach:
The pipeline writes CSV files (authors.csv and books.csv) as a mock database.
	•	Future Migration:
In a production environment, these CSV outputs would be replaced by a robust database solution (e.g., PostgreSQL on AWS RDS) with proper partitioning and indexing to handle larger datasets and provide better query performance.

6. Continuous Integration

A CI/CD pipeline (using GitHub Actions) is set up to run automated tests and generate code coverage reports. Example configuration:

name: Python CI
on: [push, pull_request]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.8'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests with coverage
        run: |
          pytest --maxfail=1 --disable-warnings -q
          coverage run -m pytest
          coverage report
          # Optionally, upload coverage reports as artifacts

7. Production Enhancements
	•	AWS Integration:
Designed for deployment on AWS, with extraction and transformation functions suitable for AWS Lambda. Output CSVs are stored in an S3 bucket provisioned via Terraform.
	•	Caching:
API responses are cached to reduce redundant calls and improve performance.
	•	Lifecycle Management:
The S3 bucket is configured (via Terraform in main.tf) with versioning, server-side encryption, and lifecycle rules to manage costs and data retention.
	•	Monitoring & Metrics:
Runtime performance metrics are logged, and additional AWS monitoring tools (such as CloudWatch) are recommended.

8. Local Development Setup

To set up your local development environment:
	•	Environment Setup:
Create a virtual environment using virtualenv or conda:

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate


	•	Install Dependencies:
Run pip install -r requirements.txt.
	•	Run Tests:
Execute pytest to run the unit and integration tests.
	•	Run Pipeline Locally:
Use python pipeline.py (optionally with --subject <subject> to override the default).

9. Future Enhancements Roadmap

Potential future improvements include:
	•	Database Migration:
Transition from CSV outputs to a production-grade database (e.g., PostgreSQL on AWS RDS) with proper partitioning, indexing, and query optimization.
	•	Distributed Processing:
For very large datasets, consider using distributed processing frameworks (such as Dask or Apache Spark) to improve performance.
	•	Enhanced Monitoring:
Integrate AWS CloudWatch metrics and AWS Config for comprehensive monitoring and compliance.
	•	Security Enhancements:
Implement more granular IAM policies, VPC endpoints for S3, and regular security audits.
	•	Automated Error Review:
Set up automated alerts for error record review and integrate these with incident management tools.

10. Presentation and Communication of Findings

A separate presentation is provided in presentation.pdf. This slide deck includes:
	•	A summary of the pipeline’s objectives and key architectural decisions.
	•	Visualizations (e.g., graphs generated via matplotlib/seaborn in a Jupyter Notebook) that illustrate:
	•	The number of books per author per publication year.
	•	The average number of books per author per year.
	•	Details on production enhancements, performance metrics, and future migration plans.
	•	Proposed next steps for scaling and enhanced monitoring.

This presentation is designed to effectively communicate insights to product and analytics teams.

11. Security Considerations
	•	IAM Policies & Encryption:
The S3 bucket is configured with server-side encryption (AES256) and strict public access settings. In production, additional measures (e.g., VPC endpoints) would be implemented.
	•	Sensitive Data Management:
Sensitive configuration details, such as API keys, should be managed using environment variables or a secrets manager.

12. Testing Strategy

Testing is comprehensive and includes:
	•	Unit Tests:
Each module (extraction, transformation, loading, aggregation) has unit tests covering normal and edge cases.
	•	Integration Tests:
End-to-end tests run the entire pipeline and verify that output files are correctly generated.
	•	CI/CD Integration:
Automated tests and code coverage reports are generated via GitHub Actions.
	•	Fixtures and Mocks:
Pytest fixtures and mocking are used to isolate external dependencies and ensure deterministic test outcomes.

13. Infrastructure as Code

AWS resources are provisioned using Terraform. The main.tf file provisions an S3 bucket with:
	•	Versioning enabled.
	•	Server-side encryption (AES256).
	•	Lifecycle rules to manage storage costs.
	•	Explicit public access block configurations.

Refer to the main.tf file for complete details.

14. File Structure

The project consists of the following files and directories:
	1.	config.yaml – Centralized configuration for API, AWS, and schema settings.
	2.	requirements.txt – Lists all Python dependencies.
	3.	pipeline.py – Main orchestrator of the pipeline.
	4.	extract.py – Module for extracting data from the Open Library API.
	5.	transform.py – Module for transforming raw JSON into structured DataFrames.
	6.	load.py – Module for loading DataFrames to CSV files.
	7.	aggregate.py – Module for aggregating book data to produce insights.
	8.	tests/ – Directory containing unit and integration tests:
	•	tests/test_extract.py
	•	tests/test_transform.py
	•	tests/test_load.py
	•	tests/test_aggregate.py
	•	tests/test_pipeline.py
	9.	main.tf – Terraform configuration for provisioning the S3 bucket.
	10.	work.ipynb – notebook summarizing the project’s design, performance, and production enhancements.

15. Final Notes

This README provides a comprehensive overview of the DriveWealth Open Library Data Pipeline project. It details the system architecture, configuration, production enhancements, testing strategy, security considerations, and future roadmap. All information is intended to guide developers, testers, and reviewers through the project and demonstrate a production-ready solution with forward-thinking design.

For any questions or further clarifications, please refer to the inline comments in the source modules or contact the development team.