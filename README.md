# Cloud Ecosystem - Data Processing

## A data-driven initiative to processing data
Cloud Ecosystem - Data Processing is a framework for creating PySpark codes capable of handling
data from different sources in a unified way.

- Initially developed special for Google Cloud Platform. 
- Read/Write data from: Google Cloud Storage, Google Cloud PubSub, and Google Cloud BigQuery.
- In development for other cloud environments.

## Features
- Capable of handling data in batch, stream and analytical format.
- Allows data engineers to focus on delivering data.
- Able to support business logic for different use cases.
- Highly configurable for the most diverse needs.

## Develop a Job
### Model
The image below presents the UML class model for the Job. A data is composed of three other entities, namely:

- Batch Path: The location of this data in the data lake.
- Analytical Path: The location of this data in the data warehouse.
- Content: The content itself of the data, such as a dataframe (PySpark), and its schema.

![image](https://drive.google.com/uc?export=view&id=1bgcY3mSBWdDwt2kiTp6ztAIKfrceVIrB#center)

Below is an example of python code accessing the batch and analytics paths, as well as their content and dataframe.

````python
    batch_path = data.get_batch_path()
    analytical_path = data.get_analytical_path()
    
    schema = data.get_content().get_schema()
    dataframe = data.get_content().get_as_dataframe()
    
    print(batch_path)
    print(analytical_path)
    print(schema)
    print(dataframe)
````

### Services
When thinking about processing data in a cloud data platform, it’s a good idea to visualize the data flowing
through several stages. At each stage, we apply some data transformation and validation logic.

#### Standardize
Standardize file formats and resolve schema differences

````python
class StandardizeService(object):
    def __init__(self):
        pass

    def execute(self, data):
        data = self.standardize1(data)
        data = self.standardize2(data)
        data = self.standardize3(data)
        return data

    def standardize1(self, data):
        return data
    
    def standardize2(self, data):
        return data

    def standardize3(self, data):
        return data
````

#### Deduplication
Eliminate duplicate data copies.

````python
class DeduplicationService(object):
    def __init__(self):
        pass

    def execute(self, data):
        data = self.deduplication1(data)
        data = self.deduplication2(data)
        data = self.deduplication3(data)
        return data

    def deduplication1(self, data):
        return data
    
    def deduplication2(self, data):
        return data

    def deduplication3(self, data):
        return data
````

#### Quality Check
Measure data accuracy, completeness, consistency and reliability.

````python
class QualityCheckService(object):
    def __init__(self):
        pass

    def execute(self, data):
        data = self.qualitycheck1(data)
        data = self.qualitycheck2(data)
        data = self.qualitycheck3(data)
        return data

    def qualitycheck1(self, data):
        return data
    
    def qualitycheck2(self, data):
        return data

    def qualitycheck3(self, data):
        return data
````


#### Transformation 
Converting data from one structure another structure.

````python
class TransformationService(object):
    def __init__(self):
        pass

    def execute(self, data):
        data = self.transformation1(data)
        data = self.transformation2(data)
        data = self.transformation3(data)
        return data

    def transformation1(self, data):
        return data
    
    def transformation2(self, data):
        return data

    def transformation3(self, data):
        return data
````

#### Validation
Ensuring data is correct and useful.

````python
class ValidationService(object):
    def __init__(self):
        pass

    def execute(self, data):
        data = self.validation1(data)
        data = self.validation2(data)
        data = self.validation3(data)
        return data

    def validation1(self, data):
        return data
    
    def validation2(self, data):
        return data

    def validation3(self, data):
        return data
````

## Submit a Job
### Main python file
The URI of the main Python file to use as the driver. The following path contains the main python file:
```sh
[SOURCE_CODE]/Job.py
```

### Additional python files
The URIs Python files to pass to the PySpark framework. Supported file types: .py, .egg, and .zip.
The following paths contain the additional python files:
```sh
[SOURCE_CODE]/client
[SOURCE_CODE]/configuration
[SOURCE_CODE]/factory
[SOURCE_CODE]/model
[SOURCE_CODE]/proxy
[SOURCE_CODE]/service
[SOURCE_CODE]/util
```

### Jar Files
The URIs of jar files to add to the CLASSPATHs of the Python driver and tasks.
Below are the following extra files and their locations for running the Job:

| Files          | Source                                                                                          |
|----------------|-------------------------------------------------------------------------------------------------|
| Spark BigQuery | gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.0.jar                        |
| Spark Avro     | https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.1.2/spark-avro_2.12-3.1.2.jar |

### Arguments
The arguments to pass to the driver. Do not include arguments, such as --conf, that can be set as job properties,
since a collision may occur that causes an incorrect job submission.  Next, we present the following arguments
necessary to execute the Job:

| Key               | Value                                                                  |
|-------------------|------------------------------------------------------------------------|
| --id              | The job id                                                             |
| --allowDelete     | Allow to delete original data (default=False)                          |
| --deduplication   | Remove deduplication from original data (default=False)                |
| --fromBucket      | The bucket with original data                                          |
| --toBucket        | The bucket with modified data                                          |
| --fromDataset     | The dataset with original data                                         |
| --toDataset       | The dataset with modified data                                         |
| --temporaryBucket | A temporary bucket for data manipulation                               |
| --company         | A company that owns the data                                           |
| --region          | The region where the data was extracted                                |
| --businessUnit    | The business unit that the data belongs to                             |
| --vicePresidency  | The vice presidency the data belongs to                                |
| --domain          | The domain that the data belongs to                                    |
| --subdomain       | The subdomain that the data belongs to                                 |
| --context         | The context that the data belongs to (default=same as the subdomain)   |
| --pipeline        | The pipeline that ran the job                                          |
| --dataSource      | The source of the data (eg FTP, RDBMS and others)                      |
| --year            | The year of execution of the job (default=current year)                |
| --month           | The month of execution of the job (default=current month)              |
| --day             | The day of execution of the job (default=current day)                  |
| --execution       | A unique execution identifier (default=uuid version 4)                 |
| --schema          | A string containing the interpretation of the data (columns and types) |
