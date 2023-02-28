# composer-ci-cd

This code looks into a way to implement CI/CD in Cloud Composer using Cloud Build and GitHub.

The blog for this code is published on [Medium](https://medium.com/@amarachi.ogu/implementing-ci-cd-in-cloud-composer-using-cloud-build-and-github-part-2-a721e4ed53da) 

Workflow Architecture Diagram   
![Workflow Architecture Diagram](https://miro.medium.com/v2/resize:fit:1400/format:webp/1*hIIbqvmZqgrcgDcp3jv4Iw.png)

## File Layout

```
.
├── dags
│   ├── stock_data_dag.py
│   └── stock_data_dag_test.py
├── dags-to-composer.cloudbuild.yaml
├── requirements-composer.txt
├── requirements-test.txt
├── requirements.txt
├── test-dags.cloudbuild.yaml
└── utils
    ├── add_dags_to_composer.py
    └── requirements.txt

```

-dags: 
This directory contains the DAG and the DAG test files.   
- stock_data_dag.py file is the stock data DAG we developed in a previous blog which gets deployed to Cloud Composer.    
- stock_data_dag_test.py contains dag validation tests to ensure the reliability and correctness of the workflows.  

-requirements-composer.txt - 
Contains the packages required to update the Cloud Composer environment.  

-requirements-test.txt - 
Contains the packages required by the DAG test file.  

-requirements.txt - 
Contains the packages required by the DAG file.  

-test-dags.cloudbuild.yaml - 
A YAML configuration file for the Cloud Build DAG validation checks.  

-utils:  
The utility contains a script  
- add_dags_to_composer.py - syncs the DAGs with your Cloud Composer environment after they are merged to the main branch in the repository.  
- requirements.txt - contains the packages required by the add_dags_to_composer.py.  


## Create Composer environment
```
gcloud composer environments create dev-environment \
 --location us-central1 \
 --image-version composer-1.20.5-airflow-2.3.4 \
 --service-account "example-account@example-project.iam.gserviceaccount.com"
```

## Automated Workflow

1. Make a change to a DAG and push that change to a development branch in your repository
2. Open a pull request against the main branch of your repository
3. Cloud Build runs unit tests to check your DAG is valid
4. If your pull request is approved and merged into your main branch
5. Cloud Build syncs your development Cloud Composer environment with these new changes 
6. You verify that the DAG behaves as expected in your development environment

The blog for this code is published on [Medium](https://medium.com/@amarachi.ogu/implementing-ci-cd-in-cloud-composer-using-cloud-build-and-github-part-2-a721e4ed53da)  