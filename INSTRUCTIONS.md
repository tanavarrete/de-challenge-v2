# Introduction
This is a simple code that executes a data pipeline with a extraction, transformation and loading process (ETL). Starts with the extraction of the data from jsons files and finish loading the results into cvs files.

The stack of technologies used were the followings: Python, different Python libraries and Docker.

# Instructions for the deployment

The steps you have to follow to deploy the job are the followings:

1. Clone the repository.

2. Go to the root folder of the cloned repository.

3. Create the Docker volume:
```
docker build -t epljob -f ./deploy/Dockerfile .
```

3. Run the job.
\
 \
If you are running this on Mac:

    ```
    docker run -v ${PWD}:/output/ epljob
    ```
    If you are running this on Windows:
    ```
    docker run -v %cd%:/output/ epljob
    ```

4. The results are going to be exported to the root folder.



# Architecture case 

The technologies applied to meet the requirements of the case, considering both batch and streaming data flow inputs are in the architecture case folder.