# Introduction
This code run a simple that Extract-Transform-Load (ETL) data pipeline. Starts extracting the data from json files and finish loading the results into cvs files.

The stack of technologies used are Python, diferent Python libraries and Docker.

# Instructions for the deploy

The comands you have to run to deploy the job are the followings:

1. Clone the repository.

2. Go to the root folder of the cloned repository.

3. Create the Docker volume:
```
docker build -t epljob -f ./deploy/Dockerfile .
```

3. Run the job. If you are running this in Mac:

```
docker run -v ${PWD}:/output/ epljob
```
If you are running this in Windows:
```
docker run -v %cd%:/output/ epljob
```

4. The results are going to be in the root folder.