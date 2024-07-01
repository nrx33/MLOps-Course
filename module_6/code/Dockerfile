# Use the official AWS Lambda Python 3.9 base image
FROM public.ecr.aws/lambda/python:3.9

# Upgrade pip and install pipenv
RUN pip install -U pip
RUN pip install pipenv

# Set the working directory to /var/task
WORKDIR /var/task

# Copy Pipfile and Pipfile.lock to the working directory
COPY Pipfile Pipfile.lock ./

# Install dependencies from Pipfile
RUN pipenv install --system --deploy

# Copy the lambda function and model script to the working directory
COPY lambda_function.py model.py ./

# Verify the files are in the correct location
RUN ls -l /var/task

# Define the CMD to run the lambda function handler
CMD ["lambda_function.lambda_handler"]
