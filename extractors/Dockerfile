# Use the official AWS Lambda Python 3.11 base image for ARM64 architecture
FROM --platform=linux/arm64 public.ecr.aws/lambda/python:3.11

# Set the working directory inside the container
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy requirements.txt first to leverage Docker caching
COPY requirements.txt ./

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all necessary files into the image (including batch handlers, scripts, etc.)
COPY . .

# Set the CMD to your Lambda function's handler
CMD ["main_extractor.lambda_handler"]
