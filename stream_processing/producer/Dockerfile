FROM python:3.7-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Install any needed packages specified in requirements.txt
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container
COPY . .

# Keep the container running to allow interaction via docker exec
CMD ["sleep", "infinity"]

# Run producer.py when the container launches
#CMD ["python", "-u", "./producer.py"]