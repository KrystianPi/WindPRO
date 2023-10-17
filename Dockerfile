# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Copy the application code to the working directory
COPY . .

# Expose the port on which the application will run
EXPOSE 8080

# Define environment variable
ENV NAME World

# Run FastAPI when the container launches
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
