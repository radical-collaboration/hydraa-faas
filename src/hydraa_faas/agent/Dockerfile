FROM python:3.9-slim

# set working directory
WORKDIR /app

# install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy agent source code
COPY . .

# expose port
EXPOSE 8000

# start the fastapi app (adjust path if necessary)
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
