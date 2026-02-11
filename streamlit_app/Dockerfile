# Use lightweight python image
FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY . .

# Streamlit default port
EXPOSE 8501

# Command to run the dashboard
CMD ["streamlit", "run", "app_visualization.py", "--server.port=8501", "--server.address=0.0.0.0"]