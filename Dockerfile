FROM python:3.11-slim

# Install required dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc-dev \
    libgssapi-krb5-2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Add Microsoft GPG key and repository
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list

# Install Microsoft ODBC driver
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Setup ODBC driver config
RUN echo "[ODBC Driver 18 for SQL Server]\n\
Description=Microsoft ODBC Driver 18 for SQL Server\n\
Driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.4.so.1.1\n\
UsageCount=1" >> /etc/odbcinst.ini

WORKDIR /app

# Copy requirements and install as root (globally)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose the port the app runs on
EXPOSE 5000

# Command to run the application
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "app.main:app"]