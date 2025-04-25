# Use the official PostgreSQL image as the base image
FROM postgres:17

# Set the environment variables for the PostgreSQL database
ENV POSTGRES_USER=admin
ENV POSTGRES_PASSWORD=admin
ENV POSTGRES_DB=db

# Copy the init_db.sql script to the Docker container
COPY init-db.sql /docker-entrypoint-initdb.d/

# Set the permissions for the init_db.sql script
RUN chmod 755 /docker-entrypoint-initdb.d/init-db.sql

# Expose the default PostgreSQL port
EXPOSE 5432
