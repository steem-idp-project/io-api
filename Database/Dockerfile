FROM postgres:17
COPY init-db.sql /docker-entrypoint-initdb.d/
RUN chmod 755 /docker-entrypoint-initdb.d/init-db.sql
EXPOSE 5432
