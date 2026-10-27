FROM python:3.12-alpine

WORKDIR /app
COPY tracker.py .
COPY sample/ sample/
COPY tests/ tests/

EXPOSE 8080
# Cookies come from the mounted /data volume (cookies.json), never from the image.
# USER_AGENT is required in normal mode — must match the browser that holds the session.
CMD ["python3", "-u", "tracker.py"]