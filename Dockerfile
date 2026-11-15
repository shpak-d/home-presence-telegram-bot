FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && groupadd --system --gid 10001 presence \
    && useradd --system --uid 10001 --gid presence --no-create-home presence \
    && install -d -m 0700 -o presence -g presence /data \
    && install -m 0600 -o presence -g presence /dev/null /data/.initialized

COPY config.py database.py main.py notifications.py presence_tracker.py telegram_bot.py tenda_client.py ./
USER 10001:10001
CMD ["python", "-m", "main"]
