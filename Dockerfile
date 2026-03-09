FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

RUN mkdir -p /app/data

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 CMD ["python", "-c", "import sys,urllib.request; urls=('http://localhost:8501/_stcore/health','http://localhost:8501/'); ok=False\nfor u in urls:\n    try:\n        with urllib.request.urlopen(u, timeout=3) as r:\n            ok = 200 <= r.status < 400\n            if ok:\n                break\n    except Exception:\n        pass\nsys.exit(0 if ok else 1)"]

CMD ["streamlit", "run", "app/streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
