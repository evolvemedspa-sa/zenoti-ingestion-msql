# Railway: Root Directory = Zenoti; Dockerfile Path = guest_form_retrieve/Dockerfile.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY guest_form_retrieve/requirements.txt /tmp/guest-form-requirements.txt
RUN pip install --no-cache-dir -r /tmp/guest-form-requirements.txt
RUN python -m playwright install --with-deps chromium

COPY guest_form_retrieve ./guest_form_retrieve
COPY zconnect/_client.py zconnect/login_headers.py ./zconnect/

CMD ["python", "guest_form_retrieve/service_forms.py"]
