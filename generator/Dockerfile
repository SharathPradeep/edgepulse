FROM python:3.13-slim
WORKDIR /app
COPY generator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY generator/ ./generator/
COPY contracts/ ./contracts/
ENV CONTRACTS_DIR=/app/contracts
CMD ["python", "-u", "generator/generator.py"]
