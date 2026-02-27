## CQC CSV Downloader Service

This is a small production-ready FastAPI service designed to run on Render.  
It downloads two dynamic CSV exports from the Care Quality Commission (CQC) website, saves them to a persistent disk, and exposes an authenticated API that Zapier (or any client) can use to trigger downloads and fetch the resulting files.

The service does **not** scrape HTML. It only uses the official CSV export URLs.

### CSV Sources

- **Homecare agencies CSV**  
  `https://www.cqc.org.uk/search/all?query=&location-query=&radius=&display=csv&sort=relevance&last-published=week&filters[]=archived:active&filters[]=lastPublished:all&filters[]=more_services:all&filters[]=overallRating:Not%20rated&filters[]=overallRating:Inadequate&filters[]=overallRating:Requires%20improvement&filters[]=services:homecare-agencies&filters[]=specialisms:all`

- **Care homes CSV**  
  `https://www.cqc.org.uk/search/all?query=&location-query=&radius=&display=csv&sort=relevance&last-published=week&filters[]=archived:active&filters[]=careHomes:all&filters[]=lastPublished:all&filters[]=more_services:all&filters[]=overallRating:Not%20rated&filters[]=overallRating:Inadequate&filters[]=overallRating:Requires%20improvement&filters[]=services:care-home&filters[]=specialisms:all`

---

## API Overview

- **GET `/health`**  
  - Public healthcheck endpoint.  
  - Response: `{"ok": true}`

- **POST `/run`**  
  - Authenticated. Triggers downloads for both CSVs.  
  - Uses header `X-Auth-Token` which must match `RUN_TOKEN`.  
  - Returns JSON containing file names, file sizes, and URLs to download.

- **GET `/files/{filename}`**  
  - Authenticated via query param `?token=`.  
  - Example: `/files/cqc_homecare_20260224_120000Z.csv?token=YOUR_RUN_TOKEN`  
  - Returns the file as a CSV attachment.

### Authentication

- **Environment variable**: `RUN_TOKEN`  
- **POST `/run`**:
  - Require header: `X-Auth-Token: <RUN_TOKEN>`
- **GET `/files/{filename}`**:
  - Require query param: `?token=<RUN_TOKEN>`

If `RUN_TOKEN` is not configured, authenticated endpoints will return HTTP 500.

---

## Storage and Filenames

- **Environment variable**: `DATA_DIR` (default: `/data`)
- The service will create this directory on startup if it does not exist.
- Files are saved with timestamped names and never overwritten:
  - `cqc_homecare_YYYYMMDD_HHMMSSZ.csv`
  - `cqc_carehomes_YYYYMMDD_HHMMSSZ.csv`

The service also performs a **best-effort cleanup** of files older than **30 days** in `DATA_DIR` whose names start with `cqc_` and end with `.csv`.

---

## Download Robustness

The service uses `requests` with:

- Streaming downloads (`stream=True`)
- Browser-like headers:
  - `User-Agent`: modern desktop browser UA
  - `Accept`: `text/csv,application/csv,application/octet-stream;q=0.9,*/*;q=0.8`
- Timeouts and retries:
  - Up to **5 retries**
  - Exponential backoff between retries
  - Retries on common transient statuses: `429`, `500`, `502`, `503`, `504`

### CSV Validation

Each response is validated before the file is finalized:

- If `Content-Type` contains `"csv"`, it is accepted, **or**
- First chunk is inspected to ensure:
  - It contains commas and newlines, and
  - It does **not** look like HTML (`<html`, `<!doctype html>`, etc.)

If validation fails, the partial file is deleted and the error is reported in the `/run` response.

---

## `/run` Response Format

Example successful response:

```json
{
  "status": "ok",
  "saved_to": "/data",
  "results": {
    "homecare": {
      "file": "cqc_homecare_20260224_120000Z.csv",
      "bytes": 12345,
      "url": "/files/cqc_homecare_20260224_120000Z.csv",
      "error": null
    },
    "carehomes": {
      "file": "cqc_carehomes_20260224_120000Z.csv",
      "bytes": 67890,
      "url": "/files/cqc_carehomes_20260224_120000Z.csv",
      "error": null
    }
  }
}
```

- **status**:
  - `"ok"`: both downloads succeeded
  - `"partial"`: one succeeded, one failed
  - `"error"`: both failed
- **results.\*.error**: error message string when a download fails, otherwise `null`.

---

## Running Locally

### 1. Create and activate a virtualenv (optional but recommended)

```bash
cd /Users/benji/Desktop/Adastra/Clients/Ash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Set environment variables

At minimum, set `RUN_TOKEN` so authenticated endpoints work:

```bash
export RUN_TOKEN="your-secret-token"
export DATA_DIR="/data"  # or any local path, e.g. "./data"
export PORT=10000
mkdir -p "$DATA_DIR"
```

### 4. Run with Uvicorn (development)

```bash
uvicorn app:app --host 0.0.0.0 --port "$PORT" --reload
```

Healthcheck:

```bash
curl http://localhost:10000/health
```

Trigger downloads:

```bash
curl -X POST "http://localhost:10000/run" \
  -H "X-Auth-Token: $RUN_TOKEN"
```

Download a file (replace filename with the one from `/run`):

```bash
curl -L "http://localhost:10000/files/cqc_homecare_20260224_120000Z.csv?token=$RUN_TOKEN" \
  -o homecare.csv
```

---

## Docker Usage (Local)

Build the image:

```bash
docker build -t cqc-csv-service .
```

Run the container:

```bash
docker run --rm \
  -e RUN_TOKEN="your-secret-token" \
  -e DATA_DIR="/data" \
  -e PORT=10000 \
  -p 10000:10000 \
  -v "$(pwd)/data:/data" \
  cqc-csv-service
```

Test:

```bash
curl http://localhost:10000/health
curl -X POST "http://localhost:10000/run" -H "X-Auth-Token: your-secret-token"
```

---

## Deploying to Render

### 1. Create a new Web Service

1. Push this repository to GitHub or another Git provider.
2. In Render, click **New > Web Service**.
3. Select your repo.
4. **Environment**: `Docker`.
5. **Region**: choose as appropriate.

Render will build the Dockerfile in the root of the repo.

### 2. Configure environment variables

Under **Environment**:

- **RUN_TOKEN**: your shared secret for Zapier and any other clients.
- **DATA_DIR**: `/data` (recommended; matches Dockerfile and docs).
- **PORT**: `10000` (Render will also provide `PORT` automatically; this default is safe).

### 3. Configure persistent disk

1. In the Render service dashboard, go to **Disks**.
2. Add a new disk, for example:
   - **Name**: `cqc-data`
   - **Size**: suitable for your usage (e.g. 1–5 GB).
   - **Mount Path**: `/data`
3. Redeploy if needed.

### 4. Healthcheck

Set the Render healthcheck path to:

- `/health`

Confirm that your service responds with:

```json
{"ok": true}
```

---

## Render Scheduled Job (Cron) Setup

In addition to the web service, you can configure a **Render Scheduled Job** that runs the CSV download once on a cron schedule.

### 1. Create a Scheduled Job

1. In Render, click **New > Cron Job**.
2. Use the **same repository** as the web service.
3. **Environment**: `Docker` (it will reuse the same Dockerfile).

### 2. Command and schedule

- **Docker Command**:

  ```text
  python3 run_job.py
  ```

- **Schedule** (as requested):

  ```text
  0 9 * * 5
  ```

  This runs every Friday at 09:00 UTC (or according to your configured timezone in Render).

### 3. Environment and disk

Configure the same environment variables as the web service:

- **DATA_DIR**: `/data` (recommended; matches the web service and Dockerfile).

Optionally also set:

- **RUN_TOKEN**: not required for `run_job.py`, but safe to keep consistent with your web service.

Attach a **persistent disk** (or reuse an existing one) with:

- **Mount Path**: `/data`

The job will:

- Read `DATA_DIR` (default `./data` if not set, but `/data` is recommended on Render).
- Create it if needed.
- Download and save:
  - `cqc_homecare_YYYYMMDD_HHMMSSZ.csv`
  - `cqc_carehomes_YYYYMMDD_HHMMSSZ.csv`
- Log:
  - Start and end messages.
  - HTTP status codes.
  - Bytes downloaded.
  - Approximate number of lines.
  - Final paths for both files.
- Exit with:
  - **Code `0`** if both downloads succeed.
  - **Non-zero** if any download fails (so Render will mark the job as failed).

---

## Zapier Integration

Zapier will:

1. **Trigger**: something in your Zap (e.g. schedule, webhook, etc.)  
2. **Action 1**: `Webhook by Zapier` – POST to `/run`.  
3. **Action 2**: `Webhook by Zapier` – GET the file(s) from `/files/{filename}?token=...` using the filenames returned by `/run`.

### Step 1 – Trigger `/run`

- **Zapier action**: Webhooks by Zapier → **Custom Request** or **POST**.
- **URL**:  
  `https://your-render-service.onrender.com/run`
- **Method**: `POST`
- **Headers**:
  - `X-Auth-Token: YOUR_RUN_TOKEN`
- **Body**: can be empty JSON or form data; the endpoint ignores the body.

Zapier will receive a JSON payload like the example in the `/run` Response section, from which you can map `results.homecare.file` and `results.carehomes.file`.

### Step 2 – Download the CSV file(s)

For each CSV you want to pull into Zapier:

- **Zapier action**: Webhooks by Zapier → **GET**.
- **URL (example for homecare)**:

  ```text
  https://your-render-service.onrender.com/files/{{results.homecare.file}}?token=YOUR_RUN_TOKEN
  ```

  Replace `{{results.homecare.file}}` with the exact reference to the output field from the previous step in Zapier’s UI.

- **Authentication**: already handled via the `?token=` query parameter.
- **Response**: Zapier will get the CSV file content which can be passed to downstream steps (e.g. Google Sheets, Storage, etc.).

---

## Example cURL Commands

Assuming your Render service base URL is `https://your-render-service.onrender.com`:

- **Healthcheck**:

```bash
curl "https://your-render-service.onrender.com/health"
```

- **Trigger run**:

```bash
curl -X POST "https://your-render-service.onrender.com/run" \
  -H "X-Auth-Token: YOUR_RUN_TOKEN"
```

- **Download homecare CSV**:

```bash
curl -L "https://your-render-service.onrender.com/files/cqc_homecare_20260224_120000Z.csv?token=YOUR_RUN_TOKEN" \
  -o homecare.csv
```

- **Download carehomes CSV**:

```bash
curl -L "https://your-render-service.onrender.com/files/cqc_carehomes_20260224_120000Z.csv?token=YOUR_RUN_TOKEN" \
  -o carehomes.csv
```

Replace filenames with real ones from the `/run` response.

