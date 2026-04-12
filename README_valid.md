# Multilingual Streamlit questionnaire for strategic validation

This repository contains a Streamlit app for the strategic validation of the draft document on priority socio-economic statistics in Africa.

## Features

- Four interface languages: English, French, Portuguese, and Arabic
- Single-page institutional questionnaire based on the decision-oriented summary note
- Required-question validation before submission
- GitHub API saving to JSON files, with local JSON/CSV download fallback
- Arabic right-to-left interface support
- Flat export section included inside each JSON submission for easier consolidation

## Files

- `streamlit_app.py`: main Streamlit application
- `requirements.txt`: Python dependencies

## Local run

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Recommended Streamlit secrets

Create `.streamlit/secrets.toml` with content like this:

```toml
[github]
owner = "YOUR_GITHUB_OWNER"
repo = "YOUR_PUBLIC_REPO_NAME"
token = "YOUR_GITHUB_FINE_GRAINED_TOKEN"
branch = "main"
folder = "submissions"

[links]
note_url = "https://..."
full_doc_url = "https://..."
```

## How saving works

Each response is saved as one JSON file in the configured GitHub repository, under a path like:

```text
submissions/YYYY/MM/DD/SUB-20260412T210000Z-ABC123.json
```

This approach avoids concurrent writes to a single CSV file and is more robust for public-repository deployments.

## Deployment

This app is suitable for deployment on Streamlit Community Cloud or another Streamlit-compatible host. The code repository can remain public, while the GitHub write token is stored securely in Streamlit secrets.
