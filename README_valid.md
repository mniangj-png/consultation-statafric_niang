# Streamlit validation questionnaire

This package contains a multilingual Streamlit questionnaire for the validation of the draft document on priority socio-economic statistics in Africa.

## Main features

- Languages: English, French, Portuguese (Portugal), Arabic
- Step-by-step workflow with required-field checks before moving forward
- Empty initial choices for selection widgets, with values preserved in `st.session_state` when the user goes back to a previous step
- Conditional justifications when the respondent selects:
  - Go with reservations / Validé sous réserve / equivalente
  - No-Go / Non-validé / equivalente
- Dropdown fields for:
  - country or REC represented
  - title of main respondent
- Draft save and resume for 48 hours
- Final submission to a GitHub repository through the GitHub Contents API
- Fallback download of JSON and CSV response files
- Default response folder in GitHub: `validation_doc`
- Built-in links to:
  - the English and French Word Online draft documents
  - the summary note in English, French, Portuguese and Arabic

## Files

- `streamlit_app.py`: main app
- `requirements.txt`: dependencies

## Default GitHub target

The app is preconfigured to target:

- owner: `mniangj-png`
- repo: `consultation-statafric_niang`
- branch: `data`
- folder: `validation_doc`

You only need to provide a GitHub token with write permission to the repository.

## GitHub / Streamlit secrets

Configure these secrets in `.streamlit/secrets.toml` or in the Streamlit Cloud secrets panel:

```toml
[github]
owner = "mniangj-png"
repo = "consultation-statafric_niang"
token = "YOUR_GITHUB_FINE_GRAINED_TOKEN"
branch = "data"
```

If needed, you may still override these values through environment variables.

## Optional environment variables

You can override the built-in document links if needed:

- `NOTE_URL_EN`
- `NOTE_URL_FR`
- `NOTE_URL_PT`
- `NOTE_URL_AR`
- `FULL_DOC_URL_EN`
- `FULL_DOC_URL_FR`
- `GITHUB_OWNER`
- `GITHUB_REPO`
- `GITHUB_BRANCH`
- `GITHUB_TOKEN`

## Suggested repository structure after submissions

```text
validation_doc/
  drafts/
    ABCD1234.json
  submissions/
    2026/
      04/
        13/
          SUB-20260413T011500Z-ABC123.json
```

## Running locally

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Notes

- Drafts expire 48 hours after saving.
- If GitHub is not configured, the app still works in local-only mode with downloadable JSON and CSV files.
- The question on the three most important revisions is optional.
