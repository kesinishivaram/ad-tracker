# 🎈 Blank app template

A simple Streamlit app template for you to modify!

[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://blank-app-template.streamlit.app/)

### How to run it on your own machine

1. Install the requirements

   ```
   $ pip install -r requirements.txt
   ```

2. Run the app

   ```
   $ streamlit run streamlit_app.py
   ```

### Email notifications (deployment)

The notifier sends emails when new ads match a subscription. It can run **locally** (using `.streamlit/secrets.toml`) or in **GitHub Actions** (using repository secrets).

**Required GitHub repository secrets** (Settings → Secrets and variables → Actions):

| Secret | Description |
|--------|-------------|
| `SMTP_HOST` | SMTP server (e.g. `smtp.gmail.com`) |
| `SMTP_USER` | SMTP login / sender email |
| `SMTP_PASSWORD` | SMTP password or app password |
| `META_ACCESS_TOKEN` | Meta Ads Library API token (for Meta ads) |
| `GCP_SERVICE_ACCOUNT_JSON` | **Full JSON** of your Google service account key (for BigQuery / Google ads) |

For Gmail, use an [App Password](https://support.google.com/accounts/answer/185833) and set `SMTP_HOST` to `smtp.gmail.com`.

**Subscriptions:** The workflow reads `subscriptions.json` from the repo. If users subscribe only in the deployed Streamlit app (e.g. Streamlit Cloud), that file on the server is **not** the same as the repo. To get emails from the scheduled job, keep `subscriptions.json` in the repo (commit it with at least one subscription, or sync it from your app’s storage).

**Run the workflow:** Actions → **Run Ad Notifier** → **Run workflow**. It also runs hourly on the schedule.

### Testing notifications locally

1. **Test SMTP only (no subscriptions)**  
   Put email settings in `.streamlit/secrets.toml` under `[email]` (`smtp_host`, `smtp_port`, `smtp_user`, `smtp_password`, `from_address`). Then:

   ```bash
   python notifier.py --test-email your@email.com
   ```

   You should receive one email.

2. **Full run**  
   With `subscriptions.json` and secrets in place:

   ```bash
   python notifier.py
   ```

   The first run for a new subscription treats all matching ads as new and sends one email; later runs only send when there are new ads.
