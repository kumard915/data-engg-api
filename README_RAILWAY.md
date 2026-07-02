# Deploying to Railway

Railway is a beginner-friendly cloud platform that offers a free tier, auto-deployments on GitHub push, and free SSL (HTTPS).

Follow these simple steps to deploy your Mock Data Generator API:

---

## Step 1: Push Code to GitHub

1. Create a new repository on your GitHub account (e.g., `mock-data-generator-api`).
2. Run these commands in your project directory to push the code:
   ```bash
   git init
   git add .
   git commit -m "Initialize project with auth & rate limiting"
   git branch -M main
   git remote add origin <your-github-repo-url>
   git push -u origin main
   ```

---

## Step 2: Set Up Railway Project

1. Go to [Railway.app](https://railway.app) and sign up/sign in.
2. Click **New Project** -> **Deploy from GitHub repo**.
3. Select your repository `mock-data-generator-api`.
4. (Optional) Choose the folder where the server code resides (if it's in a subdirectory like `/server`). If it's in the root folder, leave it as default.
5. Click **Deploy**.

---

## Step 3: Add PostgreSQL Database

1. In your Railway project dashboard, click **+ New** (top right) or **Add Service**.
2. Select **Database** -> **Add PostgreSQL**.
3. Railway will provision a Postgres database service.
4. Railway will automatically link the database. It will inject a `DATABASE_URL` environment variable to your Node application service automatically!

---

## Step 4: Configure Environment Variables

1. Go to your Node application service in Railway.
2. Click on the **Variables** tab.
3. Add the following variables:
   - `NODE_ENV`: `production`
   - `JWT_SECRET`: `some_long_random_secret_string` (used to sign JWT logins)
   - `ADMIN_USERNAME`: `admin` (or any username you want)
   - `ADMIN_PASSWORD`: `your_secure_password` (credentials you will use to log in)
   - `AUTO_GENERATE`: `true` (runs the background transaction generator)
   - `AUTO_INTERVAL_MS`: `10000` (generates 1 transaction pair every 10 seconds)
4. Click **Save**. Railway will automatically redeploy the service with the new configuration.

---

## Step 5: Generate Public HTTPS Domain

1. In your Node application service, go to the **Settings** tab.
2. Under the **Networking** section, click **Generate Domain**.
3. Railway will assign a public HTTPS domain (e.g., `https://mock-data-generator-production.up.railway.app`).
4. You can now access your API globally over HTTPS!

---

## Step 6: Test Your Deployed API

1. **Health Check (Public):**
   `GET https://<your-railway-url>/health`
   Should return: `{"status":"ok", "postgres": true, ...}`

2. **Login (Public):**
   `POST https://<your-railway-url>/login`
   Body (JSON):
   ```json
   {
     "username": "admin",
     "password": "your_secure_password"
   }
   ```
   Should return: `{"token": "<JWT_TOKEN>", "expires_in": "24h"}`

3. **Query Data (Protected):**
   `GET https://<your-railway-url>/payins`
   Header: `Authorization: Bearer <JWT_TOKEN>`
   Should return the paginated list of payin events.
