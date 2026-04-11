# Zero-Cost Deployment Guide for Stock Scanner

Since you are looking for the absolute easiest and 100% free way to host your website and have it automatically update, here is the exact strategy I recommend. 

Your website has two parts: the **Frontend** (what the user sees) and the **Backend** (the engine & data scheduler). We will use the best modern free platforms for each part.

## Step 1: Upload your code to GitHub
All modern hosting platforms pull your code directly from GitHub.

1. Go to [GitHub](https://github.com/) and create a free account.
2. Download [GitHub Desktop](https://desktop.github.com/). This is the easiest way to upload files for non-technical users.
3. Open GitHub Desktop, go to **File** -> **Add Local Repository**, and select your `Stock Scanner c` folder.
4. Click **Publish repository** to push your code securely to GitHub.

## Step 2: Deploy the Frontend (Vercel)
We will use Vercel because it is specifically built to host visual interfaces instantly for free.

1. Go to [Vercel.com](https://vercel.com/) and sign up using your GitHub account.
2. Click **Add New** -> **Project**.
3. Import the Github repository you just uploaded.
4. **Important**: Before clicking deploy, look for **Root Directory**, click edit, and select the `frontend` folder.
5. Click **Deploy**. Vercel will give you a live URL for your website (e.g., `https://your-app.vercel.app`). Copy this URL.

## Step 3: Deploy the Backend (Hugging Face Spaces)
Render sometimes asks for a credit card for account verification. Since we want an option that requires NO credit card whatsoever, we will use **Hugging Face Spaces**. It is wildly powerful, completely free, and does not require a credit card.

To make this magical for you, **I have just created a `Dockerfile` in your folder.**

1. Go to [HuggingFace.co](https://huggingface.co/) and create a free account.
2. In the top right corner (click your profile icon), select **New Space**.
3. Name your space (e.g. `stock-scanner-backend`).
4. Select the **Docker** icon under the "Select the Space SDK" section.
5. In the "Docker template" option, leave it as "Blank".
6. Click **Create Space**.
7. Now, on the next page, click on **Files** (near the top menu). 
8. Click **Add file > Upload files** and drag the files from your local `Stock Scanner c` folder up into this space. (Or link your GitHub repo if you feel comfortable!). Make sure to upload the new `Dockerfile` I just created for you.
9. **Link them together**: Go to the Space's settings -> Variables and secrets. Click **New secret**. Create a secret with the name `FRONTEND_ORIGIN` and paste the Vercel URL you got in Step 2. Then, inside your local `frontend/.env` file, you can set `VITE_API_BASE=https://your-username-stock-scanner-backend.hf.space` (and push the update to Github so Vercel builds it).

## Step 4: The "Keep-Awake" Trick (For Automatic Updates)
Your app already has a built-in schedule to automatically update data depending on market time. However, free containers go into "sleep mode" if not used for a couple days. 

Here is how we keep it awake 24/7 for free:
1. Go to [cron-job.org](https://cron-job.org/en/) and create a free account.
2. Click **Create Cronjob**.
3. For the URL, paste your Hugging Face space URL with `/docs` at the end (e.g., `https://your-username-stock-scanner-backend.hf.space/docs`).
4. Set the execution schedule to **every 14 minutes**.
5. Save. 

> [!TIP]
> Now, `cron-job.org` will pretend to be a visitor every 14 minutes. Your Hugging Face Space will stay continuously active, meaning the internal data scheduler will successfully trigger its daily automatic updates without you lifting a finger!
