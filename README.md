# Montana Tax Lien Intelligence Dashboard

Nightly-refreshing dashboard for Gallatin County delinquent property taxes.
Hosted free on GitHub Pages. Data scraped automatically via GitHub Actions.

---

## How it works

```
GitHub Actions (runs at 2 AM nightly)
  → scraper.py downloads the Gallatin County PDF
  → extracts all Parcel IDs
  → scrapes each parcel from itax.gallatin.mt.gov
  → calculates tax/market value ratio
  → saves results to docs/data.json
  → commits & pushes to the repo

GitHub Pages serves docs/
  → your browser opens index.html
  → index.html fetches data.json
  → dashboard renders with fresh data
```

---

## Step-by-step deployment (no coding required)

### Step 1 — Create a GitHub account
If you don't have one: https://github.com/signup (free)

### Step 2 — Create a new repository
1. Go to https://github.com/new
2. Name it: `mt-tax-liens`
3. Set it to **Public** (required for free GitHub Pages)
4. Click **Create repository**

### Step 3 — Upload these files
On your new empty repository page, click **uploading an existing file**.

Upload ALL of these files, keeping the folder structure:
```
mt-tax-liens/
├── scraper.py
├── requirements.txt
├── .github/
│   └── workflows/
│       └── nightly-scrape.yml
└── docs/
    ├── index.html
    └── data.json
```

> **Tip:** You can drag the entire `mt-tax-liens` folder into the GitHub upload page.

Click **Commit changes**.

### Step 4 — Enable GitHub Pages
1. In your repository, click **Settings** (top menu)
2. In the left sidebar, click **Pages**
3. Under "Source", select **Deploy from a branch**
4. Branch: `main` — Folder: `/docs`
5. Click **Save**

After ~60 seconds, your dashboard will be live at:
`https://YOUR-USERNAME.github.io/mt-tax-liens/`

### Step 5 — Run the scraper for the first time
1. In your repository, click the **Actions** tab
2. Click **Nightly Tax Lien Scrape** in the left sidebar
3. Click **Run workflow** → **Run workflow** (green button)
4. Wait ~5–20 minutes (depends on number of parcels)
5. Refresh your dashboard — it now has real data!

### Step 6 — Verify nightly runs
The scraper runs automatically every night at 2 AM Mountain Time.
You can check the Actions tab any time to see run history and logs.

---

## Updating the PDF URL

When Gallatin County publishes a new delinquent list, update the URL in `scraper.py`:

```python
PDF_URL = "https://www.gallatinmt.gov/sites/g/files/vyhlif606/f/uploads/NEW_FILE.pdf"
```

Edit the file directly on GitHub (click the file → pencil icon → commit).
The next nightly run will use the new URL automatically.

---

## Expanding to other Montana counties

Each county has its own delinquent list URL and parcel system.
To add a county:
1. Duplicate `scraper.py` as `scraper_COUNTYNAME.py`
2. Update `PDF_URL` and `ITAX_BASE` for that county
3. Add a second step to the workflow in `nightly-scrape.yml`
4. Save results to `docs/data_COUNTYNAME.json`

---

## Troubleshooting

**Dashboard shows "Error loading data"**
- Make sure GitHub Pages is enabled pointing to `/docs`
- Check the Actions tab — if the scraper failed, there will be a red X

**Scraper gets 0 parcels from PDF**
- The county may have updated the PDF format
- Open an issue or manually check the PDF column layout

**Actions tab shows the workflow but it never ran**
- Go to Actions → enable workflows if GitHub prompts you to

---

## Files reference

| File | Purpose |
|------|---------|
| `scraper.py` | Python script — downloads PDF, scrapes iTax, saves data.json |
| `requirements.txt` | Python dependencies installed by GitHub Actions |
| `.github/workflows/nightly-scrape.yml` | Schedules the scraper to run nightly |
| `docs/index.html` | The dashboard UI — served by GitHub Pages |
| `docs/data.json` | The scraped data — read by the dashboard |
