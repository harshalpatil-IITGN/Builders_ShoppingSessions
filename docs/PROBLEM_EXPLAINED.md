# The Shopping Sessions Problem: Complete Plain-English Guide

Welcome to the **Shopping Sessions** data engineering project! This document explains everything you need to know about the project: the real-world story, what data we receive, what transformations we apply, why each step exists, and how our code is structured.

After reading this guide, you will be able to explain the entire system to your teammates, interviewers, or hackathon judges with confidence.

---

## 1. The Big Picture: What Problem Are We Solving?

Imagine you own a major online shopping store (like Amazon or Flipkart).

Every day, millions of customers visit your website:

- They click on advertisements on Google, Instagram, or YouTube.
- They browse different product pages.
- They add items to their shopping cart.
- Some of them end up purchasing products.

As a business, you need answers to two fundamental questions:

1. **User Behavior Question**: What did each customer do on our site each day? How many browsing sessions did they have? How much money did they spend, and what was their favorite shopping category?
2. **Marketing ROI Question**: Which ad campaigns actually brought customers to the site? When someone buys a \$50 pair of shoes, which ad campaign deserves the credit? And which other ads helped convince the buyer along the way?

Our job is to build a high-performance **Data Pipeline (ETL)** that ingests dirty, real-world log files, cleans them, connects fragmented user identities, groups clicks into browsing sessions, credits ad campaigns, and outputs two clean, daily summary tables in **Parquet** format.

```
+-----------------------------------------------------------------------------------+
|                                  THE PIPELINE                                     |
+-----------------------------------------------------------------------------------+
|  [Dirty Input CSVs]                                                               |
|  - Events (views, carts, purchases)                                               |
|  - Ad Clicks (Google, Facebook, etc.)                                             |
|  - Identity Links (device & cookie links)                                         |
|  - Catalogs (products.csv, campaigns.csv)                                         |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|  [Our Cleaning & Identity Engine]                                                 |
|  1. Identity Resolution: Merge fragmented user IDs into real human beings         |
|  2. Event Cleaning: Dedup, validate timestamps, drop 24h-late events              |
|  3. Sessionization: Group events into browsing sessions (30-min inactivity gap)   |
|  4. Attribution Engine: Credit session to the right ad click (7-day lookback)    |
|  5. Assist Counter: Count supporting ad campaigns that assisted the purchase      |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|  [Two Clean Parquet Tables Output]                                                |
|  1. /output/user_daily/      --> One row per Person per Day                       |
|  2. /output/campaign_daily/  --> One row per Ad Campaign per Day                  |
+-----------------------------------------------------------------------------------+
```

---

## 2. Understanding the Input Data

All input data is stored in CSV format under a directory called `/data`.

### File 1: `events/*.csv` (User actions on the website)

Whenever someone browses the website, an event is logged:

- `event_id`: Unique ID for this log entry (e.g. `e123`).
- `user_id`: The ID of the visitor at that moment (e.g. `u1`).
- `ts`: When the user performed the action (timestamp).
- `ingest_ts`: When the server actually recorded the log into the database.
- `event_type`: One of three actions: `view`, `cart`, or `purchase`.
- `product_id`: The product involved (e.g. `p55`).
- `price_cents`: The price in cents if it was a purchase (e.g. `2500` = \$25.00). Empty for views and carts.

### File 2: `clicks/*.csv` (Marketing ad clicks)

Whenever a user clicks an advertisement on the web:

- `click_id`: Unique ID of the ad click (e.g. `k99`).
- `user_id`: The cookie or user ID of the person who clicked.
- `campaign_id`: Which marketing campaign it was (e.g. `c_search_shoes`).
- `ts`: When the ad was clicked.

### File 3: `links/*.csv` (Identity Graph)

In the real world, **one person has multiple IDs**:

- When browsing on a phone, they are `u_phone`.
- When browsing on a laptop, they are `u_laptop`.
- When they log in with their email, the system realizes: `u_phone` and `u_laptop` belong to the same person!
- `links/*.csv` has rows like: `u_phone, u_laptop`.

### File 4 & 5: Static Catalogs

- `products.csv`: List of all products and their categories (`p1, shoes`, `p2, books`). If a product is missing from this file, its category is considered `unknown`.
- `campaigns.csv`: List of valid ad campaigns and their channel (`c_1, search`, `c_2, social`).
- `manifest.json`: Defines the study window (`window_start` and `window_end`).

---

## 3. Why the Data is Hard (The "Dirt" and Gotchas)

The hackathon tests our pipeline with tricky edge cases:

1. **Malformed CSV lines**: Lines with unbalanced quotes, wrong number of columns, or empty lines. These must be dropped cleanly without crashing.
2. **Weird line endings (`\r`)**: Only `\n` ends a line. If a line ends with `\r\n`, the `\r` is considered part of the last column value! If `price_cents` has `1000\r`, it is invalid because `\r` is not a digit.
3. **Three Different Timestamp Formats**:
   - **F1 (UTC)**: `2026-10-03T23:58:10Z`
   - **F2 (Timezone offset)**: `2026-10-03T19:58:10-04:00` (must subtract 4 hours to convert to UTC).
   - **F3 (Epoch Milliseconds)**: `1791071890000` (raw digits, may have leading zeros like `0001791071890000`).
4. **Duplicate Events**: If the same `event_id` appears multiple times, we must keep **strictly the copy with the smallest `ingest_ts`**. And this deduplication happens **before** validation: if that earliest copy has corrupt data, the event is permanently discarded!
5. **Late Events**: If an event arrives late (`ingest_ts - ts > 24 hours`), it is dropped.

---

## 4. The 5 Core Steps in Our Code

Our code is organized cleanly into modular files inside the `pipeline/` directory:

```
pipeline/
├── csv_dialect.py        --> Step 1: Strict CSV line parser
├── timestamp_parser.py   --> Step 2: Timestamp normalizer (F1, F2, F3)
├── identity.py           --> Step 3: Identity graph (Union-Find algorithm)
├── events.py             --> Step 4: Event dedup, validation, & sessionizer
├── attribution.py        --> Step 5: Ad click attribution & assist counter
└── pipeline.py           --> Main coordinator: runs end-to-end and writes Parquet
```

Here is what each file does:

---

### Step 1: Strict CSV Parser (`pipeline/csv_dialect.py`)

Standard CSV libraries (like Python's `csv` or DuckDB's default reader) make assumptions that violate the competition's strict rules (for example, DuckDB reads `""` as an empty string instead of `NULL`).

- **What it does**: Reads bytes directly. If a field is wrapped in quotes (`"u1"`), it strips the quotes (`u1`). If a field is empty or `""`, it returns `None` (NULL). If quotes appear inside a field (e.g. `a"b`), the line is marked malformed and dropped.

---

### Step 2: Timestamp Parser (`pipeline/timestamp_parser.py`)

Timestamps come in 3 different formats across the files.

- **What it does**: Inspects the string. If it's all digits, it treats it as epoch milliseconds (F3). If it's an ISO timestamp, it validates the calendar date (checks leap years for February 29!), parses hours/minutes/seconds and offsets, and converts everything into a single uniform number: **Unix epoch milliseconds (UTC)**.

---

### Step 3: Identity Resolution (`pipeline/identity.py`)

How do we know `u1`, `u4`, and `u9` are the same human?

- If `links.csv` says:
  - `u1` is linked to `u4`
  - `u4` is linked to `u9`
- By transitivity, `u1`, `u4`, and `u9` are **one single person**.
- **The Rule**: Each person must be named by the **byte-wise smallest ID** among all their linked IDs. Here, `u1` < `u4` < `u9`, so this person is named **`u1`**.
- **Algorithm Used**: **Disjoint Set Union (Union-Find)** with path compression. This allows us to merge millions of IDs in linear time $O(N \cdot \alpha(N))$ without running out of memory.

---

### Step 4: Event Cleaning & Sessionization (`pipeline/events.py`)

Once we know who each user is, we process their browsing activity:

1. **Deduplicate on `event_id`**: For duplicate IDs, keep the one with the smallest `ingest_ts`.
2. **Validate**: Make sure action is `view`, `cart`, or `purchase`, `product_id` is present, timestamp is within `[window_start, window_end)`, and price is an integer between 1 and 10,000,000 cents.
3. **Drop late events**: Discard if `ingest_ts - ts > 86,400,000 ms` (24 hours).
4. **Sessionize (The 30-Minute Rule)**:
   - Imagine a user visiting your website. They click around for 10 minutes, then leave to have dinner. 45 minutes later, they come back and click again.
   - That should count as **two separate sessions**!
   - **Rule**: If the gap between consecutive events is **over 30 minutes** (1,800,000 ms), start a new session. If two events happen at the exact same millisecond, they belong to the same session.
   - Each session is assigned to the **UTC calendar date of when it started**.

---

### Step 5: Attribution & Assists (`pipeline/attribution.py`)

Now that we have sessions, who brought the shopper to the store?

1. **The 7-Day Lookback Window**: We look at all ad clicks made by this person in the 7 days prior to the session start:
   $$
   \text{session\_start} - 7\text{ days} \le \text{click.ts} \le \text{session\_start}
   $$
2. **Last-Click Attribution**:
   - Which ad gets credit for the session? The **latest click** before the session started.
   - If two clicks happened at the same exact millisecond, pick the one with the byte-wise smallest `click_id`.
   - If the user never clicked any ad in the last 7 days, attribute the session to campaign **`organic`** (channel `organic`).
3. **Assisting Campaigns**:
   - If a customer clicked a Facebook ad 3 days ago, an Instagram ad 1 day ago, and a Google Search ad 10 minutes before buying:
     - Google Search gets the **credited session** (last click).
     - Facebook and Instagram get counted as **assisted sessions**!
   - Every campaign with at least one qualifying click in the 7-day window gets 1 assist for that session.
   - `organic` never receives assists.

---

## 5. The Two Final Output Tables

The pipeline writes two Parquet tables to `/output/`:

### Table 1: `/output/user_daily/`

Aggregated per **(Person, Day)**:

- `user_id`: The person's canonical name (smallest ID).
- `day`: UTC date of the session start (Parquet `DATE`).
- `sessions`: Number of sessions started that day.
- `events`: Total surviving events across those sessions.
- `revenue_cents`: Total purchase money spent that day.
- `top_category`: The category with the highest purchase revenue (ties broken by alphabetical order; `NULL` if 0 purchases).

### Table 2: `/output/campaign_daily/`

Aggregated per **(Campaign, Day)**:

- `campaign_id`: The campaign name (or `organic`).
- `channel`: Channel type (`search`, `social`, etc.).
- `day`: UTC date.
- `sessions`: Number of sessions credited to this campaign.
- `purchasing_sessions`: How many of those credited sessions had at least 1 purchase.
- `revenue_cents`: Total purchase revenue from credited sessions.
- `assisted_sessions`: How many sessions this campaign helped assist.

---

## 6. How to Run & Test the Code

### Running all 36 test fixtures locally:

```bash
python scripts/run_fixtures.py fixtures -- python pipeline/pipeline.py
```

If everything is correct, the test runner prints:

```
ok   A1
ok   A2
...
ok   E20
ok   L1
ok   L2
ok   L3
ok   L4

36/36 fixtures passed
```

### Self-Check Rule:

To verify the math is consistent:

$$
\sum \text{sessions in user\_daily} == \sum \text{sessions in campaign\_daily}
$$

$$
\sum \text{revenue\_cents in user\_daily} == \sum \text{revenue\_cents in campaign\_daily}
$$

---

## 7. Quick Summary Cheatsheet (To Explain to Others)

| Term                             | What it means in plain English                                                                              |
| :------------------------------- | :---------------------------------------------------------------------------------------------------------- |
| **Person vs User ID**      | A user ID is a temporary cookie or device ID; a Person is the real human identified by grouping linked IDs. |
| **Union-Find**             | An efficient graph algorithm that merges connected IDs into a single person group.                          |
| **Session**                | A continuous browsing visit. A gap $> 30$ minutes starts a new visit.                                     |
| **Last-Click Attribution** | Giving credit for a shopping session to the most recent ad clicked in the past 7 days.                      |
| **Assisting Campaign**     | Any other ad clicked in the 7 days prior that helped nudge the user to visit.                               |
| **Parquet**                | A fast, compressed, columnar file format used in big data systems.                                          |
