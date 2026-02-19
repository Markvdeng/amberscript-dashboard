/**
 * Fetch Google Ads cost data for all Amberscript accounts.
 * Uses REST API v19 with OAuth refresh token flow.
 * Outputs: raw/google-ads.json
 */

import {
  GOOGLE_ADS_ACCOUNTS,
  GOOGLE_ADS_MCC,
  getDateRange,
  getWeekStart,
  getProductType,
  saveRaw,
  retry,
} from './utils.mjs';

const {
  GOOGLE_ADS_DEVELOPER_TOKEN,
  GOOGLE_ADS_CLIENT_ID,
  GOOGLE_ADS_CLIENT_SECRET,
  GOOGLE_ADS_REFRESH_TOKEN,
} = process.env;

if (!GOOGLE_ADS_DEVELOPER_TOKEN || !GOOGLE_ADS_REFRESH_TOKEN) {
  console.error('Missing Google Ads credentials. Set GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN');
  process.exit(1);
}

async function getAccessToken() {
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      client_id: GOOGLE_ADS_CLIENT_ID,
      client_secret: GOOGLE_ADS_CLIENT_SECRET,
      refresh_token: GOOGLE_ADS_REFRESH_TOKEN,
    }),
  });
  if (!res.ok) throw new Error(`OAuth failed: ${res.status} ${await res.text()}`);
  const data = await res.json();
  return data.access_token;
}

async function queryGoogleAds(accessToken, customerId, query) {
  const url = `https://googleads.googleapis.com/v19/customers/${customerId}/googleAds:searchStream`;
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
      'login-customer-id': GOOGLE_ADS_MCC,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Google Ads API error (${customerId}): ${res.status} ${text}`);
  }
  const results = await res.json();
  // searchStream returns array of batches
  const rows = [];
  for (const batch of results) {
    if (batch.results) rows.push(...batch.results);
  }
  return rows;
}

async function main() {
  const { start, end } = getDateRange(365);
  console.log(`Fetching Google Ads data: ${start} to ${end}`);

  const accessToken = await getAccessToken();
  const allRows = [];

  const query = `
    SELECT
      campaign.name,
      segments.week,
      metrics.cost_micros,
      metrics.clicks,
      metrics.impressions,
      metrics.conversions,
      metrics.conversions_value
    FROM campaign
    WHERE segments.date BETWEEN '${start}' AND '${end}'
      AND metrics.cost_micros > 0
    ORDER BY segments.week
  `;

  for (const [accountName, customerId] of Object.entries(GOOGLE_ADS_ACCOUNTS)) {
    console.log(`  Fetching ${accountName} (${customerId})...`);
    try {
      const rows = await retry(() => queryGoogleAds(accessToken, customerId, query));
      for (const row of rows) {
        allRows.push({
          account: accountName,
          campaign: row.campaign?.name || '',
          week: row.segments?.week || '',
          cost: (row.metrics?.costMicros || 0) / 1_000_000,
          clicks: parseInt(row.metrics?.clicks || 0),
          impressions: parseInt(row.metrics?.impressions || 0),
          conversions: parseFloat(row.metrics?.conversions || 0),
          conversionsValue: parseFloat(row.metrics?.conversionsValue || 0),
          productType: getProductType(row.campaign?.name || ''),
        });
      }
      console.log(`    ${rows.length} rows`);
    } catch (err) {
      console.warn(`    Error fetching ${accountName}: ${err.message}`);
    }
  }

  // Aggregate by week + account + productType
  const weeklyMap = {};
  for (const row of allRows) {
    const key = `${row.week}|${row.account}|${row.productType}`;
    if (!weeklyMap[key]) {
      weeklyMap[key] = {
        week: row.week,
        account: row.account,
        productType: row.productType,
        cost: 0,
        clicks: 0,
        impressions: 0,
        conversions: 0,
        conversionsValue: 0,
      };
    }
    weeklyMap[key].cost += row.cost;
    weeklyMap[key].clicks += row.clicks;
    weeklyMap[key].impressions += row.impressions;
    weeklyMap[key].conversions += row.conversions;
    weeklyMap[key].conversionsValue += row.conversionsValue;
  }

  const output = Object.values(weeklyMap).sort((a, b) => a.week.localeCompare(b.week));
  saveRaw('google-ads.json', output);
  console.log(`Done: ${output.length} weekly aggregates`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
