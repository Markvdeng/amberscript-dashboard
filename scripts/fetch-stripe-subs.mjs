/**
 * Fetch Stripe subscriptions for Amberscript.
 * Outputs: raw/stripe-subs.json
 */

import { getDateRange, getWeekStart, saveRaw, retry } from './utils.mjs';

const API_KEY = process.env.STRIPE_AMBERSCRIPT_API_KEY;
if (!API_KEY) {
  console.error('Missing STRIPE_AMBERSCRIPT_API_KEY');
  process.exit(1);
}

const STRIPE_BASE = 'https://api.stripe.com/v1';

async function stripeGet(endpoint, params = {}) {
  const url = new URL(`${STRIPE_BASE}${endpoint}`);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined) url.searchParams.set(k, v);
  }
  const res = await fetch(url, {
    headers: { 'Authorization': `Bearer ${API_KEY}` },
  });
  if (!res.ok) throw new Error(`Stripe API error: ${res.status} ${await res.text()}`);
  return res.json();
}

async function fetchAllSubscriptions(since) {
  const subs = [];
  let startingAfter = undefined;

  while (true) {
    const params = {
      limit: 100,
      'created[gte]': Math.floor(new Date(since).getTime() / 1000),
      status: 'all',
    };
    if (startingAfter) params.starting_after = startingAfter;

    const data = await retry(() => stripeGet('/subscriptions', params));
    subs.push(...data.data);

    if (!data.has_more) break;
    startingAfter = data.data[data.data.length - 1].id;
    console.log(`  Fetched ${subs.length} subscriptions so far...`);
  }

  return subs;
}

function getPlanType(sub) {
  const interval = sub.items?.data?.[0]?.price?.recurring?.interval || '';
  if (interval === 'year') return 'yearly';
  if (interval === 'month') return 'monthly';
  return 'other';
}

async function main() {
  const { start } = getDateRange(365);
  console.log(`Fetching Stripe subscriptions since ${start}`);

  const subs = await fetchAllSubscriptions(start);
  console.log(`Fetched ${subs.length} subscriptions`);

  const output = subs.map(s => ({
    id: s.id,
    week: getWeekStart(new Date(s.created * 1000)),
    date: new Date(s.created * 1000).toISOString().slice(0, 10),
    status: s.status,
    planType: getPlanType(s),
    currency: (s.currency || 'eur').toUpperCase(),
    amount: (s.items?.data?.[0]?.price?.unit_amount || 0) / 100,
    interval: s.items?.data?.[0]?.price?.recurring?.interval || '',
    country: s.metadata?.country || '',
  }));

  saveRaw('stripe-subs.json', output);
  console.log(`Done: ${output.length} subscriptions saved`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
