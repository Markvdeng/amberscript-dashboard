/**
 * Fetch Stripe charges/payment intents for Amberscript.
 * Uses Stripe REST API (no SDK needed).
 * Outputs: raw/stripe-charges.json
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

async function fetchAllCharges(since) {
  const charges = [];
  let startingAfter = undefined;

  while (true) {
    const params = {
      limit: 100,
      'created[gte]': Math.floor(new Date(since).getTime() / 1000),
    };
    if (startingAfter) params.starting_after = startingAfter;

    const data = await retry(() => stripeGet('/charges', params));
    for (const charge of data.data) {
      if (charge.status !== 'succeeded') continue;
      charges.push(charge);
    }

    if (!data.has_more) break;
    startingAfter = data.data[data.data.length - 1].id;
    console.log(`  Fetched ${charges.length} charges so far...`);
  }

  return charges;
}

function classifyChargeType(charge) {
  const desc = (charge.description || '').toLowerCase();
  const invoiceId = charge.invoice;

  if (invoiceId) {
    // Invoice-based: likely subscription or manual invoice
    if (desc.includes('subscription') || desc.includes('premium')) return 'subscription';
    return 'invoice';
  }
  if (desc.includes('prepaid') || desc.includes('credit') || desc.includes('pack')) return 'prepaid';
  if (desc.includes('manual') || desc.includes('transfer')) return 'manual';
  return 'one-time';
}

function main_process() {
  return async () => {
    const { start } = getDateRange(365);
    console.log(`Fetching Stripe charges since ${start}`);

    const charges = await fetchAllCharges(start);
    console.log(`Fetched ${charges.length} successful charges`);

    const output = charges.map(c => ({
      id: c.id,
      week: getWeekStart(new Date(c.created * 1000)),
      date: new Date(c.created * 1000).toISOString().slice(0, 10),
      amount: c.amount / 100,
      currency: (c.currency || 'eur').toUpperCase(),
      type: classifyChargeType(c),
      country: c.billing_details?.address?.country || '',
      description: c.description || '',
    }));

    saveRaw('stripe-charges.json', output);
    console.log(`Done: ${output.length} charges saved`);
  };
}

main_process()().catch(err => {
  console.error(err);
  process.exit(1);
});
