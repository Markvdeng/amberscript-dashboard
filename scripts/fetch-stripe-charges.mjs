/**
 * Fetch Stripe charges for Amberscript.
 * Only grabs: date, metadata, description, currency, amount.
 * Uses Stripe REST API with expand=[] to minimize payload.
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
      // Only keep the fields we need
      charges.push({
        id: charge.id,
        created: charge.created,
        amount: charge.amount,
        currency: charge.currency,
        description: charge.description || '',
        metadata: charge.metadata || {},
        invoice: charge.invoice || null,
      });
    }

    if (!data.has_more) break;
    startingAfter = data.data[data.data.length - 1].id;
    console.log(`  Fetched ${charges.length} charges so far...`);
  }

  return charges;
}

function classifyChargeType(charge) {
  const desc = (charge.description || '').toLowerCase();
  const meta = charge.metadata || {};

  if (charge.invoice) {
    if (desc.includes('subscription') || desc.includes('premium')) return 'subscription';
    return 'invoice';
  }
  if (desc.includes('prepaid') || desc.includes('credit') || desc.includes('pack')) return 'prepaid';
  if (desc.includes('manual') || desc.includes('transfer')) return 'manual';
  return 'one-time';
}

async function main() {
  const { start } = getDateRange();
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
    description: c.description,
    metadata: c.metadata,
  }));

  saveRaw('stripe-charges.json', output);
  console.log(`Done: ${output.length} charges saved`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
