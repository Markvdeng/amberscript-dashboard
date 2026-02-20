/**
 * Fetch Stripe charges for Amberscript.
 * Only keeps: date, amount, currency, description, metadata.
 * Outputs: raw/stripe-charges.json
 */

import { getDateRange, getWeekStart, getMonth, saveRaw, retry } from './utils.mjs';

const API_KEY = process.env.STRIPE_AMBERSCRIPT_API_KEY;
if (!API_KEY) {
  console.error('Missing STRIPE_AMBERSCRIPT_API_KEY');
  process.exit(1);
}

async function stripeGet(endpoint, params = {}) {
  const url = new URL(`https://api.stripe.com/v1${endpoint}`);
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
    for (const c of data.data) {
      if (c.status !== 'succeeded') continue;
      const date = new Date(c.created * 1000).toISOString().slice(0, 10);
      const meta = c.metadata || {};
      const desc = c.description || '';

      // Country: extract from metadata key pattern P1_XX_N
      const countryKey = Object.keys(meta).find(k => /^P1_[A-Z]{2}_\d+$/.test(k));
      const country = countryKey ? countryKey.split('_')[1] : '';

      // Product: "perfect" jobType = Human-Made, else Machine-Made
      const product = meta.jobType === 'perfect' ? 'Human-Made' : 'Machine-Made';

      // Plan type from description
      let planType = 'Prepaid';
      if (/subscription/i.test(desc)) planType = 'Subscription';
      else if (/invoice/i.test(desc)) planType = 'Invoice';

      // Plan subtype
      let planSubtype = '';
      if (planType === 'Subscription') {
        planSubtype = /creation/i.test(desc) ? 'Creation' : 'Update';
      } else if (planType === 'Prepaid') {
        planSubtype = meta.uploadBatchId === 'addCredit' ? 'Top-Up' : 'Job Creation';
      }

      charges.push({
        id: c.id,
        date,
        week: getWeekStart(date),
        month: getMonth(date),
        amount: c.amount / 100,
        currency: (c.currency || 'eur').toUpperCase(),
        country,
        product,
        planType,
        planSubtype,
        customerId: c.customer || '',
        paymentIdentifier: meta.paymentIdentifier || '',
        uploadBatchId: meta.uploadBatchId || '',
      });
    }

    if (!data.has_more) break;
    startingAfter = data.data[data.data.length - 1].id;
    console.log(`  Fetched ${charges.length} charges so far...`);
  }

  return charges;
}

async function main() {
  const { start } = getDateRange();
  console.log(`Fetching Stripe charges since ${start}`);

  const charges = await fetchAllCharges(start);

  saveRaw('stripe-charges.json', charges);
  console.log(`Done: ${charges.length} charges saved`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
