/**
 * Fetch Stripe subscriptions for Amberscript.
 *
 * Two fetches:
 *   1. All currently active/trialing/past_due subs → MRR snapshot
 *   2. All subs created OR canceled in the date range → weekly new/churn tracking
 *
 * Outputs: raw/stripe-subs.json
 */

import { getDateRange, getWeekStart, getMonth, saveRaw, retry } from './utils.mjs';

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

/**
 * Paginate through all subscriptions matching params
 */
async function fetchSubs(params) {
  const subs = [];
  let startingAfter = undefined;

  while (true) {
    const p = { limit: 100, ...params };
    if (startingAfter) p.starting_after = startingAfter;

    const data = await retry(() => stripeGet('/subscriptions', p));
    subs.push(...data.data);

    if (!data.has_more) break;
    startingAfter = data.data[data.data.length - 1].id;
    console.log(`  Fetched ${subs.length} subscriptions so far...`);
  }

  return subs;
}

function ts(epoch) {
  if (!epoch) return null;
  return new Date(epoch * 1000).toISOString().slice(0, 10);
}

function getPlanType(sub) {
  const interval = sub.items?.data?.[0]?.price?.recurring?.interval || '';
  if (interval === 'year') return 'yearly';
  if (interval === 'month') return 'monthly';
  return 'other';
}

/**
 * Get monthly-equivalent amount for MRR calculation
 */
function getMonthlyAmount(sub) {
  const item = sub.items?.data?.[0];
  if (!item) return 0;
  const unitAmount = (item.price?.unit_amount || 0) / 100;
  const quantity = item.quantity || 1;
  const interval = item.price?.recurring?.interval || 'month';
  const intervalCount = item.price?.recurring?.interval_count || 1;
  const total = unitAmount * quantity;
  if (interval === 'year') return total / (12 * intervalCount);
  if (interval === 'month') return total / intervalCount;
  if (interval === 'week') return total * (52 / 12) / intervalCount;
  if (interval === 'day') return total * (365 / 12) / intervalCount;
  return total;
}

function mapSub(s) {
  const created = ts(s.created);
  const canceledAt = ts(s.canceled_at);
  return {
    id: s.id,
    status: s.status,
    created,
    createdWeek: created ? getWeekStart(created) : '',
    createdMonth: created ? getMonth(created) : '',
    startDate: ts(s.start_date),
    currentPeriodStart: ts(s.current_period_start),
    currentPeriodEnd: ts(s.current_period_end),
    canceledAt,
    canceledWeek: canceledAt ? getWeekStart(canceledAt) : '',
    canceledMonth: canceledAt ? getMonth(canceledAt) : '',
    endedAt: ts(s.ended_at),
    trialStart: ts(s.trial_start),
    trialEnd: ts(s.trial_end),
    planType: getPlanType(s),
    currency: (s.currency || 'eur').toUpperCase(),
    amount: (s.items?.data?.[0]?.price?.unit_amount || 0) / 100,
    quantity: s.items?.data?.[0]?.quantity || 1,
    interval: s.items?.data?.[0]?.price?.recurring?.interval || '',
    intervalCount: s.items?.data?.[0]?.price?.recurring?.interval_count || 1,
    monthlyAmount: Math.round(getMonthlyAmount(s) * 100) / 100,
    productName: s.items?.data?.[0]?.price?.nickname || '',
    productId: typeof s.items?.data?.[0]?.price?.product === 'string'
      ? s.items.data[0].price.product
      : s.items?.data?.[0]?.price?.product?.id || '',
    priceId: s.items?.data?.[0]?.price?.id || '',
    customerId: s.customer || '',
    metadata: s.metadata || {},
    country: s.metadata?.country || '',
  };
}

async function main() {
  const { start } = getDateRange();

  // 1. Fetch all currently active subscriptions (for MRR snapshot)
  console.log('Fetching active subscriptions...');
  const activeSubs = await fetchSubs({ status: 'active' });
  console.log(`  Active: ${activeSubs.length}`);

  const trialingSubs = await fetchSubs({ status: 'trialing' });
  console.log(`  Trialing: ${trialingSubs.length}`);

  const pastDueSubs = await fetchSubs({ status: 'past_due' });
  console.log(`  Past due: ${pastDueSubs.length}`);

  // 2. Fetch all subs created in date range (for new sub tracking)
  console.log(`Fetching subscriptions created since ${start}...`);
  const recentSubs = await fetchSubs({
    'created[gte]': Math.floor(new Date(start).getTime() / 1000),
    status: 'all',
  });
  console.log(`  Recent (all statuses): ${recentSubs.length}`);

  // 3. Fetch canceled subs in date range (for churn tracking)
  //    Stripe doesn't filter by canceled_at directly, so we get all canceled
  //    and filter client-side
  console.log('Fetching canceled subscriptions...');
  const canceledSubs = await fetchSubs({ status: 'canceled' });
  console.log(`  Canceled (all time): ${canceledSubs.length}`);

  // Deduplicate: merge all into a single map by ID
  const allMap = new Map();
  for (const s of [...activeSubs, ...trialingSubs, ...pastDueSubs, ...recentSubs, ...canceledSubs]) {
    allMap.set(s.id, s);
  }
  const allSubs = [...allMap.values()];
  console.log(`Total unique subscriptions: ${allSubs.length}`);

  // Resolve product names from product IDs
  const productIds = new Set();
  for (const s of allSubs) {
    const prod = s.items?.data?.[0]?.price?.product;
    if (typeof prod === 'string' && prod) productIds.add(prod);
  }
  const productNames = {};
  if (productIds.size > 0) {
    console.log(`Resolving ${productIds.size} product names...`);
    for (const pid of productIds) {
      try {
        const prod = await retry(() => stripeGet(`/products/${pid}`));
        productNames[pid] = prod.name || '';
      } catch (e) {
        console.warn(`  Could not fetch product ${pid}: ${e.message}`);
      }
    }
  }

  // Map to output format
  const mapped = allSubs.map(s => {
    const m = mapSub(s);
    // Enrich with resolved product name
    if (!m.productName && m.productId && productNames[m.productId]) {
      m.productName = productNames[m.productId];
    }
    return m;
  });

  // Compute MRR snapshot
  const activeStatuses = new Set(['active', 'trialing', 'past_due']);
  const activeMapped = mapped.filter(s => activeStatuses.has(s.status));
  const mrr = Math.round(activeMapped.reduce((sum, s) => sum + s.monthlyAmount, 0) * 100) / 100;
  const mrrByPlan = {};
  const mrrByCurrency = {};
  for (const s of activeMapped) {
    mrrByPlan[s.planType] = (mrrByPlan[s.planType] || 0) + s.monthlyAmount;
    mrrByCurrency[s.currency] = (mrrByCurrency[s.currency] || 0) + s.monthlyAmount;
  }
  // Round
  for (const k of Object.keys(mrrByPlan)) mrrByPlan[k] = Math.round(mrrByPlan[k] * 100) / 100;
  for (const k of Object.keys(mrrByCurrency)) mrrByCurrency[k] = Math.round(mrrByCurrency[k] * 100) / 100;

  const output = {
    fetchedAt: new Date().toISOString(),
    snapshot: {
      activeSubs: activeMapped.filter(s => s.status === 'active').length,
      trialingSubs: activeMapped.filter(s => s.status === 'trialing').length,
      pastDueSubs: activeMapped.filter(s => s.status === 'past_due').length,
      totalActiveSubs: activeMapped.length,
      mrr,
      mrrByPlan,
      mrrByCurrency,
    },
    subscriptions: mapped,
  };

  saveRaw('stripe-subs.json', output);
  console.log(`Done: ${mapped.length} subscriptions saved`);
  console.log(`MRR snapshot: ${mrr} EUR (${activeMapped.length} active subs)`);
  console.log(`  By plan: ${JSON.stringify(mrrByPlan)}`);
  console.log(`  By currency: ${JSON.stringify(mrrByCurrency)}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
