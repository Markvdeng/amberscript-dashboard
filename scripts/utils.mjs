import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RAW_DIR = join(__dirname, '..', 'raw');

/**
 * Get the Monday of the ISO week containing a date
 */
export function getWeekStart(date) {
  const d = new Date(date);
  const day = d.getUTCDay();
  const diff = d.getUTCDate() - day + (day === 0 ? -6 : 1);
  d.setUTCDate(diff);
  return formatDate(d);
}

/**
 * Format date as YYYY-MM-DD
 */
export function formatDate(date) {
  const d = new Date(date);
  return d.toISOString().slice(0, 10);
}

/**
 * Get date range: last N days from today
 */
export function getDateRange(daysBack = 90) {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - daysBack);
  return {
    start: formatDate(start),
    end: formatDate(end),
  };
}

/**
 * Get month string YYYY-MM from a date
 */
export function getMonth(dateStr) {
  return dateStr.slice(0, 7);
}

/**
 * Save raw fetched data to raw/ directory
 */
export function saveRaw(filename, data) {
  mkdirSync(RAW_DIR, { recursive: true });
  writeFileSync(join(RAW_DIR, filename), JSON.stringify(data, null, 2));
  console.log(`Saved raw/${filename}`);
}

/**
 * Load raw data from raw/ directory
 */
export function loadRaw(filename) {
  const path = join(RAW_DIR, filename);
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, 'utf8'));
}

/**
 * Retry a function with exponential backoff
 */
export async function retry(fn, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await fn();
    } catch (err) {
      if (i === maxRetries - 1) throw err;
      const wait = Math.pow(2, i) * 1000;
      console.warn(`Retry ${i + 1}/${maxRetries} after ${wait}ms: ${err.message}`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
}

/**
 * Google Ads account map
 */
export const GOOGLE_ADS_ACCOUNTS = {
  'NL/DE': '3243863045',
  'FR': '1161689617',
  'WW': '5291727311',
  'IT': '4116625392',
  'AmberNotes': '6307359695',
};

export const GOOGLE_ADS_MCC = '2439612811';

/**
 * Determine product type from campaign name
 * Machine-Made campaigns typically contain "MM", "ASR", "Machine", "Automated"
 * Human-Made campaigns typically contain "HM", "Human", "Professional"
 */
export function getProductType(campaignName) {
  const lower = (campaignName || '').toLowerCase();
  if (lower.includes('ambernotes') || lower.includes('amber notes')) return 'AmberNotes';
  if (lower.match(/\bhm\b/) || lower.includes('human') || lower.includes('professional')) return 'Human-Made';
  if (lower.match(/\bmm\b/) || lower.includes('machine') || lower.includes('asr') || lower.includes('automat')) return 'Machine-Made';
  return 'Other';
}

/**
 * Classify channel from source/medium or UTM
 */
export function classifyChannel(source, medium) {
  const src = (source || '').toLowerCase();
  const med = (medium || '').toLowerCase();

  if (med.includes('cpc') || med.includes('ppc') || med.includes('paid')) {
    if (src.includes('brand') || med.includes('brand')) return 'Paid Search Brand';
    return 'Paid Search Generic';
  }
  if (med === 'organic' || med === 'seo') return 'Organic';
  if (src === '(direct)' || src === 'direct' || (!src && !med)) return 'Direct';
  if (med === 'referral') return 'Referral';
  if (med === 'email') return 'Email';
  return 'Other';
}

/**
 * Map country codes to geo groups
 */
export function classifyGeo(country) {
  const c = (country || '').toUpperCase();
  if (c === 'NL' || c === 'NETHERLANDS') return 'NL';
  if (c === 'DE' || c === 'GERMANY') return 'DE';
  if (c === 'CH' || c === 'SWITZERLAND') return 'CH';
  if (c === 'AT' || c === 'AUSTRIA') return 'AT';
  if (c === 'FR' || c === 'FRANCE') return 'FR';
  if (c === 'IT' || c === 'ITALY') return 'IT';
  return 'Other';
}
