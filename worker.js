/**
 * Cloudflare Worker: Claude API proxy for Amberscript Dashboard chat.
 *
 * Deploy:
 *   1. Go to Cloudflare Dashboard > Workers & Pages > Create
 *   2. Name it "dashboard-chat" (or whatever you like)
 *   3. Paste this code
 *   4. Add a custom domain: dashboard-chat.amberscript.com
 *      (or update CHAT_WORKER_URL in index.html to match your worker URL)
 *
 * The API key is passed from the client (stored in their browser localStorage).
 * No secrets needed on the worker itself.
 */

const ALLOWED_ORIGINS = [
  'https://dashboard.amberscript.com',
  'http://dashboard.amberscript.com',
  'https://markvdeng.github.io',
];

function corsHeaders(origin) {
  const allowed = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return {
    'Access-Control-Allow-Origin': allowed,
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };
}

export default {
  async fetch(request, env) {
    const origin = request.headers.get('Origin') || '';

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(origin) });
    }

    if (request.method !== 'POST') {
      return new Response('Method not allowed', { status: 405, headers: corsHeaders(origin) });
    }

    try {
      const body = await request.json();
      const { key, messages, system } = body;

      if (!key || !messages) {
        return new Response(JSON.stringify({ error: 'Missing key or messages' }), {
          status: 400,
          headers: { ...corsHeaders(origin), 'Content-Type': 'application/json' },
        });
      }

      // Call Claude API
      const claudeRes = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': key,
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model: 'claude-haiku-4-5-20251001',
          max_tokens: 1024,
          system: system || 'You are a helpful data analyst.',
          messages,
        }),
      });

      if (!claudeRes.ok) {
        const err = await claudeRes.text();
        return new Response(JSON.stringify({ error: err }), {
          status: claudeRes.status,
          headers: { ...corsHeaders(origin), 'Content-Type': 'application/json' },
        });
      }

      const claudeData = await claudeRes.json();
      const content = claudeData.content?.[0]?.text || '';

      return new Response(JSON.stringify({ content }), {
        headers: { ...corsHeaders(origin), 'Content-Type': 'application/json' },
      });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), {
        status: 500,
        headers: { ...corsHeaders(origin), 'Content-Type': 'application/json' },
      });
    }
  },
};
