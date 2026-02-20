/**
 * Cloudflare Worker: Claude API proxy for Amberscript Dashboard chat.
 *
 * The Anthropic API key is stored as a Worker secret (ANTHROPIC_API_KEY).
 * Set it with: npx wrangler secret put ANTHROPIC_API_KEY
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
      const { messages, system } = body;

      if (!messages) {
        return new Response(JSON.stringify({ error: 'Missing messages' }), {
          status: 400,
          headers: { ...corsHeaders(origin), 'Content-Type': 'application/json' },
        });
      }

      const apiKey = env.ANTHROPIC_API_KEY;
      if (!apiKey) {
        return new Response(JSON.stringify({ error: 'API key not configured on server' }), {
          status: 500,
          headers: { ...corsHeaders(origin), 'Content-Type': 'application/json' },
        });
      }

      // Call Claude API
      const claudeRes = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': apiKey,
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
