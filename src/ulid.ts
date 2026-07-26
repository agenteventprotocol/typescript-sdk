// ULID (Crockford base32; 48-bit ms time + 80-bit random) on the WebCrypto
// RNG so it runs anywhere (AEP-0001 §4: `id` is a ULID).
const B32 = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

export function ulid(now: number = Date.now()): string {
  let t = now;
  let time = '';
  for (let i = 0; i < 10; i++) { time = B32.charAt(t % 32) + time; t = Math.floor(t / 32); }
  const rnd = crypto.getRandomValues(new Uint8Array(16));
  let rand = '';
  for (let i = 0; i < 16; i++) rand += B32.charAt((rnd[i] ?? 0) % 32);
  return time + rand;
}
