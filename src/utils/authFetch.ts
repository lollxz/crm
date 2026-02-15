export function authFetch(input: RequestInfo, init: RequestInit = {}) {
  const token = localStorage.getItem('token');
  let url = input as string;
  // If input is a relative path (starts with '/'), prefix with VITE_API_URL if set
  try {
    const isRelative = typeof url === 'string' && url.startsWith('/');
    // Use configured VITE_API_URL when present, otherwise fall back to the
    // backend IP so local builds without env config still call the right host.
    const base = import.meta.env.VITE_API_URL || 'https://conferencecare.org/api';
    if (isRelative && base) {
      // avoid double slash
      url = base.replace(/\/$/, '') + url;
    }
  } catch (e) {
    // ignore — if this fails the browser will attempt relative requests
  }

  const headers: Record<string, string> = {
    ...(init.headers as Record<string, string> || {}),
    'Authorization': token ? `Bearer ${token}` : '',
  };

  // If body present and no content-type, default to JSON — but do NOT set
  // Content-Type when the body is a FormData / URLSearchParams / Blob so the
  // browser can set the correct multipart boundary header.
  const method = (init.method || 'GET').toUpperCase();
  const bodyAny: any = init.body as any;
  const isFormData = (typeof FormData !== 'undefined') && (bodyAny instanceof FormData);
  const isURLSearchParams = (typeof URLSearchParams !== 'undefined') && (bodyAny instanceof URLSearchParams);
  const isBlob = (typeof Blob !== 'undefined') && (bodyAny instanceof Blob);
  const hasContentTypeHeader = !Object.keys(headers || {}).every(() => true) ? false : Object.keys(headers).some(h => h.toLowerCase() === 'content-type');
  // Fallback: if headers is empty object above returned false, compute properly
  // (above .every trick avoids TS complaints about possibly undefined)
  const computedHasContentType = Object.keys(headers).some(h => h.toLowerCase() === 'content-type');
  const finalHasContentType = hasContentTypeHeader || computedHasContentType;

  if (init.body && !finalHasContentType && ['POST','PUT','PATCH'].includes(method) && !isFormData && !isURLSearchParams && !isBlob) {
    headers['Content-Type'] = 'application/json';
  }

  console.debug('[authFetch] Request:', url, 'Token present:', !!token);
  return fetch(url, { ...init, headers });
}
