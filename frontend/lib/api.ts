const configuredBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL?.trim();
const isDevelopment = process.env.NODE_ENV !== 'production';

// Development defaults to local FastAPI; production defaults to the
// Vercel services backend route prefix declared in vercel.json.
const defaultBaseUrl = isDevelopment ? 'http://localhost:8000' : '/_/backend';

export const API_BASE_URL =
  configuredBaseUrl && configuredBaseUrl.length > 0
    ? configuredBaseUrl.replace(/\/+$/, '')
    : defaultBaseUrl;

export const buildApiUrl = (path: string): string => {
  if (path.startsWith('/')) {
    return `${API_BASE_URL}${path}`;
  }
  return `${API_BASE_URL}/${path}`;
};
