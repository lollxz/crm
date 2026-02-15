/**
 * API Configuration Utility
 * 
 * This utility helps check API connectivity and automatically switch 
 * between HTTP and HTTPS protocols if needed.
 */

import { authFetch } from '@/utils/authFetch';

// Remove the custom ImportMeta.env declaration and use the standard Vite ImportMetaEnv type

// Get the current API URL from environment
const currentApiUrl: string = import.meta.env.VITE_API_URL || '';

// Function to check if an API endpoint is accessible
async function checkApiConnection(url: string, token?: string): Promise<boolean> {
  try {
    console.log(`Testing API connection to: ${url}`);
    const response = await authFetch(url, {
      method: 'HEAD',
      headers: token ? {
        'Authorization': `Bearer ${token}`
      } : {
        'Content-Type': 'application/json'
      },
      // Short timeout to avoid long waits
      signal: AbortSignal.timeout(10000),
      // Allow self-signed certificates for development
      mode: 'cors'
    });
    
    return response.ok;
  } catch (error) {
    console.error(`Connection to ${url} failed:`, error);
    return false;
  }
}

// Function to try alternative protocol if the current one fails
async function tryAlternativeProtocol(token?: string): Promise<string | null> {
  if (!currentApiUrl) return null;
  
  const isHttps = currentApiUrl.startsWith('https://');
  const alternativeUrl = isHttps 
    ? currentApiUrl.replace('https://', 'http://') 
    : currentApiUrl.replace('http://', 'https://');
  
  console.log(`Trying alternative protocol: ${alternativeUrl}`);
  
  const isAlternativeWorking = await checkApiConnection(`${alternativeUrl}/tasks`, token);
  
  if (isAlternativeWorking) {
    console.log(`Alternative protocol works! Using: ${alternativeUrl}`);
    // Store the working URL in session storage for this session
    sessionStorage.setItem('workingApiUrl', alternativeUrl);
    return alternativeUrl;
  }
  
  return null;
}

// Function to get the best working API URL
async function getBestApiUrl(token?: string): Promise<string> {
  // First check if we have a working URL in session storage
  const cachedUrl = sessionStorage.getItem('workingApiUrl');
  if (cachedUrl) {
    console.log(`Using cached working API URL: ${cachedUrl}`);
    return cachedUrl;
  }
  
  // Check if current URL works
  const isCurrentWorking = await checkApiConnection(`${currentApiUrl}/tasks`, token);
  
  if (isCurrentWorking) {
    console.log(`Current API URL works: ${currentApiUrl}`);
    return currentApiUrl;
  }
  
  // Try alternative protocol
  const alternativeUrl = await tryAlternativeProtocol(token);
  if (alternativeUrl) {
    return alternativeUrl;
  }
  
  // If nothing works, return the original URL
  console.log(`No working API URL found, using default: ${currentApiUrl}`);
  return currentApiUrl;
}

export { 
  getBestApiUrl, 
  checkApiConnection,
  currentApiUrl
}; 