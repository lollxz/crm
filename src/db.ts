// Add this at the top of the file to fix the linter error for import.meta.env
declare global {
  interface ImportMetaEnv {
    VITE_API_URL: string;
    [key: string]: any;
  }
  interface ImportMeta {
    env: ImportMetaEnv;
  }
}

import { Event, CampaignContact, ContactRelationsData, ContactEventRelation } from './types';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

// Ensure we have a reliable backend base URL. If VITE_API_URL is not set in
// the environment (common in some dev/prod setups), fall back to the
// backend IP so the frontend doesn't try to call the page origin.
const API_URL = import.meta.env.VITE_API_URL || 'https://www.conferencecare.org/api';

const getAuthHeaders = () => {
  const token = localStorage.getItem('token');
  return {
    'Authorization': `Bearer ${token}`,
    'Accept': 'application/json',
    'Content-Type': 'application/json'
  };
};

const handleResponse = async (response: Response) => {
  if (!response.ok) {
    const errorText = await response.text();
    let errorMessage;
    try {
      const errorJson = JSON.parse(errorText);
      errorMessage = errorJson.message || errorJson.error || errorJson.detail || 'An error occurred';
    } catch {
      errorMessage = errorText || `HTTP error ${response.status}`;
    }
    throw new Error(errorMessage);
  }
  const data = await response.json();
  
  if (Array.isArray(data.customers)) {
    return data.customers.map((customer: any) => ({
      ...customer,
      nationality: customer.Nationality || customer.nationality || '',
      payment_method: customer["Payment Method"] || customer.payment_method || '',
      workplace: customer.Workplace || customer.workplace || '',
      speaker_type: customer["Speaker Type"] || customer.speaker_type || '',
      supplier: customer.supplier || '',
      sending_time: customer.sending_time || new Date().toISOString()
    }));
  } else if (data.customers) {
    return {
      ...data.customers,
      nationality: data.customers.Nationality || data.customers.nationality || '',
      payment_method: data.customers["Payment Method"] || data.customers.payment_method || '',
      workplace: data.customers.Workplace || data.customers.workplace || '',
      speaker_type: data.customers["Speaker Type"] || data.customers.speaker_type || '',
      supplier: data.customers.supplier || '',
      sending_time: data.customers.sending_time || new Date().toISOString()
    };
  }
  
  return data.matches || data.customers || [];
};

export const initDatabase = async () => {
  try {
    const response = await fetch(`${API_URL}/customers`, {
      headers: getAuthHeaders()
    });
    return response.ok;
  } catch (error) {
    console.error('Database initialization error:', error);
    throw new Error('Failed to connect to the API server');
  }
};

// Global fetch wrapper to handle 401/403 and auto-logout
async function authFetch(input: RequestInfo, init?: RequestInit) {
  const response = await fetch(input, init);
  if (response.status === 401 || response.status === 403) {
    localStorage.removeItem('token');
    localStorage.removeItem('user');
    window.location.href = '/login';
    throw new Error('Session expired. Please log in again.');
  }
  return response;
}

// Update getAllEvents to use authFetch
export const getAllEvents = async (): Promise<Event[]> => {
  const headers = getAuthHeaders();
  console.log('getAllEvents headers:', headers);
  const response = await authFetch(`${API_URL}/events`, { headers });
  const data = await response.json();
  
  // Sanitize events data to prevent date formatting errors
  const sanitizedEvents = (data.events || []).map((event: any) => ({
    ...event,
    date2: event.date2 && event.date2 !== 'null' && event.date2 !== 'undefined' ? event.date2 : null
  }));
  
  console.log('Sanitized events data:', sanitizedEvents);
  return sanitizedEvents;
};

export const getContactsForEvent = async (event_id: number): Promise<CampaignContact[]> => {
  const response = await fetch(`${API_URL}/campaign_contacts/search?event_id=${event_id}`, { headers: getAuthHeaders() });
  const data = await response.json();
  return data.contacts || [];
};

export const searchContactsByQuery = async (event_id: number, query: string): Promise<CampaignContact[]> => {
  const response = await fetch(`${API_URL}/campaign_contacts/search?event_id=${event_id}&query=${encodeURIComponent(query)}`, { headers: getAuthHeaders() });
  const data = await response.json();
  return data.results || data.contacts || [];
};

export const addContact = async (contact: Partial<CampaignContact>) => {
  const response = await fetch(`${API_URL}/campaign_contacts`, {
      method: 'POST',
      headers: getAuthHeaders(),
    body: JSON.stringify(contact),
    });
    return handleResponse(response);
};

export const updateContact = async (contact: CampaignContact) => {
  const response = await fetch(`${API_URL}/campaign_contacts/${contact.id}`, {
      method: 'PUT',
      headers: getAuthHeaders(),
    body: JSON.stringify(contact),
    });
    return handleResponse(response);
};

export const patchContact = async (contactId: number, partial: Partial<CampaignContact>) => {
  const response = await fetch(`${API_URL}/campaign_contacts/${contactId}`, {
    method: 'PATCH',
    headers: getAuthHeaders(),
    body: JSON.stringify(partial),
  });
  return handleResponse(response);
};

export const deleteContact = async (id: number) => {
  const response = await fetch(`${API_URL}/campaign_contacts/${id}`, {
      method: 'DELETE',
    headers: getAuthHeaders(),
    });
    return handleResponse(response);
};

export const pauseCampaignContact = async (id: number) => {
  const response = await fetch(`${API_URL}/campaign_contacts/${id}/pause`, {
      method: 'POST',
      headers: getAuthHeaders(),
  });
  return handleResponse(response);
};

export const resumeCampaignContact = async (id: number) => {
  const response = await fetch(`${API_URL}/campaign_contacts/${id}/resume`, {
    method: 'POST',
    headers: getAuthHeaders(),
  });
  return handleResponse(response);
};

export const uploadContactsExcel = async (file: File, commit: boolean = false) => {
  const token = localStorage.getItem('token');
  const formData = new FormData();
  formData.append('file', file); // Only append the file

  // First, ask the server to run a preview (validation + duplicate detection)
  const previewResp = await fetch(`${API_URL}/campaign_contacts/upload-excel?preview=true`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`
      // Do NOT set 'Content-Type' here!
    },
    body: formData,
  });

  // If preview failed, throw so callers can decide whether to proceed
  if (!previewResp.ok) {
    const text = await previewResp.text();
    let errMsg = text;
    try { errMsg = JSON.parse(text).detail || JSON.parse(text).message || text; } catch {}
    throw new Error(`Preview failed: ${errMsg}`);
  }

  const previewJson = await previewResp.json();

  // If caller only wanted preview, return it
  if (!commit) return previewJson;

  // Caller requested commit -> perform the real upload (persist)
  const commitResp = await fetch(`${API_URL}/campaign_contacts/upload-excel`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`
    },
    body: formData,
  });

  return handleResponse(commitResp);
};

export const startCampaign = async (params: any) => {
  console.log('[startCampaign] Sending payload:', JSON.stringify(params, null, 2));
  
  // If only contact_ids is present, send as a raw array (bulk)
  if (params && Array.isArray(params.contact_ids) && Object.keys(params).length === 1) {
    console.log('[startCampaign] Detected contact_ids array, sending contact_ids only');
    const response = await fetch(`${API_URL}/email_queue/bulk-start-campaign`, {
      method: 'POST',
      headers: getAuthHeaders(),
      body: JSON.stringify(params.contact_ids)
    });
    return handleResponse(response);
  }
  // Otherwise, send the full payload (array of campaign objects with contact_id, subject, body, etc.)
  console.log('[startCampaign] Sending full payload as array');
  const response = await fetch(`${API_URL}/email_queue/bulk-start-campaign`, {
    method: 'POST',
    headers: getAuthHeaders(),
    body: JSON.stringify(params)
    });
    return handleResponse(response);
};

export const handleExcelUpload = async (data: Partial<CampaignContact>[]) => {
  try {
    const response = await fetch(`${API_URL}/customers/upload-excel`, {
      method: 'POST',
      headers: getAuthHeaders(),
      body: JSON.stringify({ customers: data }), // <-- Fix key here
    });
    
    const result = await handleResponse(response);
    
    if (result.skipped_count > 0) {
      toast.success(`${result.saved_count} customers saved, ${result.skipped_count} skipped`);
    } else {
      toast.success(`${result.saved_count} customers saved successfully`);
    }
    
    return result;
  } catch (err: any) {
    console.error('Excel upload error:', err);
    throw err;
  }
};
export const getContactRelations = async (contactId: number): Promise<ContactRelationsData> => {
  const headers = getAuthHeaders();
  const response = await authFetch(`${API_URL}/campaign_contacts/${contactId}/relations`, { headers });
  if (!response.ok) {
    throw new Error('Failed to fetch contact relations');
  }
  return response.json();
};

export const getEmailRelations = async (email: string): Promise<ContactEventRelation[]> => {
  const headers = getAuthHeaders();
  const response = await authFetch(
    `${API_URL}/campaign_contacts/email-relations/${encodeURIComponent(email)}`,
    { headers }
  );
  if (!response.ok) {
    throw new Error('Failed to fetch email relations');
  }
  return response.json();
};
