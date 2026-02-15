import React, { useEffect, useState, useMemo, ChangeEvent } from 'react';
import { CampaignContact, ContactRelationsData, ContactEventRelation } from '../types';
import CustomFlowManager from './CustomFlowManager';
import CustomFlowModal from './CustomFlowModal.tsx';
import { CustomMessagesModal } from './CustomMessagesModal';
import { authFetch } from '../utils/authFetch';

// Extend CampaignContact type to include optional attachment
interface ExtendedCampaignContact extends CampaignContact {
  attachment?: File | null;
  cc_store?: string | null;
  invoice_number?: string | null;
  email_error?: string | null;
  last_error_at?: string | null;
}
import { toast } from 'react-toastify';
import { getContactsForEvent, searchContactsByQuery, patchContact, deleteContact, pauseCampaignContact, resumeCampaignContact, startCampaign, uploadContactsExcel, getContactRelations } from '../db';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { ExcelUpload } from './ExcelUpload';
import { DEFAULT_STATUS_OPTIONS, STAGE_OPTIONS } from '../types';
import { useAuth } from '../contexts/AuthContext';

interface ContactsTableProps {
  eventId: number;
  onRefresh: () => void;
  globalSearch?: string;
  onSingleCampaign?: (contact: CampaignContact) => void;
  onUpload?: (file: File) => Promise<void>;
}

type BulkFields = { [id: number]: { forms_link: string; payment_link: string; attachment: File | null; attachmentBase64: string | null; invoice_number?: string } };

function getStageColor(stage: string) {
  const stageLower = (stage || '').toLowerCase();
  if (stageLower === 'n/a') {
    return 'text-gray-600';
  }
  switch (stageLower) {
    case 'initial':
      return 'bg-[#b2cef3] text-black';
    case 'forms':
      return 'bg-[#1967B2] text-white';
    case 'payments':
      return 'bg-[#dcffd4] text-black';
    case 'sepa':
    case 'sepa bt payment':
    case 'sepa bt':
      return 'bg-[#DDEBF7] text-black';
    case 'rh':
    case 'rh bt payment':
    case 'rh bt':
      return 'bg-[#FFEFD5] text-black';
    case 'invoice & confirmation':
      return 'bg-[#546612] text-white';
    case 'payment due':
      return 'bg-[#c3e645] text-white';
    case 'completed':
      return 'bg-[#28A745] text-white';
    case 'problem':
      return 'bg-[#FF0000] text-white';
    case 'wrong person':
      return 'bg-[#d16afc] text-white';
    case 'mail delivery':
      return 'bg-[#ababab] text-white';
    case 'hcn':
      return 'bg-[#FFC8DD] text-black';
    case "supplier's payment":
      return 'bg-[#E65C9C] text-white';
    default:
      return 'bg-blue-100 text-blue-800';
  }
}

function getStatusColor(status: string) {
  const statusLower = (status || '').toLowerCase();
  if (statusLower === 'n/a') return 'text-gray-600';

  // Treat various reminder-like tokens as the same category
  if (statusLower.includes('reminder') || statusLower.includes('_reminder') || statusLower.includes('initial_sent')) {
    return 'bg-[#FF8800] text-white';
  }

  switch (statusLower) {
    case 'no response':
      return 'bg-[#a36c50] text-white';
    case 'not interested':
      return 'bg-[#ffff00] text-black';
    case 'covered':
      return 'bg-[#000000] text-white';
    case 'follow-up':
      return 'bg-[#FF8800] text-white';
    case 'completed':
      return 'bg-[#28A745] text-white';
    case 'cancelled':
      return 'bg-[#FF0000] text-white';
    case 'pending':
      return 'bg-[#526FD0] text-white';
    default:
      return 'bg-gray-100 text-gray-800';
  }
}

// Small list of common countries for nationality dropdown. Add more if needed.
const COUNTRIES = [
  'Afghanistan','Albania','Algeria','Andorra','Angola','Antigua and Barbuda','Argentina','Armenia','Australia','Austria',
  'Azerbaijan','Bahamas','Bahrain','Bangladesh','Barbados','Belarus','Belgium','Belize','Benin','Bhutan',
  'Bolivia','Bosnia and Herzegovina','Botswana','Brazil','Brunei','Bulgaria','Burkina Faso','Burundi','Cambodia','Cameroon',
  'Canada','Cape Verde','Central African Republic','Chad','Chile','China','Colombia','Comoros','Congo','Costa Rica',
  'Croatia','Cuba','Cyprus','Czech Republic','Czechia','Denmark','Djibouti','Dominica','Dominican Republic','East Timor',
  'Ecuador','Egypt','El Salvador','Equatorial Guinea','Eritrea','Estonia','Eswatini','Ethiopia','Fiji','Finland',
  'France','Gabon','Gambia','Georgia','Germany','Ghana','Gibraltar','Greece','Grenada','Guatemala',
  'Guinea','Guinea-Bissau','Guyana','Haiti','Honduras','Hong Kong','Hungary','Iceland','India','Indonesia',
  'Iran','Iraq','Ireland','Israel','Italy','Ivory Coast','Jamaica','Japan','Jordan','Kazakhstan',
  'Kenya','Kiribati','Kosovo','Kuwait','Kyrgyzstan','Laos','Latvia','Lebanon','Lesotho','Liberia',
  'Libya','Liechtenstein','Lithuania','Luxembourg','Macau','Madagascar','Malawi','Malaysia','Maldives','Mali',
  'Malta','Marshall Islands','Mauritania','Mauritius','Mexico','Micronesia','Moldova','Monaco','Mongolia','Montenegro',
  'Morocco','Mozambique','Myanmar','Namibia','Nauru','Nepal','Netherlands','New Zealand','Nicaragua','Niger',
  'Nigeria','North Korea','North Macedonia','Norway','Oman','Pakistan','Palau','Palestine','Panama','Papua New Guinea',
  'Paraguay','Peru','Philippines','Poland','Portugal','Qatar','Romania','Russia','Rwanda','Saint Kitts and Nevis',
  'Saint Lucia','Saint Vincent and the Grenadines','Samoa','San Marino','Sao Tome and Principe','Saudi Arabia','Senegal','Serbia','Seychelles','Sierra Leone',
  'Singapore','Slovakia','Slovenia','Solomon Islands','Somalia','South Africa','South Korea','South Sudan','Spain','Sri Lanka',
  'Sudan','Suriname','Sweden','Switzerland','Syria','Taiwan','Tajikistan','Tanzania','Thailand','Timor-Leste',
  'Togo','Tonga','Trinidad and Tobago','Tunisia','Turkey','Turkmenistan','Tuvalu','Uganda','Ukraine','United Arab Emirates',
  'United Kingdom','United States','Uruguay','Uzbekistan','Vanuatu','Vatican City','Venezuela','Vietnam','Yemen','Zambia','Zimbabwe','Other'
];

// RelationsCell Component - AUTOMATIC LOADING & FILTERED
const RelationsCell = ({ contact }: { contact: ExtendedCampaignContact }) => {
  const [relationsData, setRelationsData] = useState<ContactRelationsData | null>(null);
  const [showModal, setShowModal] = useState(false);
  const [isLoading, setIsLoading] = useState(true); 

  // EFFECT: Fetch data automatically on mount
  useEffect(() => {
    let isMounted = true;

    const fetchRelations = async () => {
      try {
        const data = await getContactRelations(contact.id);
        
        if (isMounted) {
          // --- FILTERING LOGIC ---
          // We remove the event that matches the current contact's event_id
          // so we don't show the user the row they are already looking at.
          const otherEvents = data.relations.filter(
            (r: ContactEventRelation) => r.event_id !== contact.event_id
          );

          // Update the data state with the filtered list and new count
          setRelationsData({
            ...data,
            relations: otherEvents,
            total_events: otherEvents.length
          });
        }
      } catch (err) {
        console.error('Failed to load relations', err);
      } finally {
        if (isMounted) {
          setIsLoading(false);
        }
      }
    };

    fetchRelations();

    return () => { isMounted = false; };
  }, [contact.id, contact.event_id]); // Added contact.event_id to dependencies


  // --- RENDER STATE 1: Loading ---
  if (isLoading) {
    return (
      <div className="animate-pulse flex space-x-2 items-center">
        <div className="h-2 w-16 bg-gray-200 rounded"></div>
      </div>
    );
  }

  // --- RENDER STATE 2: Error or No Data ---
  if (!relationsData) {
    return <span className="text-gray-300 text-xs">Error</span>;
  }

  // --- RENDER STATE 3: 0 Matches (After filtering) ---
  if (relationsData.total_events === 0) {
    return <span className="text-gray-400 text-xs italic">None</span>;
  }

  // --- RENDER STATE 4: Exactly 1 OTHER Match (Show Inline) ---
  if (relationsData.total_events === 1) {
    const singleEvent = relationsData.relations[0];
    return (
      <div className="flex flex-col text-xs leading-snug">
        <span 
          className="font-semibold text-gray-700 truncate max-w-[150px]" 
          title={singleEvent.event_name}
        >
          {singleEvent.event_name}
        </span>
        <span className="text-gray-500 text-[11px]">
          {singleEvent.stage || '-'} • {singleEvent.status || '-'}
        </span>
      </div>
    );
  }

  // --- RENDER STATE 5: Multiple Matches (Badge + Modal Trigger) ---
  return (
    <>
      <div className="flex items-center gap-2">
        <Badge
          className="cursor-pointer hover:opacity-90 transition-opacity"
          onClick={() => setShowModal(true)}
          style={{
            backgroundColor: '#ff6b6b',
            color: '#ffffff'
          }}
        >
          {relationsData.total_events} Relations
        </Badge>
        
        <button
          onClick={() => setShowModal(true)}
          className="text-blue-600 hover:text-blue-800 text-xs font-bold bg-blue-50 px-2 py-1 rounded hover:bg-blue-100 transition-colors"
        >
          View ⤢
        </button>
      </div>

      {/* Relations Modal */}
      <Dialog open={showModal} onOpenChange={setShowModal}>
        <DialogContent className="w-full max-w-3xl">
          <DialogHeader>
            <DialogTitle>
              Other Events for {relationsData.email}
            </DialogTitle>
          </DialogHeader>

          <div className="max-h-[60vh] overflow-y-auto border rounded-md">
            <table className="w-full border-collapse text-sm text-left">
              <thead className="sticky top-0 bg-gray-100 text-gray-700 font-semibold z-10 shadow-sm">
                <tr>
                  <th className="px-4 py-3 border-b">Event Name</th>
                  <th className="px-4 py-3 border-b">Stage</th>
                  <th className="px-4 py-3 border-b">Status</th>
                  <th className="px-4 py-3 border-b text-right">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {relationsData.relations.map((relation: ContactEventRelation, idx: number) => (
                  <tr key={idx} className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-3">
                      <div className="font-medium text-gray-900">{relation.event_name}</div>
                      <div className="text-xs text-gray-400">ID: {relation.event_id}</div>
                    </td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                         relation.status === 'Won' ? 'bg-green-100 text-green-800' : 
                         relation.status === 'Lost' ? 'bg-red-100 text-red-800' : 
                         'bg-blue-100 text-blue-800'
                       }`}>
                        {relation.status || 'N/A'}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
                        {relation.stage || 'N/A'}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-right">
                      <Button
                        size="sm"
                        variant="ghost"
                        className="text-indigo-600 hover:text-indigo-900 hover:bg-indigo-50"
                        onClick={() => {
                          window.location.href = `/events/${relation.event_id}`;
                        }}
                      >
                        Go to Event →
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
};

export function ContactsTable({ eventId, onRefresh, globalSearch, onSingleCampaign, onUpload }: ContactsTableProps) {
  const [contacts, setContacts] = useState<ExtendedCampaignContact[]>([]);
  const { user } = useAuth();
  const [isLoading, setIsLoading] = useState(true);
  // Modal-based edit (open with pen icon)
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [editContactData, setEditContactData] = useState<ExtendedCampaignContact | null>(null);
  const [selected, setSelected] = useState<{ [id: number]: boolean }>({});
  const [campaignModal, setCampaignModal] = useState(false);
  const [linksModal, setLinksModal] = useState(false);
  const [currentContact, setCurrentContact] = useState<ExtendedCampaignContact | null>(null);
  const [campaignSubject, setCampaignSubject] = useState('');
  const [campaignMessage, setCampaignMessage] = useState('');
  const [isCampaignLoading, setIsCampaignLoading] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  // Custom Flow modal state
  const [customFlowModalOpen, setCustomFlowModalOpen] = useState(false);
  const [modalContactId, setModalContactId] = useState<number | null>(null);
  const [modalFlowId, setModalFlowId] = useState<number | undefined>(undefined);
  const [modalInitialSteps, setModalInitialSteps] = useState<any[]>([]);
  // Custom Messages modal state
  const [customMessagesModalOpen, setCustomMessagesModalOpen] = useState(false);
  const [messagesModalContactId, setMessagesModalContactId] = useState<number | null>(null);
  const [messagesModalContactName, setMessagesModalContactName] = useState('');

  // Add these at the top-level of the component
  const [defaultSubject, setDefaultSubject] = useState('');
  const [defaultMessage, setDefaultMessage] = useState('');
  const [bulkFields, setBulkFields] = useState<BulkFields>({});
  const [showFullTrigger, setShowFullTrigger] = useState<{ [id: number]: boolean }>({});
  const [jobProgress, setJobProgress] = useState<{ jobId: string | null; status: string; total: number; processed: number }>({ jobId: null, status: 'idle', total: 0, processed: 0 });
  const [progressModalOpen, setProgressModalOpen] = useState(false);

  // Normalize cc_history for the edit modal to avoid runtime errors when the
  // backend returns a string or JSON-like value instead of an array.
  const editCcHistory = useMemo(() => {
    if (!editContactData) return [] as any[];
    const raw: any = (editContactData as any).cc_history;
    if (!raw) return [] as any[];
    if (Array.isArray(raw)) return raw;
    if (typeof raw === 'string') {
      try {
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
      } catch (e) {
        return [] as any[];
      }
    }
    return [] as any[];
  }, [editContactData]);

  useEffect(() => {
  const fetchContacts = async () => {
      setIsLoading(true);
      try {
        if (globalSearch && globalSearch.trim()) {
          // If there's a search query, use the search API
          const data = await searchContactsByQuery(eventId, globalSearch);
          setContacts(data);
        } else {
          // Otherwise, load all contacts for the event
          const data = await getContactsForEvent(eventId);
          setContacts(data);
        }
      } catch (err) {
        console.error('Failed to load contacts:', err);
        toast.error('Failed to load contacts');
      } finally {
        setIsLoading(false);
      }
    };
    fetchContacts();
  }, [eventId, globalSearch]);

  useEffect(() => {
    // Fetch template files from public folder at runtime
    fetch('/templates/emails/campaign_default_subject.txt')
      .then(res => res.text())
      .then(setDefaultSubject)
      .catch(() => setDefaultSubject('Accommodation in {{city}} - {{month}} - {{venue}}'));
    fetch('/templates/emails/campaign_default_body.txt')
      .then(res => res.text())
      .then(setDefaultMessage)
      .catch(() => setDefaultMessage('Dear {{name}},\nWe are pleased to invite you to {{event_name}} in {{city}}...'));
  }, []);

  // Update bulkFields when selection changes
  useEffect(() => {
    const contact_ids = Object.keys(selected).filter(id => selected[+id]).map(Number);
    setBulkFields((prev: BulkFields) => {
      const updated: BulkFields = { ...prev };
      contact_ids.forEach(id => {
        if (!updated[id]) {
          const c = contacts.find(x => x.id === id);
          updated[id] = {
            forms_link: c?.forms_link || '',
            payment_link: c?.payment_link || '',
            attachment: null,
            attachmentBase64: null,
            invoice_number: (c as any)?.invoice_number || ''
          };
        }
      });
      // Remove unselected
      Object.keys(updated).forEach(id => {
        if (!contact_ids.includes(Number(id))) delete updated[Number(id)];
      });
      return updated;
    });
  }, [selected, contacts]);

  // Handle per-contact field changes
  const handleBulkFieldChange = (id: number, field: 'forms_link' | 'payment_link', value: string) => {
    setBulkFields((prev: BulkFields) => ({ ...prev, [id]: { ...prev[id], [field]: value } }));
  };
  const handleBulkInvoiceChange = (id: number, value: string) => {
    setBulkFields((prev: BulkFields) => ({ ...prev, [id]: { ...prev[id], invoice_number: value } }));
  };
  const handleBulkAttachment = (id: number, file: File | null) => {
    if (!file) {
      setBulkFields((prev: BulkFields) => ({ ...prev, [id]: { ...prev[id], attachment: null, attachmentBase64: null } }));
      return;
    }
    const reader = new FileReader();
    reader.onload = e => {
      setBulkFields((prev: BulkFields) => ({ ...prev, [id]: { ...prev[id], attachment: file, attachmentBase64: e.target?.result as string } }));
    };
    reader.readAsDataURL(file);
  };

  // legacy inline edit removed; edits now through modal

  // Modal save: user confirms edits to the whole contact
  const handleModalSave = async () => {
    if (!editContactData) return;
    try {
      // Find original contact to compute diff
      const original = contacts.find(c => c.id === editContactData.id);
      if (!original) throw new Error('Original contact not found');
      // Normalize email and cc_store: if email field contains commas, first is main, rest go to cc_store
      const normalizeEmails = (emailStr: string | undefined, existingCc: string | undefined) => {
        const parts = (emailStr || '').split(',').map(s => s.trim()).filter(Boolean);
        const main = parts.length ? parts[0] : '';
        const remainder = parts.length > 1 ? parts.slice(1).join(', ') : (existingCc || '');
        return { main, remainder };
      };

      const emailField = editContactData.email || '';
      const { main } = normalizeEmails(emailField, (original as any).cc_store || '');

      const normalized: any = { ...editContactData };
      // Ensure campaign_paused is normalized to a boolean and remove possible alias fields
      if (normalized.campaign_paused === 'pause' || normalized.campaign_paused === 'paused') normalized.campaign_paused = true;
      if (normalized.campaign_paused === 'resume' || normalized.campaign_paused === 'active') normalized.campaign_paused = false;
      normalized.campaign_paused = !!normalized.campaign_paused;
      delete normalized.paused;
      delete normalized.is_paused;

      // Save paused state separately and remove from normalized so PATCH doesn't update it
      const newPaused = !!normalized.campaign_paused;
      delete normalized.campaign_paused;

      // Always set normalized.email to the main address (the UI edit field)
      normalized.email = main;

      // Preserve existing cc_store unless the user explicitly changed the CC input in the modal.
      const originalCc = (original as any).cc_store || '';
      const editedCcRaw = (editContactData as any).cc_store;
      const editedCcTrim = editedCcRaw != null ? editedCcRaw.toString().trim() : '';

      if (editedCcRaw !== undefined && editedCcTrim !== originalCc) {
        // User changed the CC input explicitly (either cleared it or provided new values)
        normalized.cc_store = editedCcTrim ? editedCcTrim.split(',').map((s: string) => s.trim()).filter(Boolean).join(', ') : '';
      } else {
        // User did not change the CC input — keep the original cc_store unchanged
        normalized.cc_store = originalCc;
      }

      const diff: Partial<ExtendedCampaignContact> = {};
      // Compare against original, include cc_store comparison
      const keys = new Set([...Object.keys(normalized), ...Object.keys(original as any)]);
      // Ensure campaign_paused is never included in the PATCH diff — it is handled
      // separately via pauseCampaignContact / resumeCampaignContact to avoid
      // generating duplicate SQL assignments for the same column.
      keys.delete('campaign_paused');
      keys.delete('paused');
      keys.delete('is_paused');
      keys.delete('isPaused');
      keys.delete('paused_state');
      keys.forEach((k) => {
        const newVal = (normalized as any)[k];
        const oldVal = (original as any)[k];
        if (JSON.stringify(newVal) !== JSON.stringify(oldVal)) {
          diff[k as keyof ExtendedCampaignContact] = newVal;
        }
      });

      // Explicitly ensure phone_number and country_code are included if they have values
      // This handles the case where a contact didn't have these fields before
      if ((editContactData as any).phone_number && ((original as any).phone_number || '') !== ((editContactData as any).phone_number || '')) {
        (diff as any).phone_number = (editContactData as any).phone_number;
      }
      if ((editContactData as any).country_code && ((original as any).country_code || 'US') !== ((editContactData as any).country_code || 'US')) {
        (diff as any).country_code = (editContactData as any).country_code;
      }

      // If email changed but cc_store wasn't included in the diff, explicitly include
      // the original cc_store so the backend does not recompute/clear it from the email field.
      if ((diff as any).email !== undefined && (diff as any).cc_store === undefined) {
        (diff as any).cc_store = originalCc;
      }

      if (Object.keys(diff).length === 0) {
        toast.info('No changes to save');
        setEditModalOpen(false);
        setEditContactData(null);
        return;
      }

      // Map human-friendly stage labels to canonical backend stage tokens
      // so that selecting 'Sepa BT payment' stores 'sepa' in DB while the UI
      // continues to display the friendly label.
      const mapStageLabelToToken = (label: any) => {
        if (!label) return label;
        const s = String(label).trim().toLowerCase();
        if (s === 'sepa bt payment' || s === 'sepa bt' || s === 'sepa') return 'sepa';
        if (s === 'rh bt payment' || s === 'rh bt' || s === 'rh') return 'rh';
        if (s === 'first message') return 'initial';
        // default: return lowercased label (backend is tolerant and lowercases stages)
        return s;
      };

      if ((diff as any).stage !== undefined) {
        (diff as any).stage = mapStageLabelToToken((diff as any).stage);
      }

      // Call PATCH helper
      await patchContact(editContactData.id as number, diff as any);

      // If campaign paused state changed, call pause/resume API
      try {
        const origPaused = !!(original as any).campaign_paused;
        if (origPaused !== newPaused) {
          if (newPaused) {
            await pauseCampaignContact(editContactData.id as number);
            toast.success('Campaign paused');
          } else {
            await resumeCampaignContact(editContactData.id as number);
            toast.success('Campaign resumed');
          }
        }
      } catch (e) {
        console.error('Failed to update campaign paused state', e);
        toast.error('Failed to update campaign state');
      }

      toast.success('Contact updated');
      setEditModalOpen(false);
      setEditContactData(null);
      onRefresh();
    } catch (err) {
      console.error('Patch contact error', err);
      toast.error('Failed to update contact');
    }
  };

  const handleDelete = async (id: number) => {
    if (!window.confirm('Delete this contact?')) return;
    try {
      await deleteContact(id);
      toast.success('Contact deleted');
      onRefresh();
    } catch {
      toast.error('Failed to delete contact');
    }
  };

  // Bulk actions for selected contacts
  const getSelectedIds = () => Object.keys(selected).filter(k => selected[Number(k)]).map(k => Number(k));

  const handleBulkPause = async () => {
    const ids = getSelectedIds();
    if (!ids.length) return;
    // Only admins may perform bulk actions
    if (ids.length > 1 && !(user && (user as any).is_admin)) {
      toast.error('Only administrators can perform bulk pause.');
      return;
    }
    if (!window.confirm(`Pause ${ids.length} selected contact(s)?`)) return;
    try {
      await Promise.all(ids.map(id => pauseCampaignContact(id)));
      toast.success('Selected campaigns paused');
      setSelected({});
      onRefresh();
    } catch (err) {
      console.error('Bulk pause error', err);
      toast.error('Failed to pause some selected campaigns');
    }
  };

  const handleBulkResume = async () => {
    const ids = getSelectedIds();
    if (!ids.length) return;
    if (ids.length > 1 && !(user && (user as any).is_admin)) {
      toast.error('Only administrators can perform bulk resume.');
      return;
    }
    if (!window.confirm(`Resume ${ids.length} selected contact(s)?`)) return;
    try {
      await Promise.all(ids.map(id => resumeCampaignContact(id)));
      toast.success('Selected campaigns resumed');
      setSelected({});
      onRefresh();
    } catch (err) {
      console.error('Bulk resume error', err);
      toast.error('Failed to resume some selected campaigns');
    }
  };

  const handleBulkDelete = async () => {
    const ids = getSelectedIds();
    if (!ids.length) return;
    if (ids.length > 1 && !(user && (user as any).is_admin)) {
      toast.error('Only administrators can perform bulk delete.');
      return;
    }
    if (!window.confirm(`Delete ${ids.length} selected contact(s)? This cannot be undone.`)) return;
    try {
      await Promise.all(ids.map(id => deleteContact(id)));
      toast.success('Selected contacts deleted');
      setSelected({});
      onRefresh();
    } catch (err) {
      console.error('Bulk delete error', err);
      toast.error('Failed to delete some selected contacts');
    }
  };

  const handleRetryFailed = async () => {
    const ids = getSelectedIds();
    if (!ids.length) return;
    if (!window.confirm(`Retry failed messages for ${ids.length} selected contact(s)?`)) return;
    try {
      const token = localStorage.getItem('token');
      const apiUrl = (import.meta.env.VITE_API_URL || '') + `/campaign_contacts/retry-failed`;
      const response = await fetch(apiUrl, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ contact_ids: ids })
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to retry failed messages');
      }
      const result = await response.json();
      toast.success(`Retry initiated for ${result.count} message(s)`);
      setSelected({});
      onRefresh();
    } catch (err: any) {
      console.error('Retry failed error', err);
      toast.error(err.message || 'Failed to retry messages');
    }
  };

  const handleSelect = (id: number) => setSelected((s: { [id: number]: boolean }) => ({ ...s, [id]: !s[id] }));
  const handleSelectAll = () => {
    const all = contacts.every((c: ExtendedCampaignContact) => selected[c.id]);
    const newSel: { [id: number]: boolean } = {};
    contacts.forEach((c: ExtendedCampaignContact) => { newSel[c.id] = !all; });
    setSelected(newSel);
  };
  const handleStartCampaign = () => {
    const contact_ids = Object.keys(selected).filter(id => selected[+id]).map(Number);
    // Opening the Start Campaign modal should be allowed for all users.
    // The server may still enforce permissions on the actual send action.
    
    setCampaignSubject(defaultSubject);
    setCampaignMessage(defaultMessage);
    setCampaignModal(true);
  };
  const handleCampaignSubmit = async () => {
    setIsCampaignLoading(true);
    try {
      const contact_ids = Object.keys(selected).filter(id => selected[+id]).map(Number);
      const payload = contacts
        .filter((c: ExtendedCampaignContact) => contact_ids.includes(c.id))
        .map((c: ExtendedCampaignContact) => ({
          contact_id: c.id,
          sender_email: (c as any).sender_email || '',
          subject: campaignSubject,
          body: campaignMessage,
          forms_link: bulkFields[c.id]?.forms_link || '',
          payment_link: bulkFields[c.id]?.payment_link || '',
          invoice_number: bulkFields[c.id]?.invoice_number || (c as any).invoice_number || null,
          attachment: bulkFields[c.id]?.attachmentBase64 || null
        }));
      await startCampaign(payload);
      toast.success('Campaign started!');
      setCampaignModal(false);
      setSelected({});
      setCampaignSubject('');
      setCampaignMessage('');
      setBulkFields({});
      onRefresh();
    } catch {
      toast.error('Failed to start campaign');
    } finally {
      setIsCampaignLoading(false);
    }
  };

  const handleExcelUpload = async (file: File) => {
    setIsUploading(true);
    try {
      const { usePreviewResults } = await import('../contexts/PreviewResultsContext');
      const ctx = usePreviewResults();
      
      // Submit as a background job
      const token = localStorage.getItem('token');
      const formData = new FormData();
      formData.append('file', file);
      const apiUrl = (import.meta.env.VITE_API_URL || '') + `/campaign_contacts/upload-excel-job`;
      const resp = await fetch(apiUrl, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`
        },
        body: formData
      });
      if (!resp.ok) throw new Error('Failed to create upload job');
      const data = await resp.json();
      const jobId = data.job_id;
      toast.info('Upload started. Validating rows...');

      // Open results modal and initialize with empty results
      ctx.setPreviewResults([]);
      ctx.setOpen(true);

      // Poll results endpoint every 1s to stream in results as they complete
      const statusUrl = (import.meta.env.VITE_API_URL || '') + `/campaign_contacts/upload-job/${jobId}/status`;
      const resultsUrl = (import.meta.env.VITE_API_URL || '') + `/campaign_contacts/upload-job/${jobId}/results`;
      setJobProgress({ jobId, status: 'pending', total: 0, processed: 0 });
      setProgressModalOpen(true);

      let done = false;
      let lastResultsCount = 0;
      
      while (!done) {
        await new Promise(r => setTimeout(r, 1000));
        try {
          const sresp = await fetch(statusUrl, { headers: { 'Authorization': `Bearer ${token}` } });
          if (!sresp.ok) throw new Error('Failed to fetch job status');
          const statusJson = await sresp.json();
          const total = statusJson.total_rows || statusJson.total || 0;
          const processed = statusJson.processed_rows || statusJson.processed || 0;
          setJobProgress({ jobId, status: statusJson.status, total, processed });

          // Fetch new results that have been processed
          try {
            const rresp = await fetch(resultsUrl, { headers: { 'Authorization': `Bearer ${token}` } });
            if (rresp.ok) {
              const resultsJson = await rresp.json();
              const newResults = resultsJson.results || [];
              if (newResults.length > lastResultsCount) {
                ctx.setPreviewResults(newResults);
                lastResultsCount = newResults.length;
              }
            }
          } catch (e) {
            console.debug('Results fetch error (non-blocking)', e);
          }

          if (statusJson.status === 'finished') {
            // Fetch final results
            const finalResp = await fetch(resultsUrl, { headers: { 'Authorization': `Bearer ${token}` } });
            if (finalResp.ok) {
              const finalJson = await finalResp.json();
              ctx.setPreviewResults(finalJson.results || []);
            }
            
            // No Excel download - results shown in modal only
            toast.success('Validation complete! All results shown above.');
            
            done = true;
            onRefresh();
            setTimeout(() => setProgressModalOpen(false), 1500);
          } else if (statusJson.status === 'failed') {
            toast.error(`Validation job failed: ${statusJson.error || 'unknown'}`);
            done = true;
            onRefresh();
            setTimeout(() => setProgressModalOpen(false), 800);
          }
        } catch (e) {
          console.warn('Job poll error', e);
        }
      }
    } catch {
      toast.error('Failed to upload contacts');
    } finally {
      setIsUploading(false);
    }
  };

  // Apply global search filtering.
  // - If globalSearch contains '+' with two or more tokens, treat as strict
  //   positional match: token0 -> stage, token1 -> status. Both must match
  //   (whole-word) for the contact to be included.
  // - Otherwise fall back to the existing any-field contains behavior.
  // Normalization used for search matching: lowercase, remove punctuation, collapse spaces
  const normalizeForSearch = (s: any) => {
    if (s === null || s === undefined) return '';
    try {
      return s.toString().toLowerCase().trim().replace(/\s+/g, ' ').replace(/[\p{P}$+<=>^`|~]/gu, '');
    } catch (e) {
      return s.toString().toLowerCase();
    }
  };

  const filteredContacts = useMemo(() => {
    if (!globalSearch || !globalSearch.trim()) return contacts;
    const q = globalSearch.toString().trim().toLowerCase();
    const qnorm = normalizeForSearch(q);

    if (q.includes('+')) {
      const parts = q.split('+').map(p => p.trim()).filter(Boolean);
      if (parts.length >= 2) {
        const stageToken = parts[0];
        const statusToken = parts[1];
        const splitWords = (s: string) => (s || '').split(/[^a-z0-9]+/).filter(Boolean);

        return contacts.filter((c: ExtendedCampaignContact) => {
          const stage = ((c as any).stage || '').toString().toLowerCase();
          const status = ((c as any).status || '').toString().toLowerCase();

          const stageWords = splitWords(stage);
          const statusWords = splitWords(status);

          const stageMatches = stage === stageToken || stageWords.includes(stageToken);
          const statusMatches = status === statusToken || statusWords.includes(statusToken);

          return stageMatches && statusMatches;
        });
      }
      // if only one token after splitting, fall through to permissive mode
    }

    const qLower = q;
    return contacts.filter((c: ExtendedCampaignContact) =>
  Object.values(c).some((v) => normalizeForSearch(v).includes(qnorm))
    );
  }, [contacts, globalSearch]);

  // Helper: convert various stored date formats (dd/MM/YYYY or ISO) to yyyy-MM-dd for <input type="date">
  const toInputDate = (dateStr?: string | null) => {
    if (!dateStr) return '';
    try {
      const s = dateStr.toString().trim();
      // dd/MM/YYYY
      if (/^\d{2}\/\d{2}\/\d{4}$/.test(s)) {
        const [d, m, y] = s.split('/');
        return `${y}-${m.padStart(2, '0')}-${d.padStart(2, '0')}`;
      }
      // YYYY-MM-DD or ISO
      const d = new Date(s);
      if (!isNaN(d.getTime())) {
        const yyyy = d.getFullYear();
        const mm = String(d.getMonth() + 1).padStart(2, '0');
        const dd = String(d.getDate()).padStart(2, '0');
        return `${yyyy}-${mm}-${dd}`;
      }
    } catch (e) {
      // fallthrough
    }
    return '';
  };

  // Word-aware wrap: build lines up to maxLength, avoid breaking words when possible.
  const wrapText = (s: string | undefined | null, maxLength = 50) => {
    if (!s) return '';
    const str = s.toString();
    if (str.length <= maxLength) return str;
    const words = str.split(/(\s+)/); // keep separators so we preserve spaces
    const lines: string[] = [];
    let current = '';

    for (const token of words) {
      // token may be whitespace or a word
      if ((current + token).length <= maxLength) {
        current += token;
        continue;
      }

      // if token itself is longer than maxLength, hard-slice the token
      if (token.trim().length > maxLength) {
        // flush current
        if (current) {
          lines.push(current.trimEnd());
          current = '';
        }
        // slice the long token
        let i = 0;
        while (i < token.length) {
          lines.push(token.slice(i, i + maxLength));
          i += maxLength;
        }
        continue;
      }

      // otherwise token fits on next line
      if (current) {
        lines.push(current.trimEnd());
      }
      current = token.trimStart();
    }

    if (current) lines.push(current.trimEnd());
    return lines.join('\n');
  };
  const wrapColumns = new Set(['hotel_name', 'notes', 'trigger']);

  const handleLinksSubmit = async (values: {
    forms_link: string;
    payment_link: string;
    attachment: File | null;
    invoice_number?: string | null;
  }) => {
    if (!currentContact) return;

    try {
      // Get the token from localStorage
      const token = localStorage.getItem('token');
      console.log('Auth token:', token ? 'Token exists' : 'No token found');
      
      if (!token) {
        throw new Error('No authentication token found');
      }

      const formData = new FormData();
      formData.append('forms_link', values.forms_link);
      formData.append('payment_link', values.payment_link);
      if (values.invoice_number != null) {
        formData.append('invoice_number', values.invoice_number);
      }
      if (values.attachment) {
        formData.append('attachment', values.attachment);
      }

      // Log form data for debugging
      for (let [key, value] of formData.entries()) {
        console.log(key, value);
      }

      const url = `https://conferencecare.org/api/campaign_contacts/${currentContact.id}/links`;
      console.log('Making request to:', url);

      const response = await fetch(url, {
        method: 'PUT',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Accept': 'application/json',
        },
        credentials: 'include', // Important for sending cookies with CORS
        mode: 'cors',
        body: formData
      });

      console.log('Response status:', response.status);
      if (!response.ok) {
        const errorText = await response.text();
        console.error('Error response:', errorText);
        throw new Error(`HTTP error! status: ${response.status}, details: ${errorText}`);
      }

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to update links and files');
      }

      toast.success('Links and files updated');
      setLinksModal(false);
      onRefresh();
    } catch (error: any) {
      console.error('Error updating links:', error);
      toast.error(error.message || 'Failed to update links and files');
    }
  };

  return (
    <div>
      <div className="flex items-center mb-2 gap-2">
        {Object.values(selected).some(Boolean) && (
          <span className="text-sm font-semibold text-blue-600 bg-blue-50 px-3 py-1 rounded">
            {getSelectedIds().length} selected
          </span>
        )}
        <Button size="sm" onClick={handleStartCampaign} disabled={!Object.values(selected).some(Boolean)}>
          Start Campaign for Selected
        </Button>
        <Button size="sm" onClick={handleBulkPause} disabled={!Object.values(selected).some(Boolean)} style={{ backgroundColor: '#f0ad4e', color: '#000000' }}>
          Pause Selected
        </Button>
        <Button size="sm" onClick={handleBulkResume} disabled={!Object.values(selected).some(Boolean)} style={{ backgroundColor: '#28a745', color: '#ffffff' }}>
          Resume Selected
        </Button>
        <Button size="sm" onClick={handleRetryFailed} disabled={!Object.values(selected).some(Boolean)} style={{ backgroundColor: '#ff6b6b', color: '#ffffff' }} title="Retry failed messages for selected contacts">
          Retry Failed
        </Button>
        <Button size="sm" variant="destructive" onClick={handleBulkDelete} disabled={!Object.values(selected).some(Boolean)} style={{ backgroundColor: '#dc3545', color: '#ffffff' }}>
          Delete Selected
        </Button>
        <ExcelUpload isProcessing={isUploading} onUpload={(file: File) => (onUpload ? onUpload(file) : handleExcelUpload(file))} />
      </div>

      <Dialog open={linksModal} onOpenChange={setLinksModal}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Manage Links & Files for {currentContact?.name}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4 p-4">
            <div>
              <label className="block text-sm font-medium mb-1">Forms Link</label>
              <input
                type="text"
                className="w-full border rounded px-2 py-1"
                defaultValue={currentContact?.forms_link || ''}
                onChange={(e: ChangeEvent<HTMLInputElement>) => setCurrentContact((prev: ExtendedCampaignContact | null) => prev ? {...prev, forms_link: e.target.value} : null)}
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Payment Link</label>
              <input
                type="text"
                className="w-full border rounded px-2 py-1"
                defaultValue={currentContact?.payment_link || ''}
                onChange={(e: ChangeEvent<HTMLInputElement>) => setCurrentContact((prev: ExtendedCampaignContact | null) => prev ? {...prev, payment_link: e.target.value} : null)}
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Attachment</label>
              <input
                type="file"
                className="block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded-full file:border-0 file:text-sm file:font-semibold file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100"
                onChange={(e: ChangeEvent<HTMLInputElement>) => {
                  const file = e.target.files?.[0] || null;
                  setCurrentContact((prev: ExtendedCampaignContact | null) => prev ? {...prev, attachment: file} : null);
                }}
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Invoice Number</label>
              <input
                type="text"
                className="w-full border rounded px-2 py-1"
                defaultValue={currentContact?.invoice_number || ''}
                onChange={(e: ChangeEvent<HTMLInputElement>) => setCurrentContact((prev: ExtendedCampaignContact | null) => prev ? {...prev, invoice_number: e.target.value} : null)}
              />
            </div>
            <Button 
              onClick={() => handleLinksSubmit({
                forms_link: currentContact?.forms_link || '',
                payment_link: currentContact?.payment_link || '',
                attachment: currentContact?.attachment || null,
                invoice_number: currentContact?.invoice_number || null
              })}
            >
              Save
            </Button>
          </div>
        </DialogContent>
      </Dialog>
      <table className="min-w-full divide-y divide-gray-200 bg-white">
        <thead className="bg-gray-100">
          <tr>
            <th className="px-2 py-1 border-r border-gray-200"><input type="checkbox" checked={contacts.every((c: ExtendedCampaignContact) => selected[c.id])} onChange={handleSelectAll} /></th>
            <th className="px-2 py-1 border-r border-gray-200">+</th>
            <th className="px-2 py-1 border-r border-gray-200">Title</th>
            <th className="px-2 py-1 border-r border-gray-200">Name</th>
            <th className="px-2 py-1 border-r border-gray-200">Email</th>
            <th className="px-2 py-1 border-r border-gray-200">Phone</th>
            <th className="px-2 py-1 border-r border-gray-200">CC</th>
            <th className="px-2 py-1 border-r border-gray-200">Date</th>
            <th className="px-2 py-1 border-r border-gray-200">Validation</th>
            <th className="px-2 py-1 border-r border-gray-200">Organizer</th>
            <th className="px-2 py-1 border-r border-gray-200">Stage</th>
            <th className="px-2 py-1 border-r border-gray-200">Status</th>
            <th className="px-2 py-1 border-r border-gray-200">Hotel Name</th>
            <th className="px-2 py-1 border-r border-gray-200">Supplier</th>
            <th className="px-2 py-1 border-r border-gray-200">Notes</th>
            <th className="px-2 py-1 border-r border-gray-200">Booking ID</th>
            <th className="px-2 py-1 border-r border-gray-200">Speaker Type</th>
            <th className="px-2 py-1 border-r border-gray-200">Nationality</th>
            <th className="px-2 py-1 border-r border-gray-200">Workplace</th>
            <th className="px-2 py-1 border-r border-gray-200">Payment Method</th>
            <th className="px-2 py-1 border-r border-gray-200">Trigger</th>
            <th className="px-2 py-1 border-r border-gray-200">Relations</th>
            <th className="px-2 py-1 border-r border-gray-200">Links & Files</th>
            <th className="px-2 py-1 border-r border-gray-200">Paused</th>
            <th className="px-2 py-1">Actions</th>
          </tr>
        </thead>
        <tbody>
          {filteredContacts.map((c: CampaignContact, idx: number) => (
            <tr key={c.id} className={`${idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'} border-b`}>
              <td className="px-2 py-1 border-r border-gray-200"><input type="checkbox" checked={!!selected[c.id]} onChange={() => handleSelect(c.id)} /></td>
              <td className="px-2 py-1 border-r border-gray-200">
                <div className="flex gap-1">
                  {onSingleCampaign && (
                    <button
                      onClick={() => onSingleCampaign(c)}
                      className="text-blue-600 hover:text-blue-800 font-bold text-lg"
                      title="Start Campaign for this contact"
                    >
                      +
                    </button>
                  )}
                  <button
                    onClick={() => {
                      setMessagesModalContactId(c.id);
                      setMessagesModalContactName(c.name || `Contact ${c.id}`);
                      setCustomMessagesModalOpen(true);
                    }}
                    className="text-purple-600 hover:text-purple-800 font-bold text-lg"
                    title="Customize messages for this contact"
                  >
                    ✎
                  </button>
                </div>
              </td>
              {/* Editable columns */}
        {[
                { key: 'prefix', label: 'Title' },
                { key: 'name', label: 'Name' },
                { key: 'email', label: 'Email' },
                { key: 'phone_number', label: 'Phone' },
          { key: 'cc_store', label: 'CC' },
                { key: 'date', label: 'Date' },
                { key: 'validation_result', label: 'Validation' },
                { key: 'organizer', label: 'Organizer' },
                { key: 'stage', label: 'Stage', options: STAGE_OPTIONS },
                { key: 'status', label: 'Status', options: DEFAULT_STATUS_OPTIONS },
                { key: 'hotel_name', label: 'Hotel Name' },
                { key: 'supplier', label: 'Supplier' },
                { key: 'notes', label: 'Notes' },
                { key: 'booking_id', label: 'Booking ID' },
                { key: 'speaker_type', label: 'Speaker Type' },
                { key: 'nationality', label: 'Nationality' },
                { key: 'workplace', label: 'Workplace' },
                { key: 'payment_method', label: 'Payment Method' },
                { key: 'trigger', label: 'Trigger' }
              ].map(col => (
                <td className="px-2 py-1 border-r border-gray-200" key={col.key}>
                    {/* Render date with a friendly format and leave editing to the modal */}
                    {col.key === 'date' ? (
                      <span className="px-1">{(c as any)[col.key] ? (() => {
                        try {
                          const d = new Date((c as any)[col.key]);
                          if (isNaN(d.getTime())) return (c as any)[col.key];
                          return d.toLocaleDateString();
                        } catch { return (c as any)[col.key]; }
                      })() : ''}</span>
                    ) : col.key === 'validation_result' ? (
                      <span className="px-2 py-1">
                        {(() => {
                          const val = (c as any)[col.key] || '';
                          const text = String(val);
                          const isGray = /450|451|Temporary Failure/i.test(text);
                          const isValid = /valid|catch-all|Catch-All|Valid email/i.test(text) || text === 'Valid';
                          const badgeStyle = isGray ? { backgroundColor: '#6c757d', color: '#fff' } : isValid ? { backgroundColor: '#28a745', color: '#fff' } : { backgroundColor: '#dc3545', color: '#fff' };
                          return (
                            <span className="px-2 py-1 rounded text-sm inline-block" style={badgeStyle} title={text}>
                              {text || 'N/A'}
                            </span>
                          );
                        })()}
                      </span>
                    ) : col.key === 'trigger' ? (
                      // Truncate trigger to 50 chars with a read-more toggle
                      <span className="px-1">
                        {(() => {
                          const full = (c as any)[col.key] || '';
                          const limit = 50;
                          if (!full) return '';
                          if (full.length <= limit) return full;
                          const showing = !!showFullTrigger[c.id];
                          return (
                            <div className="flex items-center gap-2">
                              <span>{showing ? full : `${full.slice(0, limit)}...`}</span>
                              <Button size="sm" variant="ghost" onClick={() => setShowFullTrigger((s: { [id: number]: boolean }) => ({ ...s, [c.id]: !s[c.id] }))}>
                                {showing ? 'Show less' : 'Read more'}
                              </Button>
                            </div>
                          );
                        })()}
                      </span>
                    ) : col.key === 'email' ? (
                      <span className="px-1">{(() => {
                        const emailRaw = (c as any).email || '';
                        const parts = ('' + emailRaw).split(/[;,\s]+/).map(s => s.trim()).filter(Boolean);
                        // Show the full list (main + CCs) in the Email column, comma-separated
                        return parts.length ? parts.join(', ') : emailRaw;
                      })()}</span>
                    ) : col.key === 'phone_number' ? (
                      <span className="px-1" title={`Country: ${(c as any).country_code || 'Not set'}, Phone: ${(c as any).phone_number || 'None'}`}>
                        {(() => {
                          const phone = (c as any).phone_number;
                          if (!phone) return '';
                          const country = (c as any).country_code || 'US';
                          const countryMap: Record<string, string> = {
                            'US': '+1', 'UK': '+44', 'CA': '+1', 'AU': '+61', 'DE': '+49',
                            'FR': '+33', 'IT': '+39', 'ES': '+34', 'NL': '+31', 'BE': '+32',
                            'CH': '+41', 'AT': '+43', 'SE': '+46', 'NO': '+47', 'DK': '+45',
                            'FI': '+358', 'IE': '+353', 'PL': '+48', 'CZ': '+420', 'RU': '+7',
                            'UA': '+380', 'TR': '+90', 'AE': '+971', 'SA': '+966', 'IN': '+91',
                            'CN': '+86', 'JP': '+81', 'KR': '+82', 'BR': '+55', 'MX': '+52',
                            'AR': '+54', 'ZA': '+27', 'NG': '+234', 'EG': '+20', 'IL': '+972',
                            'SG': '+65', 'MY': '+60', 'TH': '+66', 'VN': '+84', 'PH': '+63',
                            'ID': '+62', 'NZ': '+64'
                          };
                          const dialCode = countryMap[country] || '+1';
                          return `${dialCode} ${phone}`;
                        })()}
                      </span>
                    ) : col.key === 'cc_store' ? (
                      <span className="px-1" title={(c as any).cc_store || (() => {
                        const emailRaw = (c as any).email || '';
                        const parts = ('' + emailRaw).split(/[;,\s]+/).map(s => s.trim()).filter(Boolean);
                        return parts.length > 1 ? parts.slice(1).join(', ') : '';
                      })()}>
                        {(() => {
                          const cc = (c as any).cc_store;
                          if (cc && cc.toString().trim()) return cc;
                          const emailRaw = (c as any).email || '';
                          const parts = ('' + emailRaw).split(/[;,\s]+/).map(s => s.trim()).filter(Boolean);
                          return parts.length > 1 ? parts.slice(1).join(', ') : '';
                        })()}
                      </span>
                    ) : col.key === 'stage' ? (
                      <div className="flex items-center gap-2">
                        <span className={`px-2 py-1 rounded text-sm inline-block ${getStageColor((c as any).stage || '')}`}>
                          {(c as any).stage || ''}
                        </span>
                        {(() => {
                          const s = ((c as any).stage || '').toString().toLowerCase();
                          if (s === 'rh' || s === 'rh bt payment' || s === 'rh bt') {
                            return (
                              <Badge title={"RH BT payment — first 3 reminders: every 2 days; afterwards use standard payments reminders"} style={{ backgroundColor: '#FFD966', color: '#000000', fontWeight: 600 }}>
                                RH
                              </Badge>
                            );
                          }
                          return null;
                        })()}
                      </div>
                    ) : col.key === 'status' ? (
                      <div className="flex flex-col gap-1">
                        <span className={`px-2 py-1 rounded text-sm inline-block ${getStatusColor((c as any).status || '')}`}>
                          {(c as any).status || ''}
                        </span>
                        {(c as any).email_error && (
                          <div className="text-xs px-2 py-1 rounded bg-red-100 text-red-800 border border-red-300" title={(c as any).email_error}>
                            ⚠️ Error: {((c as any).email_error || '').length > 40 ? ((c as any).email_error || '').substring(0, 40) + '...' : (c as any).email_error}
                          </div>
                        )}
                      </div>
                    ) : (
                      // For regular columns, apply the same wrap/truncation behavior
                      // used for Hotel Name: wrap at ~50 chars and preserve line breaks.
                      <pre
                        className="px-1 whitespace-pre text-sm"
                        style={{ margin: 0, whiteSpace: 'pre', wordBreak: 'normal', overflowWrap: 'normal' }}
                      >
                        {wrapText((c as any)[col.key], 50)}
                      </pre>
                    )}
                </td>
              ))}
              {/* Relations Column */}
              <td className="px-2 py-1 border-r border-gray-200">
                <RelationsCell contact={c} />
              </td>
              {/* Links & Files Column */}
              <td className="px-2 py-1 border-r border-gray-200">
                <div className="flex flex-col gap-1">
                  {((c as any).has_attachment || (c as any).attachment_filename) ? (
                    <div className="flex items-center gap-2">
                      <button
                        onClick={async () => {
                          try {
                            const API_BASE = import.meta.env.VITE_API_URL || '';
                            const token = localStorage.getItem('token');
                            const res = await fetch(`${API_BASE}/campaign_contacts/${c.id}/attachment`, {
                              headers: { Authorization: `Bearer ${token}` },
                            });
                            if (!res.ok) throw new Error('Failed to fetch attachment');
                            const blob = await res.blob();
                            const url = URL.createObjectURL(blob);
                            window.open(url, '_blank');
                            // revoke after 1 minute
                            setTimeout(() => URL.revokeObjectURL(url), 60000);
                          } catch (err: any) {
                            console.error('View attachment error', err);
                            toast.error(err?.message || 'Failed to open attachment');
                          }
                        }}
                        className="text-xs text-green-600 hover:underline"
                      >
                        View Attachment
                      </button>
                      <button
                        onClick={async () => {
                          if (!window.confirm('Delete attachment for this contact?')) return;
                          try {
                            const API_BASE = import.meta.env.VITE_API_URL || '';
                            const token = localStorage.getItem('token');
                            const res = await fetch(`${API_BASE}/campaign_contacts/${c.id}/attachment`, {
                              method: 'DELETE',
                              headers: { Authorization: `Bearer ${token}` },
                            });
                            if (!res.ok) throw new Error('Failed to delete attachment');
                            toast.success('Attachment deleted');
                            onRefresh();
                          } catch (err: any) {
                            console.error('Delete attachment error', err);
                            toast.error(err?.message || 'Failed to delete attachment');
                          }
                        }}
                        className="text-xs text-red-600 hover:underline"
                      >
                        Delete
                      </button>
                    </div>
                  ) : (
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => {
                        setCurrentContact(c);
                        setLinksModal(true);
                      }}
                      style={{ backgroundColor: '#0d6efd', color: '#ffffff', borderColor: '#0d6efd' }}
                    >
                      Manage Links & Files
                    </Button>
                  )}
                  {(c as any).attachment_filename && <div className="text-xs text-gray-600">{(c as any).attachment_filename}</div>}
                </div>
              </td>
              <td className="px-2 py-1 border-r border-gray-200">
                {c.campaign_paused ? (
                  <Badge style={{ backgroundColor: '#f0ad4e', color: '#000000' }}>Paused</Badge>
                ) : (
                  <Badge style={{ backgroundColor: '#28a745', color: '#ffffff' }}>Active</Badge>
                )}
              </td>
              <td className="px-2 py-1 border-r border-gray-200">
                {/* Show a compact tag and an edit (+) button that opens Custom Follow-Ups modal */}
                <div className="flex items-center gap-2">
                  <span title={((c as any).flow_type === 'custom') ? 'Has custom flow' : 'Using default flow'}>
                    {(c as any).flow_type === 'custom' ? '✅ Yes' : '❌ No'}
                  </span>
                  <button
                    className="text-blue-600 hover:text-blue-800"
                    onClick={async () => {
                      // Open modal and fetch existing flow (if any)
                      setModalContactId(c.id);
                      setModalInitialSteps([]);
                      setModalFlowId(undefined);
                      setCustomFlowModalOpen(true);
                      try {
                        const res = await authFetch(`/api/contacts/${c.id}/custom_flow`);
                        if (res && res.ok) {
                          const data = await res.json();
                          if (data && data.flow_steps) {
                            setModalInitialSteps(data.flow_steps.map((s: any) => ({
                              type: s.type === 'wait' ? 'task' : s.type,
                              subject: s.subject || '',
                              body: s.body || '',
                              delay_days: s.delay_days || 0
                            })));
                            setModalFlowId(data.flow_id);
                          }
                        }
                      } catch (e) {
                        console.error('Failed to load custom flow', e);
                      }
                    }}
                    title="Edit Custom Follow-Ups"
                  >
                    +
                  </button>
                </div>
              </td>
              <td className="px-2 py-1 flex gap-2">
                {/* Edit (pen) button opens modal to edit all contact fields, confirm to save */}
                <Button size="sm" onClick={() => {
                  // Pre-fill edit modal with full email list and cc_store
                  const rawEmailField = (c.email || '').toString();
                  // Normalize into comma-separated list preserving order
                  const emails = rawEmailField.split(',').map(s => s.trim()).filter(Boolean);
                  const fullList = emails.join(', ');

                  // Prefer explicit cc_store if present; otherwise derive remainder from email list
                  const ccFromStore = (c as any).cc_store ? (c as any).cc_store.toString().trim() : '';
                  const derivedCc = emails.length > 1 ? emails.slice(1).join(', ') : '';
                  const cc = ccFromStore || derivedCc;

                  // Populate editContactData.email with the full list so modal shows all addresses
                  // Ensure country_code is set, defaulting to 'US' if not present
                  setEditContactData({ ...(c as any), email: fullList, cc_store: cc, country_code: (c as any).country_code || 'US' });
                  setEditModalOpen(true);
                }}>✎</Button>
                {/* Pause/Resume moved into Edit modal - removed per-row buttons */}
                <Button size="sm" variant="destructive" onClick={() => handleDelete(c.id)} style={{ backgroundColor: '#dc3545', color: '#ffffff' }}>Delete</Button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      <Dialog open={campaignModal} onOpenChange={setCampaignModal}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Start Campaign</DialogTitle>
          </DialogHeader>              
          <div className="flex flex-col gap-2">
            <input className="border rounded px-2 py-1" placeholder="Subject" value={campaignSubject} onChange={e => setCampaignSubject(e.target.value)} />
            <textarea className="border rounded px-2 py-1" placeholder="Message" value={campaignMessage} onChange={e => setCampaignMessage(e.target.value)} rows={4} />
            <div className="my-2" /> {/* Padding between message and table */}
            <div className="overflow-x-auto max-h-64 border rounded" style={{ maxHeight: 220 }}>
              <table className="min-w-full text-xs border">
                <thead className="sticky top-0 bg-gray-100 z-10">
                  <tr>
                    <th className="px-2 py-1">Name</th>
                    <th className="px-2 py-1">Email</th>
                    <th className="px-2 py-1 w-48">Forms Link</th>
                    <th className="px-2 py-1 w-48">Payment Link</th>
                    <th className="px-2 py-1">Attachment</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.keys(bulkFields).map((id, idx) => {
                    const c = contacts.find(x => x.id === Number(id));
                    if (!c) return null;
                    return (
                      <tr key={id} className={idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                        <td className="px-2 py-1">{c.name}</td>
                        <td className="px-2 py-1">{c.email}</td>
                        <td className="px-2 py-1 w-48">
                          <input type="text" className="border rounded px-1 py-0.5 w-full" value={bulkFields[c.id]?.forms_link || ''} onChange={e => handleBulkFieldChange(c.id, 'forms_link', e.target.value)} />
                        </td>
                        <td className="px-2 py-1 w-48">
                          <input type="text" className="border rounded px-1 py-0.5 w-full" value={bulkFields[c.id]?.payment_link || ''} onChange={e => handleBulkFieldChange(c.id, 'payment_link', e.target.value)} />
                        </td>
                        <td className="px-2 py-1">
                          <label className="flex items-center gap-1 cursor-pointer">
                            <input type="file" accept="*" style={{ display: 'none' }} onChange={e => handleBulkAttachment(c.id, e.target.files?.[0] || null)} />
                            <span className="border px-2 py-1 rounded bg-gray-200 hover:bg-gray-300">Choose File</span>
                            {bulkFields[c.id]?.attachment && (
                              <span className="text-green-600 ml-1" title={bulkFields[c.id]?.attachment?.name || ''}>
                                ✓ {bulkFields[c.id]?.attachment?.name?.slice(0, 16)}
                              </span>
                            )}
                          </label>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            <Button onClick={handleCampaignSubmit} loading={isCampaignLoading} disabled={!campaignSubject || !campaignMessage || isCampaignLoading}>
              {isCampaignLoading ? 'Sending...' : 'Send Campaign'}
            </Button>
          </div>
        </DialogContent>
      </Dialog>
     {/* Edit Contact Modal */}
<Dialog open={editModalOpen} onOpenChange={setEditModalOpen}>
  <DialogContent className="h-[90vh] max-h-[90vh] flex flex-col p-0 gap-0 w-full max-w-2xl overflow-hidden">

    {/* Header */}
    <DialogHeader className="border-b px-6 py-4 sticky top-0 bg-white z-20">
      <DialogTitle>Edit Contact</DialogTitle>
    </DialogHeader>

    {/* SCROLLABLE BODY */}
    <div className="flex-1 overflow-y-auto px-6 py-4 flex flex-col gap-4">

      {/* Basic Info Section */}
      <div className="space-y-2">
        <h3 className="text-sm font-semibold text-gray-700">Basic Info</h3>

        <input className="border rounded px-3 py-2 w-full text-sm"
          placeholder="Title (e.g., Prof, Dr, Mr)"
          value={(editContactData as any)?.prefix || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, prefix: e.target.value } : prev)
          }
        />

        <input className="border rounded px-3 py-2 w-full text-sm"
          placeholder="Name"
          value={editContactData?.name || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, name: e.target.value } : prev)
          }
        />

        <input className="border rounded px-3 py-2 w-full text-sm"
          placeholder="Email"
          value={editContactData?.email || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, email: e.target.value } : prev)
          }
        />

        <div className="flex gap-2">
          <select
            value={(editContactData as any)?.country_code || 'US'}
            onChange={(e) =>
              setEditContactData(prev => prev ? { ...prev, country_code: e.target.value } : prev)
            }
            className="border rounded px-3 py-2 text-sm flex-shrink-0"
          >
            <option value="US">US (+1)</option>
            <option value="CA">CA (+1)</option>
            <option value="AG">AG (+1-268)</option>
            <option value="BB">BB (+1-246)</option>
            <option value="BS">BS (+1-242)</option>
            <option value="BM">BM (+1-441)</option>
            <option value="KY">KY (+1-345)</option>
            <option value="DM">DM (+1-767)</option>
            <option value="DO">DO (+1-809)</option>
            <option value="GD">GD (+1-473)</option>
            <option value="JM">JM (+1-876)</option>
            <option value="PR">PR (+1-787)</option>
            <option value="TC">TC (+1-649)</option>
            <option value="TT">TT (+1-868)</option>
            <option value="VC">VC (+1-784)</option>
            <option value="VG">VG (+1-284)</option>
            <option value="VI">VI (+1-340)</option>
            <option value="GB">GB (+44)</option>
            <option value="IE">IE (+353)</option>
            <option value="FK">FK (+500)</option>
            <option value="GI">GI (+350)</option>
            <option value="GG">GG (+44-1481)</option>
            <option value="IM">IM (+44-1624)</option>
            <option value="JE">JE (+44-1534)</option>
            <option value="AT">AT (+43)</option>
            <option value="BY">BY (+375)</option>
            <option value="BE">BE (+32)</option>
            <option value="BA">BA (+387)</option>
            <option value="BG">BG (+359)</option>
            <option value="HR">HR (+385)</option>
            <option value="CY">CY (+357)</option>
            <option value="CZ">CZ (+420)</option>
            <option value="DK">DK (+45)</option>
            <option value="EE">EE (+372)</option>
            <option value="FO">FO (+298)</option>
            <option value="FI">FI (+358)</option>
            <option value="FR">FR (+33)</option>
            <option value="DE">DE (+49)</option>
            <option value="GR">GR (+30)</option>
            <option value="HU">HU (+36)</option>
            <option value="IS">IS (+354)</option>
            <option value="IT">IT (+39)</option>
            <option value="LV">LV (+371)</option>
            <option value="LI">LI (+423)</option>
            <option value="LT">LT (+370)</option>
            <option value="LU">LU (+352)</option>
            <option value="MT">MT (+356)</option>
            <option value="MD">MD (+373)</option>
            <option value="MC">MC (+377)</option>
            <option value="ME">ME (+382)</option>
            <option value="NL">NL (+31)</option>
            <option value="MK">MK (+389)</option>
            <option value="PL">PL (+48)</option>
            <option value="PT">PT (+351)</option>
            <option value="RO">RO (+40)</option>
            <option value="RU">RU (+7)</option>
            <option value="SM">SM (+378)</option>
            <option value="RS">RS (+381)</option>
            <option value="SK">SK (+421)</option>
            <option value="SI">SI (+386)</option>
            <option value="ES">ES (+34)</option>
            <option value="SE">SE (+46)</option>
            <option value="CH">CH (+41)</option>
            <option value="UA">UA (+380)</option>
            <option value="XK">XK (+383)</option>
            <option value="AX">AX (+358)</option>
            <option value="EG">EG (+20)</option>
            <option value="DZ">DZ (+213)</option>
            <option value="LY">LY (+218)</option>
            <option value="MZ">MZ (+216)</option>
            <option value="MA">MA (+212)</option>
            <option value="SD">SD (+249)</option>
            <option value="TN">TN (+216)</option>
            <option value="ZA">ZA (+27)</option>
            <option value="AO">AO (+244)</option>
            <option value="BJ">BJ (+229)</option>
            <option value="BW">BW (+267)</option>
            <option value="BF">BF (+226)</option>
            <option value="BI">BI (+257)</option>
            <option value="CM">CM (+237)</option>
            <option value="CV">CV (+238)</option>
            <option value="CF">CF (+236)</option>
            <option value="TD">TD (+235)</option>
            <option value="KM">KM (+269)</option>
            <option value="CG">CG (+242)</option>
            <option value="CD">CD (+243)</option>
            <option value="CI">CI (+225)</option>
            <option value="DJ">DJ (+253)</option>
            <option value="GQ">GQ (+240)</option>
            <option value="ER">ER (+291)</option>
            <option value="ET">ET (+251)</option>
            <option value="GA">GA (+241)</option>
            <option value="GM">GM (+220)</option>
            <option value="GH">GH (+233)</option>
            <option value="GN">GN (+224)</option>
            <option value="GW">GW (+245)</option>
            <option value="KE">KE (+254)</option>
            <option value="LS">LS (+266)</option>
            <option value="LR">LR (+231)</option>
            <option value="MG">MG (+261)</option>
            <option value="MW">MW (+265)</option>
            <option value="ML">ML (+223)</option>
            <option value="MR">MR (+222)</option>
            <option value="MU">MU (+230)</option>
            <option value="YT">YT (+262)</option>
            <option value="NA">NA (+264)</option>
            <option value="NE">NE (+227)</option>
            <option value="NG">NG (+234)</option>
            <option value="RE">RE (+262)</option>
            <option value="RW">RW (+250)</option>
            <option value="ST">ST (+239)</option>
            <option value="SN">SN (+221)</option>
            <option value="SC">SC (+248)</option>
            <option value="SL">SL (+232)</option>
            <option value="SO">SO (+252)</option>
            <option value="SH">SH (+290)</option>
            <option value="SS">SS (+211)</option>
            <option value="SZ">SZ (+268)</option>
            <option value="TZ">TZ (+255)</option>
            <option value="TG">TG (+228)</option>
            <option value="UG">UG (+256)</option>
            <option value="EH">EH (+212)</option>
            <option value="ZM">ZM (+260)</option>
            <option value="ZW">ZW (+263)</option>
            <option value="IL">IL (+972)</option>
            <option value="AE">AE (+971)</option>
            <option value="AF">AF (+93)</option>
            <option value="AM">AM (+374)</option>
            <option value="AZ">AZ (+994)</option>
            <option value="BH">BH (+973)</option>
            <option value="BD">BD (+880)</option>
            <option value="BT">BT (+975)</option>
            <option value="BN">BN (+673)</option>
            <option value="KH">KH (+855)</option>
            <option value="CN">CN (+86)</option>
            <option value="GE">GE (+995)</option>
            <option value="HK">HK (+852)</option>
            <option value="IN">IN (+91)</option>
            <option value="ID">ID (+62)</option>
            <option value="IR">IR (+98)</option>
            <option value="IQ">IQ (+964)</option>
            <option value="JP">JP (+81)</option>
            <option value="JO">JO (+962)</option>
            <option value="KZ">KZ (+7)</option>
            <option value="KP">KP (+850)</option>
            <option value="KR">KR (+82)</option>
            <option value="KW">KW (+965)</option>
            <option value="KG">KG (+996)</option>
            <option value="LA">LA (+856)</option>
            <option value="LB">LB (+961)</option>
            <option value="MO">MO (+853)</option>
            <option value="MY">MY (+60)</option>
            <option value="MV">MV (+960)</option>
            <option value="MN">MN (+976)</option>
            <option value="MM">MM (+95)</option>
            <option value="NP">NP (+977)</option>
            <option value="OM">OM (+968)</option>
            <option value="PK">PK (+92)</option>
            <option value="PS">PS (+970)</option>
            <option value="PH">PH (+63)</option>
            <option value="QA">QA (+974)</option>
            <option value="SA">SA (+966)</option>
            <option value="SG">SG (+65)</option>
            <option value="LK">LK (+94)</option>
            <option value="SY">SY (+963)</option>
            <option value="TW">TW (+886)</option>
            <option value="TJ">TJ (+992)</option>
            <option value="TH">TH (+66)</option>
            <option value="TL">TL (+670)</option>
            <option value="TR">TR (+90)</option>
            <option value="TM">TM (+993)</option>
            <option value="UZ">UZ (+998)</option>
            <option value="VN">VN (+84)</option>
            <option value="YE">YE (+967)</option>
            <option value="AR">AR (+54)</option>
            <option value="BO">BO (+591)</option>
            <option value="BR">BR (+55)</option>
            <option value="CL">CL (+56)</option>
            <option value="CO">CO (+57)</option>
            <option value="EC">EC (+593)</option>
            <option value="GY">GY (+592)</option>
            <option value="PY">PY (+595)</option>
            <option value="PE">PE (+51)</option>
            <option value="SR">SR (+597)</option>
            <option value="UY">UY (+598)</option>
            <option value="VE">VE (+58)</option>
            <option value="BZ">BZ (+501)</option>
            <option value="CR">CR (+506)</option>
            <option value="SV">SV (+503)</option>
            <option value="GT">GT (+502)</option>
            <option value="HN">HN (+504)</option>
            <option value="MX">MX (+52)</option>
            <option value="NI">NI (+505)</option>
            <option value="PA">PA (+507)</option>
            <option value="AU">AU (+61)</option>
            <option value="FJ">FJ (+679)</option>
            <option value="KI">KI (+686)</option>
            <option value="MH">MH (+692)</option>
            <option value="FM">FM (+691)</option>
            <option value="NR">NR (+674)</option>
            <option value="NZ">NZ (+64)</option>
            <option value="PW">PW (+680)</option>
            <option value="PG">PG (+675)</option>
            <option value="WS">WS (+685)</option>
            <option value="SB">SB (+677)</option>
            <option value="TO">TO (+676)</option>
            <option value="TV">TV (+688)</option>
            <option value="VU">VU (+678)</option>
            <option value="CU">CU (+53)</option>
            <option value="HT">HT (+509)</option>
          </select>
          <input className="border rounded px-3 py-2 w-full text-sm flex-1"
            placeholder="Phone Number (digits only)"
            value={(editContactData as any)?.phone_number || ''}
            onChange={(e) =>
              setEditContactData(prev => prev ? { ...prev, phone_number: e.target.value } : prev)
            }
          />
        </div>

        <input className="border rounded px-3 py-2 w-full text-sm"
          placeholder="CC Emails (comma-separated)"
          value={(editContactData as any)?.cc_store || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, cc_store: e.target.value } : prev)
          }
        />
      </div>

      {/* CC History */}
      {editCcHistory && editCcHistory.length > 0 && (
        <div className="border rounded p-3 bg-blue-50">
          <div className="text-xs font-semibold mb-2 text-gray-700">
            CC History (removed)
          </div>

          {editCcHistory.map((h: any, i: number) => (
            <div key={i} className="text-xs mb-2 border-b pb-2 last:border-0">
              <div className="flex items-center justify-between gap-2">
                <div className="flex-1">
                  <div className="font-semibold text-gray-600">
                    {h?.removed_at ? new Date(h.removed_at).toLocaleString() : ''}
                  </div>
                  <div className="text-gray-700">
                    {(h?.emails || []).join(', ')}
                  </div>
                  <div className="text-gray-500 text-xs">by: {h?.by || 'system'}</div>
                </div>

                <Button
                  size="xs"
                  onClick={() => {
                    const toRestore = h?.emails?.[0];
                    if (!toRestore) return;

                    setEditContactData(prev => {
                      if (!prev) return prev;
                      const current = (prev as any).cc_store || '';
                      const arr = current
                        .split(',')
                        .map((s: string) => s.trim())
                        .filter(Boolean);

                      if (!arr.includes(toRestore)) arr.push(toRestore);

                      return { ...prev, cc_store: arr.join(', ') };
                    });
                  }}
                >
                  Restore
                </Button>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Contact Details */}
      <div className="space-y-2">
        <h3 className="text-sm font-semibold text-gray-700">Contact Details</h3>

        <input className="border rounded px-3 py-2 w-full text-sm"
          placeholder="Organizer"
          value={editContactData?.organizer || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, organizer: e.target.value } : prev)
          }
        />

        <div>
          <label className="block text-xs font-medium mb-1">Nationality</label>
          <select className="border rounded px-3 py-2 w-full text-sm"
            value={editContactData?.nationality || ''}
            onChange={(e) =>
              setEditContactData(prev => prev ? { ...prev, nationality: e.target.value } : prev)
            }
          >
            <option value="">(select country)</option>
            {COUNTRIES.map(cn => <option key={cn} value={cn}>{cn}</option>)}
          </select>
        </div>

        <input className="border rounded px-3 py-2 w-full text-sm"
          placeholder="Speaker Type"
          value={editContactData?.speaker_type || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, speaker_type: e.target.value } : prev)
          }
        />

        <input className="border rounded px-3 py-2 w-full text-sm"
          placeholder="Workplace"
          value={(editContactData as any)?.workplace || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, workplace: e.target.value } : prev)
          }
        />
      </div>

      {/* Campaign */}
      <div className="space-y-2">
        <h3 className="text-sm font-semibold text-gray-700">Campaign</h3>

        <select className="border rounded px-3 py-2 w-full text-sm"
          value={editContactData?.stage || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, stage: e.target.value } : prev)
          }
        >
          <option value="">(select stage)</option>
          {STAGE_OPTIONS.map(s => <option key={s} value={s}>{s}</option>)}
        </select>

        <select className="border rounded px-3 py-2 w-full text-sm"
          value={editContactData?.status || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, status: e.target.value } : prev)
          }
        >
          <option value="">(select status)</option>
          {DEFAULT_STATUS_OPTIONS.map(s => <option key={s} value={s}>{s}</option>)}
        </select>

        <div>
          <label className="block text-xs font-medium mb-1">Campaign State</label>
          <select className="border rounded px-3 py-2 w-full text-sm"
            value={editContactData?.campaign_paused ? 'pause' : 'resume'}
            onChange={(e) =>
              setEditContactData(prev => prev ? { ...prev, campaign_paused: e.target.value === 'pause' } : prev)
            }
          >
            <option value="resume">Resume</option>
            <option value="pause">Pause</option>
          </select>
        </div>
      </div>

      {/* Hotel & Booking */}
      <div className="space-y-2">
        <h3 className="text-sm font-semibold text-gray-700">Hotel & Booking</h3>

        <input className="border rounded px-3 py-2 w-full text-sm"
          placeholder="Hotel Name"
          value={editContactData?.hotel_name || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, hotel_name: e.target.value } : prev)
          }
        />

        <input className="border rounded px-3 py-2 w-full text-sm"
          placeholder="Booking ID"
          value={(editContactData as any)?.booking_id || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, booking_id: e.target.value } : prev)
          }
        />

        <input className="border rounded px-3 py-2 w-full text-sm"
          placeholder="Supplier"
          value={editContactData?.supplier || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, supplier: e.target.value } : prev)
          }
        />

        <input className="border rounded px-3 py-2 w-full text-sm"
          placeholder="Payment Method"
          value={(editContactData as any)?.payment_method || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, payment_method: e.target.value } : prev)
          }
        />
      </div>

      {/* Additional Info */}
      <div className="space-y-2">
        <h3 className="text-sm font-semibold text-gray-700">Additional Info</h3>

        <input
          type="date"
          className="border rounded px-3 py-2 w-full text-sm"
          value={toInputDate(editContactData?.date)}
          onChange={(e) => {
            const v = e.target.value;
            if (!v) {
              setEditContactData(prev => prev ? { ...prev, date: '' } : prev);
              return;
            }
            const [yyyy, mm, dd] = v.split('-');
            const stored = `${dd}/${mm}/${yyyy}`;
            setEditContactData(prev => prev ? { ...prev, date: stored } : prev);
          }}
        />

        <textarea className="border rounded px-3 py-2 w-full text-sm"
          placeholder="Notes" rows={3}
          value={editContactData?.notes || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, notes: e.target.value } : prev)
          }
        />

        <input className="border rounded px-3 py-2 w-full text-sm"
          placeholder="Invoice Number"
          value={(editContactData as any)?.invoice_number || ''}
          onChange={(e) =>
            setEditContactData(prev => prev ? { ...prev, invoice_number: e.target.value } : prev)
          }
        />
      </div>
    </div>

    {/* FIXED FOOTER — ALWAYS AT BOTTOM */}
    <div className="border-t px-6 py-4 bg-gray-50 flex gap-2 sticky bottom-0 z-10">
      <Button onClick={handleModalSave} className="flex-1 bg-blue-600 hover:bg-blue-700">
        Confirm
      </Button>

      <Button variant="outline"
        onClick={() => {
          setEditModalOpen(false);
          setEditContactData(null);
        }}
        className="flex-1"
      >
        Cancel
      </Button>
    </div>

  </DialogContent>
</Dialog>

      {/* Custom Flow Modal (simple wrapper around existing modal component) */}
      {modalContactId !== null && (
        <CustomFlowModal
          visible={customFlowModalOpen}
          contactId={modalContactId}
          flowId={modalFlowId}
          initialSteps={modalInitialSteps}
          onCancel={() => { setCustomFlowModalOpen(false); setModalContactId(null); setModalInitialSteps([]); }}
          onSuccess={() => { setCustomFlowModalOpen(false); setModalContactId(null); onRefresh(); }}
        />
      )}
      {/* Custom Messages Modal */}
      {messagesModalContactId !== null && (
        <CustomMessagesModal
          contactId={messagesModalContactId}
          contactName={messagesModalContactName}
          isOpen={customMessagesModalOpen}
          onClose={() => { setCustomMessagesModalOpen(false); setMessagesModalContactId(null); }}
        />
      )}
      {/* Upload job progress modal */}
      {progressModalOpen && (
        <div className="fixed inset-0 flex items-center justify-center z-50">
          <div className="bg-white border rounded p-6 shadow-lg w-96">
            <h3 className="text-lg font-semibold mb-2">Validation Progress</h3>
            <div className="text-sm mb-4">Job: {jobProgress.jobId || '—'}</div>
            <div className="mb-2">Status: {jobProgress.status}</div>
            <div className="mb-4">
              <div className="w-full bg-gray-200 rounded h-4">
                <div className="bg-green-500 h-4 rounded" style={{ width: jobProgress.total ? `${Math.round((jobProgress.processed / jobProgress.total) * 100)}%` : '0%' }} />
              </div>
              <div className="text-xs mt-1">{jobProgress.processed}/{jobProgress.total} rows processed</div>
            </div>
            <div className="flex justify-end gap-2">
              <Button variant="outline" onClick={() => { setProgressModalOpen(false); }}>Close</Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

