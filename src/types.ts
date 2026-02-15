export interface Event {
  id: number;
  event_name: string;
  org_name?: string;
  event_url?: string;
  org_id?: number;
  note?: string;
  month?: string;
  sender_email?: string;
  city?: string;
  venue?: string;
  date2?: string;
  contacts?: number;  // contact count
  contacts_list?: CampaignContact[];  // list of contacts for this event
}

export interface CampaignContact {
  id: number;
  event_id: number;
  name?: string;
  email?: string;
  cc_store?: string;
  date?: string;
  status?: string;
  stage?: string;
  link?: string;
  campaign_paused?: boolean;
  created_at?: string;
  forms_link?: string;
  payment_link?: string;
  invoice_number?: string;
  trigger?: string;
  email_error?: string;
  last_error_at?: string;
  notes?: string;
  nationality?: string;
  workplace?: string;
  supplier?: string;
  payment_method?: string;
  organizer?: string;
  hotel_name?: string;
  phone_number?: string;
  booking_id?: string;
  country_code?: string;
}

export interface ContactEventRelation {
  event_id: number;
  contact_id: number;
  event_name: string;
  status?: string;
  stage?: string;
}

export interface ContactRelationsData {
  email: string;
  total_events: number;
  relations: ContactEventRelation[];
}

export interface MatchResult {
  type: 'name' | 'email' | 'organization';
  excel_value: string;
  db_value: string;
  event_name: string;
  role?: 'participant' | 'organizer';
  stage?: string;
  status?: string;
  match_details?: string;
}

export interface GroupedMatches {
  nameMatches: MatchResult[];
  emailMatches: MatchResult[];
  orgMatches: MatchResult[];
}

export interface SelectedCustomers {
  [key: number]: CampaignContact;
}

export const DEFAULT_STATUS_OPTIONS = [
  'No Response',
  'Not Interested',
  'Covered',
  'Follow-up',
  'First Reminder',
  'Second Reminder',
  'Third Reminder',
  'Fourth Reminder',
  'Fifth Reminder',
  'Sixth Reminder',
  // Forms reminders (backend status tokens)
  'forms_initial_sent',
  'forms_reminder1_sent',
  'forms_reminder2_sent',
  'forms_reminder3_sent',
  // Payments reminders (backend status tokens)
  'payments_initial_sent',
  'payments_reminder1_sent',
  'payments_reminder2_sent',
  'payments_reminder3_sent',
  'payments_reminder4_sent',
  'payments_reminder5_sent',
  'payments_reminder6_sent',
  'Completed',
  'Cancelled',
  'Pending',
  'OOO',
  'custom'
] as const;

export const STAGE_OPTIONS = [
  'First Message',
  'Forms',
  'Payments',
  'Sepa BT payment',
  'Rh BT payment',
  'Invoice & Confirmation',
  'Payment Due',
  'Completed',
  'Problem',
  'Wrong Person',
  'Mail Delivery',
  'HCN',
  "Supplier's Payment",
  'custom'
] as const;

export const COUNTRIES = [
  'Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola', 'Antigua and Barbuda', 'Argentina', 'Armenia', 'Australia', 'Austria',
  'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bhutan',
  'Bolivia', 'Bosnia and Herzegovina', 'Botswana', 'Brazil', 'Brunei', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cabo Verde',
  'Cambodia', 'Cameroon', 'Canada', 'Central African Republic', 'Chad', 'Chile', 'China', 'Colombia', 'Comoros',
  'Congo', 'Costa Rica', 'Croatia', 'Cuba', 'Cyprus', 'Czech Republic', 'Denmark', 'Djibouti', 'Dominica',
  'Dominican Republic', 'East Timor', 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia',
  'Eswatini', 'Ethiopia', 'Fiji', 'Finland', 'France', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Greece',
  'Grenada', 'Guatemala', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti', 'Honduras', 'Hungary', 'Iceland', 'India',
  'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy', 'Jamaica', 'Japan', 'Jordan', 'Kazakhstan', 'Kenya',
  'Kiribati', 'Korea, North', 'Korea, South', 'Kosovo', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Latvia', 'Lebanon', 'Lesotho',
  'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali',
  'Malta', 'Marshall Islands', 'Mauritania', 'Mauritius', 'Mexico', 'Micronesia', 'Moldova', 'Monaco', 'Mongolia',
  'Montenegro', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nauru', 'Nepal', 'Netherlands', 'New Zealand',
  'Nicaragua', 'Niger', 'Nigeria', 'North Macedonia', 'Norway', 'Oman', 'Pakistan', 'Palau', 'Palestine', 'Panama',
  'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Poland', 'Portugal', 'Qatar', 'Romania', 'Russia', 'Rwanda',
  'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 'Sao Tome and Principe',
  'Saudi Arabia', 'Senegal', 'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 'Slovakia', 'Slovenia',
  'Solomon Islands', 'Somalia', 'South Africa', 'South Sudan', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname', 'Sweden',
  'Switzerland', 'Syria', 'Taiwan', 'Tajikistan', 'Tanzania', 'Thailand', 'Togo', 'Tonga', 'Trinidad and Tobago',
  'Tunisia', 'Turkey', 'Turkmenistan', 'Tuvalu', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom',
  'United States', 'Uruguay', 'Uzbekistan', 'Vanuatu', 'Vatican City', 'Venezuela', 'Vietnam', 'Yemen', 'Zambia', 'Zimbabwe'
] as const;

export const SUPPLIER_OPTIONS = [
  'RateHawk',
  'TBO',
  'Rezlive',
  'Webbeds',
  'Bedsonline',
  'Within Earth',
  'custom'
] as const;
