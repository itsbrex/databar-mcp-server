/**
 * Type definitions for Databar API
 */

// ============================================================================
// Enrichment Types
// ============================================================================

export interface ChoiceItem {
  id: string;
  name: string;
}

export interface ParamChoices {
  mode: 'inline' | 'remote';
  items?: ChoiceItem[] | null;
  endpoint?: string | null;
}

export interface ChoicesResponse {
  items: ChoiceItem[];
  page: number;
  limit: number;
  has_next_page: boolean;
  total_count: number;
}

export interface EnrichmentParam {
  name: string;
  is_required: boolean;
  type_field: string;
  description: string;
  choices?: ParamChoices | null;
}

export interface EnrichmentResponseField {
  name: string;
  type_field: string;
}

export interface PaginationInfo {
  supported: boolean;
  per_page?: number | null;
}

export interface EnrichmentCategoryInfo {
  id: number;
  name: string;
}

export interface Enrichment {
  id: number;
  name: string;
  description: string;
  data_source: string;
  price: number;
  auth_method: string;
  rank?: number;
  search_keywords?: string;
  category?: EnrichmentCategoryInfo[];
  params?: EnrichmentParam[];
  response_fields?: EnrichmentResponseField[];
  pagination?: PaginationInfo;
}

export interface PaginationOptions {
  pages: number;
}

export interface EnrichmentRunRequest {
  params: Record<string, any>;
  pagination?: PaginationOptions;
}

export interface BulkEnrichmentRunRequest {
  params: Record<string, any>[];
  pagination?: PaginationOptions;
}

export interface EnrichmentRunResponse {
  task_id: string;
  status: string;
}

// ============================================================================
// Task Types
// ============================================================================

export type TaskStatus =
  | 'processing'
  | 'completed'
  | 'success'
  | 'failed'
  | 'error'
  | 'gone';

export interface TaskResponse {
  task_id: string;
  status: TaskStatus;
  data?: any;
  error?: string | string[] | null;
}

// ============================================================================
// Table Types
// ============================================================================

export interface Table {
  identifier: string;
  name: string;
  created_at: string;
  updated_at: string;
}

export interface Column {
  identifier: string;
  internal_name: string;
  additional_intenal_name: string | null;
  name: string;
  type_of_value: string;
  data_processor_id: number | null;
}

export interface TableEnrichment {
  id: string;
  name: string;
}

export interface AddEnrichmentRequest {
  enrichment: number;
  mapping: Record<string, { value: string; type: string }>;
  launch_strategy?: 'run_on_click' | 'run_on_update';
}

export interface RunTableEnrichmentRequest {
  run_strategy?: 'run_all' | 'run_empty' | 'run_errors';
  row_ids?: string[];
}

export interface DeleteRowsRequest {
  row_ids: string[];
}

export interface CreateColumnRequest {
  name: string;
  type?: string;
}

export interface CreateColumnResponse {
  identifier: string;
  name: string;
  type_of_value: string;
}

export interface AddWaterfallRequest {
  waterfall: string;
  enrichments: number[];
  mapping: Record<string, string>;
  email_verifier?: number | null;
}

export interface AddWaterfallResponse {
  id: number;
  waterfall_name: string;
}

export interface InstalledWaterfall {
  id: number;
  waterfall_name: string;
}

// ============================================================================
// Row Types
// ============================================================================

export interface CreateRowsRecord {
  fields: Record<string, any>;
  insert?: { position?: 'TOP' | 'BOTTOM' };
}

export interface CreateRowsDedupeOptions {
  enabled?: boolean;
  keys?: string[];
}

export interface CreateRowsOptions {
  allowNewColumns?: boolean;
  typecast?: boolean;
  dedupe?: CreateRowsDedupeOptions;
}

export interface CreateRowsRequest {
  records: CreateRowsRecord[];
  options?: CreateRowsOptions;
}

export interface CreateRowsCreatedItem {
  rowId: string;
  fields?: Record<string, any>;
}

export interface CreateRowsErrorItem {
  index: number;
  error: { code: string; message: string };
}

export interface CreateRowsResponse {
  created: CreateRowsCreatedItem[];
  errors: CreateRowsErrorItem[];
}

export interface PatchRowOperation {
  id: string;
  fields: Record<string, any>;
}

export interface PatchRowsRequest {
  rows: PatchRowOperation[];
  return_rows?: boolean;
  overwrite?: boolean;
}

export interface PerRowError {
  code: string;
  message: string;
  matches?: number;
}

export interface PatchRowsResultItem {
  id: string;
  ok: boolean;
  row_data?: Record<string, any>;
  error?: PerRowError;
}

export interface PatchRowsResponse {
  results: PatchRowsResultItem[];
}

export interface UpsertRowOperation {
  key: Record<string, any>;
  fields: Record<string, any>;
}

export interface UpsertRowsRequest {
  rows: UpsertRowOperation[];
}

export interface UpsertRowsResultItem {
  index: number;
  match_on: Record<string, any>;
  id: string;
  action: 'created' | 'updated';
  ok: boolean;
  error?: PerRowError;
}

export interface UpsertRowsResponse {
  results: UpsertRowsResultItem[];
}

// ============================================================================
// Exporter Types
// ============================================================================

export interface ExporterInfo {
  id: number;
  name: string;
  description: string;
  dataset: number;
}

export interface ExporterParam {
  name: string;
  is_required: boolean;
  type_field: string;
  description: string;
}

export interface ExporterResponseField {
  name: string;
  display_name?: string | null;
  type_field: string;
}

export interface ExporterDetails extends ExporterInfo {
  params?: ExporterParam[];
  response_fields?: ExporterResponseField[];
}

export interface AddExporterRequest {
  exporter: number;
  mapping: Record<string, { value: string; type: string }>;
  launch_strategy?: 'run_on_click' | 'run_on_update';
}

export interface AddExporterResponse {
  id: number;
  exporter_name: string;
}

export interface InstalledExporter {
  id: number;
  name: string;
}

export interface RunTableExporterRequest {
  run_strategy?: 'run_all' | 'run_empty' | 'run_errors';
}

// ============================================================================
// Waterfall Types
// ============================================================================

export interface WaterfallInputParam {
  name: string;
  type: string;
  required: boolean;
}

export interface WaterfallOutputField {
  name: string;
  label: string;
  type: string;
}

export interface WaterfallEnrichment {
  id: number;
  name: string;
  description: string;
  price: string;
  params: string[];
}

export interface Waterfall {
  identifier: string;
  name: string;
  description: string;
  input_params: WaterfallInputParam[];
  output_fields: WaterfallOutputField[];
  available_enrichments: WaterfallEnrichment[];
  is_email_verifying: boolean;
  email_verifiers: any[];
}

export interface WaterfallRunRequest {
  params: Record<string, any>;
  enrichments: number[];
  email_verifier?: number | null;
}

export interface BulkWaterfallRunRequest {
  params: Record<string, any>[];
  enrichments: number[];
  email_verifier?: number | null;
}

export interface WaterfallStep {
  action: string;
  provider_logo?: string;
  result: string;
  details?: string | null;
  cost: string;
  provider: string;
  is_verified: boolean;
}

export interface WaterfallResultData {
  enrichment_data: Record<string, any>;
  result: Record<string, any>;
  steps: WaterfallStep[];
  result_field: string[];
}

export interface WaterfallTaskResponse {
  task_id: string;
  status: TaskStatus;
  data?: WaterfallResultData[];
  error?: string | null;
}

// ============================================================================
// Folder Types
// ============================================================================

export interface Folder {
  id: number;
  name: string;
  created_at?: string;
  updated_at?: string;
  table_count?: number;
}

// ============================================================================
// User Types
// ============================================================================

export interface User {
  first_name: string | null;
  email: string;
  balance: number;
  plan: string;
}

// ============================================================================
// Error Types
// ============================================================================

export interface DatabarErrorDetail {
  type: string;
  loc: string[];
  msg: string;
  input: any;
}

export interface DatabarError {
  detail?: DatabarErrorDetail[] | string;
  error?: string;
  details?: string;
}

// ============================================================================
// Cache Types
// ============================================================================

export interface CacheEntry {
  data: any;
  timestamp: number;
}

// ============================================================================
// Config Types
// ============================================================================

export interface DatabarConfig {
  apiKey: string;
  baseUrl: string;
  cacheTtlHours: number;
  maxPollAttempts: number;
  pollIntervalMs: number;
}

