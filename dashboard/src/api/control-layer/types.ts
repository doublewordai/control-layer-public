import { z } from "zod";

export interface PaginatedResponse<T> {
  data: T[];
  total_count: number;
  skip: number;
  limit: number;
}

export type ModelType = "CHAT" | "EMBEDDINGS" | "RERANKER";
export type ModelDisplayCategory = "generation" | "embedding" | "ocr";

export type LoadBalancingStrategy = "weighted_random" | "priority";

export type JitterStrategy = "none" | "full";

export type ReasoningEffort =
  | "none"
  | "minimal"
  | "low"
  | "medium"
  | "high"
  | "xhigh"
  | "max";

export interface ReasoningWrite {
  target_path: string;
  values: Partial<Record<ReasoningEffort, unknown>>;
}

export interface ReasoningTranslation {
  unsupported_efforts: ReasoningEffort[];
  writes: ReasoningWrite[];
}

export interface ReasoningTranslationConfig {
  chat_completions?: ReasoningTranslation;
  responses?: ReasoningTranslation;
}

export type ReasoningSurfaceOverride =
  | { mode: "inherit" }
  | { mode: "disabled" }
  | { mode: "override"; translation: ReasoningTranslation };

export interface ReasoningTranslationOverrides {
  chat_completions: ReasoningSurfaceOverride;
  responses: ReasoningSurfaceOverride;
}

export interface BackoffConfig {
  initial_ms: number;
  max_ms: number;
  factor: number;
  jitter: JitterStrategy;
}

export interface FallbackConfig {
  enabled: boolean;
  on_rate_limit: boolean;
  on_status: number[];
  realtime_on_status?: number[];
  with_replacement: boolean;
  max_attempts: number | null;
  backoff?: BackoffConfig | null;
  max_total_backoff_ms?: number | null;
}

export interface ComponentEndpointSummary {
  id: string;
  name: string;
}

export interface ComponentModelSummary {
  id: string;
  alias: string;
  model_name: string;
  description?: string;
  model_type?: ModelType;
  endpoint?: ComponentEndpointSummary;
  trusted?: boolean;
}

export interface ModelComponent {
  weight: number;
  enabled: boolean;
  sort_order: number;
  created_at: string;
  pool: "default" | "completions";
  model: ComponentModelSummary;
}

export interface AddComponentRequest {
  deployed_model_id: string;
  weight?: number;
  enabled?: boolean;
  sort_order?: number;
}

export interface UpdateComponentRequest {
  weight?: number;
  enabled?: boolean;
  sort_order?: number;
}
export type AuthSource = "vouch" | "native" | "system" | "proxy-header";
export type Role =
  | "PlatformManager"
  | "RequestViewer"
  | "StandardUser"
  | "BillingManager"
  | "BatchAPIUser"
  | "ConnectionsUser";
export type ApiKeyPurpose = "platform" | "realtime" | "batch" | "playground";
export type TariffApiKeyPurpose = "realtime" | "batch" | "playground";

export interface ServingPreset {
  ttft_ms: number;
  itl_ms: number;
  priority: number;
}
export type EndpointKind = "dynamo" | "hosted" | "external";

export type TrafficRoutingAction =
  | { type: "deny" }
  | { type: "redirect"; target: string };

export interface TrafficRoutingRule {
  api_key_purpose: ApiKeyPurpose;
  action: TrafficRoutingAction;
}

export type BatchCompletionWindow = "24h" | "1h";

export interface ConfigResponse {
  region: string;
  organization: string;
  payment_enabled: boolean;

  docs_jsonl_url?: string;
  docs_url: string;
  batches?: {
    enabled: boolean;
    allowed_completion_windows: string[];
    allowed_url_paths: string[];
    async_requests: {
      enabled: boolean;
      completion_window: string;
    };
  };
  ai_api_base_url?: string;
  onwards: {
    strict_mode: boolean;
  };
  scheduling_toleration_presets?: SchedulingTolerationPreset[];
}

export interface ModelTimeSeriesPoint {
  timestamp: string;
  requests: number;
}

export interface ModelMetrics {
  avg_latency_ms?: number;
  total_requests: number;
  total_input_tokens: number;
  total_output_tokens: number;
  last_active_at?: string;
  time_series?: ModelTimeSeriesPoint[];
}

export interface ModelProbeStatus {
  probe_id?: string;
  active: boolean;
  interval_seconds?: number;
  last_check?: string;
  last_success?: boolean;
  uptime_percentage?: number;
}

export interface ModelTariff {
  id: string;
  deployed_model_id: string;
  name: string;
  input_price_per_token: string;
  output_price_per_token: string;
  valid_from: string;
  valid_until?: string | null;
  api_key_purpose?: TariffApiKeyPurpose | null;
  completion_window?: string | null;
  is_active: boolean;
  organization_id?: string | null;
  serving_class?: string | null;
}

export interface CachePricing {
  enabled: boolean;
  write_multiplier_5m: string | null;
  write_multiplier_1h: string | null;
  write_multiplier_24h: string | null;
  read_multiplier: string | null;
  min_prefix_tokens: number | null;
  valid_from: string | null;
  valid_until: string | null;
}

export interface CachePricingUpdate {
  write_multiplier_5m?: string;
  write_multiplier_1h?: string;
  write_multiplier_24h?: string;
  read_multiplier?: string;
  min_prefix_tokens?: number;
}

export interface TariffDefinition {
  name: string;
  input_price_per_token: string;
  output_price_per_token: string;
  api_key_purpose?: TariffApiKeyPurpose | null;
  completion_window?: string | null;
}

export interface ModelMetadata {
  provider?: string;
  display_category?: ModelDisplayCategory;
  intelligence_index?: number;
  context_window?: number;
  released_at?: string;
  attribution?: string;
  quantization?: string;
  extra?: {
    evaluations?: Record<string, number>;
    summary?: string;
    use_cases?: string[];
    model_order?: number;
    deployment_providers?: string[];
  };
}

export interface ProviderDisplayConfig {
  provider_key: string;
  display_name: string;
  icon?: string | null;
  model_count: number;
  configured: boolean;
  created_by?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export function providerIconUrl(
  providerKey: string,
  iconValue: string | null | undefined,
): string | undefined {
  const icon = iconValue?.trim();
  if (!icon) return undefined;
  if (icon.startsWith("https://")) {
    return `/admin/api/v1/provider-display-configs/${encodeURIComponent(providerKey)}/icon`;
  }
  return icon;
}

export interface ProviderDisplayConfigCreateRequest {
  provider_key: string;
  display_name?: string;
  icon?: string;
}

export interface ProviderDisplayConfigUpdateRequest {
  display_name?: string;
  icon?: string | null;
}

export interface Model {
  id: string;
  alias: string;
  display_name?: string | null;
  model_name: string;
  description?: string | null;
  model_type?: ModelType | null;
  capabilities?: string[] | null;
  hosted_on?: string | null;
  realtime_inflight_limit?: number | null;
  batch_capacity?: number | null;
  throughput?: number | null;
  groups?: Group[];
  metrics?: ModelMetrics;
  status?: ModelProbeStatus;
  tariffs?: ModelTariff[];
  endpoint?: Endpoint;
  is_composite?: boolean;
  provisioning_source?: string;
  lb_strategy?: LoadBalancingStrategy | null;
  fallback?: FallbackConfig | null;
  components?: ModelComponent[];
  sanitize_responses?: boolean | null;
  trusted?: boolean;
  reasoning_translation_overrides?: ReasoningTranslationOverrides | null;
  traffic_routing_rules?: TrafficRoutingRule[] | null;
  allowed_batch_completion_windows?: string[] | null;
  metadata?: ModelMetadata | null;
}

export interface ServingOverlay {
  organization_id: string;
  organization_name: string;
  organization_display_name?: string | null;
  deployed_model_id: string;
  alias: string;
  targets?: ServingPreset;
  self_hosted_only?: boolean;
  provisioning_source?: string;
  updated_at: string;
}

export interface OrganizationCacheTariff {
  deployed_model_id: string;
  alias: string;
  write_multiplier_5m: string;
  write_multiplier_1h: string;
  write_multiplier_24h: string;
  read_multiplier: string;
  serving_class?: string;
  valid_from: string;
}

export interface OrganizationServing {
  organization_id: string;
  self_hosted_only: boolean;
  overlays: ServingOverlay[];
  tariffs: ModelTariff[];
  model_aliases?: Record<string, string>;
  cache_tariffs: OrganizationCacheTariff[];
}

export interface StandardModelCreate {
  type: "standard";
  model_name: string;
  alias?: string;
  display_name?: string;
  hosted_on: string;
  description?: string;
  model_type?: ModelType;
  capabilities?: string[];
  batch_capacity?: number;
  throughput?: number;
  trusted?: boolean;
  reasoning_translation_overrides?: ReasoningTranslationOverrides;
  traffic_routing_rules?: TrafficRoutingRule[];
  allowed_batch_completion_windows?: string[];
}

export interface VirtualModelCreate {
  type: "composite" | "virtual";
  model_name: string;
  alias?: string;
  display_name?: string;
  description?: string;
  model_type?: ModelType;
  capabilities?: string[];
  realtime_inflight_limit?: number;
  batch_capacity?: number;
  throughput?: number;
  lb_strategy?: LoadBalancingStrategy;
  fallback_enabled?: boolean;
  fallback_on_rate_limit?: boolean;
  fallback_on_status?: number[];
  fallback_realtime_on_status?: number[];
  fallback_with_replacement?: boolean;
  fallback_max_attempts?: number | null;
  backoff_enabled?: boolean;
  backoff_initial_ms?: number;
  backoff_max_ms?: number;
  backoff_factor?: number;
  backoff_jitter?: JitterStrategy;
  backoff_max_total_ms?: number | null;
  sanitize_responses?: boolean;
  traffic_routing_rules?: TrafficRoutingRule[];
  allowed_batch_completion_windows?: string[];
}

export type CompositeModelCreate = VirtualModelCreate;

export type ModelCreate = StandardModelCreate | VirtualModelCreate;

export interface Endpoint {
  id: string;
  name: string;
  description?: string;
  url: string;
  created_by: string;
  created_at: string;
  updated_at: string;
  requires_api_key: boolean;
  model_filter?: string[] | null;
  auth_header_name: string;
  auth_header_prefix: string;
  reasoning_translation?: ReasoningTranslationConfig | null;
  kind?: EndpointKind;
}

export interface EndpointSyncResponse {
  endpoint_id: string;
  changes_made: number;
  new_models_created: number;
  models_reactivated: number;
  models_deactivated: number;
  models_deleted: number;
  total_models_fetched: number;
  filtered_models_count: number;
  synced_at: string;
}

export interface Group {
  id: string;
  name: string;
  description?: string;
  created_by?: string;
  created_at?: string;
  updated_at?: string;
  users?: User[];
  models?: Model[];
  source: string;
}

export interface User {
  id: string;
  username: string;
  external_user_id: string;
  email: string;
  display_name?: string;
  avatar_url?: string;
  is_admin?: boolean;
  roles: Role[];
  groups?: Group[];
  created_at: string;
  updated_at: string;
  auth_source: AuthSource;
  credit_balance?: number;
  has_payment_provider_id: boolean;
  batch_notifications_enabled: boolean;
  low_balance_threshold: number | null;
  auto_topup_amount: number | null;
  auto_topup_threshold: number | null;
  has_auto_topup_payment_method: boolean;
  auto_topup_monthly_limit: number | null;
  zero_data_retention: boolean;
  self_hosted_only?: boolean;
  toleration_preset?: string | null;
  user_type?: "individual" | "organization";
  organizations?: OrganizationSummary[];
  active_organization_id?: string;
  last_login?: string | null;
  onboarding_redirect_url?: string;
}

export type SpendLimitInterval = "daily" | "weekly" | "monthly";

export interface ApiKey {
  id: string;
  name: string;
  description?: string;
  purpose: ApiKeyPurpose;
  created_at: string;
  last_used?: string;
  created_by: string;
  spend_limit?: string | null;
  spend_limit_interval?: SpendLimitInterval | null;
  spend?: string | null;
  total_spend?: string | null;
  resets_at?: string | null;
  secret_revealed_at?: string | null;
}

export interface ApiKeyCreateResponse extends ApiKey {
  key: string;
}

export interface ApiKeySecretResponse {
  key: string;
}

export type ModelsInclude = string;
export type GroupsInclude = "users" | "models" | "users,models";
export type UsersInclude = "groups";

export type ModelSortField =
  | "created_at"
  | "alias"
  | "intelligence_index"
  | "released_at"
  | "context_window"
  | "provider"
  | "price_from";

export type SortDirection = "asc" | "desc";

export interface ModelFacets {
  providers: string[];
  capabilities: string[];
  model_types: string[];
}

export interface ModelListResponse extends PaginatedResponse<Model> {
  facets?: ModelFacets;
}

export interface ModelsQuery {
  skip?: number;
  limit?: number;
  endpoint?: string;
  group?: string;
  include?: ModelsInclude;
  accessible?: boolean;
  search?: string;
  is_composite?: boolean;
  provider?: string;
  model_type?: ModelType;
  capability?: string;
  sort?: ModelSortField;
  sort_direction?: SortDirection;
}

export interface EndpointsQuery {
  skip?: number;
  limit?: number;
  enabled?: boolean;
}

export interface GroupsQuery {
  skip?: number;
  limit?: number;
  include?: GroupsInclude;
  search?: string;
}

export interface UsersQuery {
  skip?: number;
  limit?: number;
  include?: UsersInclude;
  search?: string;
}

export interface UserCreateRequest {
  username: string;
  email: string;
  display_name?: string;
  avatar_url?: string;
  roles: Role[];
}

export interface GroupCreateRequest {
  name: string;
  description?: string;
}

export interface ApiKeyCreateRequest {
  name: string;
  description?: string;
  purpose: ApiKeyPurpose;
  spend_limit?: string | null;
  spend_limit_interval?: SpendLimitInterval | null;
  member_id?: string;
}

export interface ApiKeyUpdateRequest {
  name?: string;
  description?: string;
  spend_limit?: string | null;
  spend_limit_interval?: SpendLimitInterval | null;
  reset_window?: boolean;
}

export interface ApiKeysQuery {
  skip?: number;
  limit?: number;
  created_by?: string;
}

export interface UserUpdateRequest {
  display_name?: string;
  avatar_url?: string;
  roles?: Role[];
  batch_notifications_enabled?: boolean;
  low_balance_threshold?: number | null;
  auto_topup_amount?: number | null;
  auto_topup_threshold?: number | null;
  auto_topup_monthly_limit?: number | null;
  zero_data_retention?: boolean;
  self_hosted_only?: boolean;
  toleration_preset?: string | null;
}

export interface GroupUpdateRequest {
  name?: string;
  description?: string;
}

export interface ModelUpdateRequest {
  alias?: string;
  display_name?: string | null;
  description?: string | null;
  model_type?: ModelType | null;
  capabilities?: string[] | null;
  realtime_inflight_limit?: number | null;
  batch_capacity?: number | null;
  throughput?: number | null;
  tariffs?: TariffDefinition[];
  lb_strategy?: LoadBalancingStrategy | null;
  fallback_enabled?: boolean | null;
  fallback_on_rate_limit?: boolean | null;
  fallback_on_status?: number[] | null;
  fallback_realtime_on_status?: number[] | null;
  fallback_with_replacement?: boolean | null;
  fallback_max_attempts?: number | null;
  backoff_enabled?: boolean | null;
  backoff_initial_ms?: number | null;
  backoff_max_ms?: number | null;
  backoff_factor?: number | null;
  backoff_jitter?: JitterStrategy | null;
  backoff_max_total_ms?: number | null;
  sanitize_responses?: boolean | null;
  trusted?: boolean | null;
  reasoning_translation_overrides?: ReasoningTranslationOverrides | null;
  traffic_routing_rules?: TrafficRoutingRule[] | null;
  allowed_batch_completion_windows?: string[] | null;
  metadata?: ModelMetadata | null;
}

export interface EndpointCreateRequest {
  name: string;
  description?: string;
  url: string;
  api_key?: string;
  model_filter?: string[];
  alias_mapping?: Record<string, string>;
  auth_header_name?: string;
  auth_header_prefix?: string;
  sync?: boolean;
  skip_fetch?: boolean;
  reasoning_translation?: ReasoningTranslationConfig;
  kind?: EndpointKind;
}

export interface EndpointUpdateRequest {
  name?: string;
  description?: string;
  url?: string;
  api_key?: string | null;
  model_filter?: string[] | null;
  alias_mapping?: Record<string, string>;
  auth_header_name?: string;
  auth_header_prefix?: string;
  reasoning_translation?: ReasoningTranslationConfig | null;
  kind?: EndpointKind;
}

export type EndpointValidateRequest =
  | {
      type: "new";
      url: string;
      api_key?: string;
      auth_header_name?: string;
      auth_header_prefix?: string;
    }
  | {
      type: "existing";
      endpoint_id: string;
    };

export interface AvailableModel {
  id: string;
  created: number;
  object: "model";
  owned_by: string;
}

export interface AvailableModelsResponse {
  object: "list";
  data: AvailableModel[];
}

export interface EndpointValidateResponse {
  status: "success" | "error";
  models?: AvailableModelsResponse;
  error?: string;
}


export interface HttpRequest {
  id: number;
  timestamp: string;
  method: string;
  uri: string;
  headers: Record<string, any>;
  body?: AiRequest;
  created_at: string;
}

export interface HttpResponse {
  id: number;
  timestamp: string;
  status_code: number;
  headers: Record<string, any>;
  body?: AiResponse;
  duration_ms: number;
  created_at: string;
}

export interface RequestResponsePair {
  request: HttpRequest;
  response?: HttpResponse;
}

export interface ListRequestsResponse {
  requests: RequestResponsePair[];
}

export interface AnalyticsEntry {
  id: number;
  timestamp: string;
  method: string;
  uri: string;
  model?: string;
  status_code?: number;
  duration_ms?: number;
  prompt_tokens?: number;
  completion_tokens?: number;
  reasoning_tokens?: number;
  total_tokens?: number;
  response_type?: string;
  fusillade_batch_id?: string;
  input_price_per_token?: string;
  output_price_per_token?: string;
  custom_id?: string;
}

export interface ListAnalyticsResponse {
  entries: AnalyticsEntry[];
}

export type AiRequest =
  | { type: "chat_completions"; data: ChatCompletionRequest }
  | { type: "completions"; data: CompletionRequest }
  | { type: "embeddings"; data: EmbeddingRequest }
  | { type: "rerank"; data: RerankRequest }
  | { type: "other"; data: any };

export type AiResponse =
  | { type: "chat_completions"; data: ChatCompletionResponse }
  | { type: "chat_completions_stream"; data: ChatCompletionChunk[] }
  | { type: "completions"; data: CompletionResponse }
  | { type: "embeddings"; data: EmbeddingResponse }
  | { type: "rerank"; data: RerankResponse }
  | { type: "other"; data: any };

export interface ChatCompletionMessage {
  role: "system" | "user" | "assistant";
  content: string;
}

export interface ChatCompletionRequest {
  model: string;
  messages: ChatCompletionMessage[];
  temperature?: number;
  max_completion_tokens?: number;
  stream?: boolean;
}

export interface ChatCompletionResponse {
  id: string;
  object: string;
  created: number;
  model: string;
  choices: {
    index: number;
    message: ChatCompletionMessage;
    finish_reason: string;
  }[];
  usage?: {
    prompt_tokens: number;
    completion_tokens: number;
    total_tokens: number;
  };
}

export interface ChatCompletionChunk {
  id: string;
  object: string;
  created: number;
  model: string;
  choices: {
    index: number;
    delta: Partial<ChatCompletionMessage>;
    finish_reason?: string;
  }[];
  usage?: {
    prompt_tokens: number;
    completion_tokens: number;
    total_tokens: number;
  };
}

export interface CompletionRequest {
  model: string;
  prompt: string;
  temperature?: number;
  max_tokens?: number;
}

export interface CompletionResponse {
  id: string;
  object: string;
  created: number;
  model: string;
  choices: {
    index: number;
    text: string;
    finish_reason: string;
  }[];
  usage?: {
    prompt_tokens: number;
    completion_tokens: number;
    total_tokens: number;
  };
}

export interface EmbeddingRequest {
  model: string;
  input: string | string[];
}

export interface EmbeddingResponse {
  object: string;
  data: {
    index: number;
    embedding: number[];
  }[];
  model: string;
  usage: {
    prompt_tokens: number;
    total_tokens: number;
  };
}

export interface RerankRequest {
  model: string;
  query: string;
  documents: string[];
}

export interface RerankResponse {
  id: string;
  model: string;
  usage: {
    total_tokens: number;
  };
  results: {
    index: number;
    document: {
      text: string;
      multi_modal: any | null;
    };
    relevance_score: number;
  }[];
}

export interface ListRequestsQuery {
  skip?: number;
  limit?: number;
  method?: string;
  uri_pattern?: string;
  status_code?: number;
  status_code_min?: number;
  status_code_max?: number;
  min_duration_ms?: number;
  max_duration_ms?: number;
  timestamp_after?: string;
  timestamp_before?: string;
  order_desc?: boolean;
  model?: string;
  fusillade_batch_id?: string;
  custom_id?: string;
}

export const listRequestsQuerySchema = z.object({
  skip: z.number().min(0).optional(),
  limit: z.number().min(1).max(1000).optional(),
  method: z.string().optional(),
  uri_pattern: z.string().optional(),
  status_code: z.number().optional(),
  status_code_min: z.number().optional(),
  status_code_max: z.number().optional(),
  min_duration_ms: z.number().min(0).optional(),
  max_duration_ms: z.number().min(0).optional(),
  timestamp_after: z.string().optional(),
  timestamp_before: z.string().optional(),
  order_desc: z.boolean().optional(),
});

export type ListRequestsQueryValidated = z.infer<
  typeof listRequestsQuerySchema
>;

export interface StatusCodeBreakdown {
  status: string;
  count: number;
  percentage: number;
}

export interface ModelUsage {
  model: string;
  count: number;
  percentage: number;
  avg_latency_ms: number;
}

export interface TimeSeriesPoint {
  timestamp: string;
  duration_minutes?: number;
  requests: number;
  input_tokens: number;
  output_tokens: number;
  avg_latency_ms?: number | null;
  p95_latency_ms?: number | null;
  p99_latency_ms?: number | null;
}

export interface RequestsAggregateResponse {
  total_requests: number;
  model?: string;
  status_codes: StatusCodeBreakdown[];
  models?: ModelUsage[];
  time_series: TimeSeriesPoint[];
}

export interface PendingRequestCountsQuery {
  window?: string;
  service_tiers?: string;
}

export type PendingRequestCountsByModelAndWindow = Record<
  string,
  Record<string, number>
>;

export interface UserUsage {
  user_id?: string;
  user_email?: string;
  request_count: number;
  total_tokens: number;
  input_tokens: number;
  output_tokens: number;
  total_cost?: number;
  last_active_at?: string;
}

export interface ModelUserUsageResponse {
  model: string;
  start_date: string;
  end_date: string;
  total_requests: number;
  total_tokens: number;
  total_cost?: number;
  users: UserUsage[];
}

export interface ModelBreakdownEntry {
  model: string;
  input_tokens: number;
  output_tokens: number;
  cost: string;
  request_count: number;
}

export interface UserBatchUsageResponse {
  total_input_tokens: number;
  total_output_tokens: number;
  total_request_count: number;
  total_batch_count: number;
  avg_requests_per_batch: number;
  total_cost: string;
  estimated_realtime_cost: string;
  by_model: ModelBreakdownEntry[];
}

export interface LoginRequest {
  email: string;
  password: string;
}

export interface RegisterRequest {
  username: string;
  email: string;
  password: string;
  display_name?: string;
}

export interface AuthResponse {
  user: UserResponse;
  message: string;
}

export interface AuthSuccessResponse {
  message: string;
}

export interface RegistrationInfo {
  enabled: boolean;
  message: string;
}

export interface LoginInfo {
  enabled: boolean;
  message: string;
}

export interface PasswordResetRequest {
  email: string;
}

export interface PasswordResetConfirmRequest {
  token_id: string;
  token: string;
  new_password: string;
}

export interface ChangePasswordRequest {
  current_password: string;
  new_password: string;
}

export type UserResponse = User;


export type TransactionType =
  | "admin_grant"
  | "admin_removal"
  | "usage"
  | "purchase";

export interface Transaction {
  id: string;
  user_id: string;
  transaction_type: TransactionType;
  batch_id?: string;
  amount: number;
  source_id: string;
  description?: string;
  created_at: string;
  service_tier?: string;
  batch_request_count?: number;
}

export interface BalanceResponse {
  balance: number;
  currency: string;
}

export interface TransactionsListResponse {
  data: Transaction[];
  has_more: boolean;
  page_start_balance: number;
}

export interface TransactionsQuery {
  limit?: number;
  skip?: number;
  userId?: string;
  group_batches?: boolean;
  search?: string;
  transaction_types?: string;
  start_date?: string;
  end_date?: string;
}

export type AdminTransactionType = "admin_grant" | "admin_removal";

export interface AddFundsRequest {
  user_id: string;
  source_id: string;
  amount: number;
  description?: string;
  transaction_type?: AdminTransactionType;
}

export type AddFundsResponse = Transaction;

export interface Probe {
  id: string;
  name: string;
  deployment_id: string;
  interval_seconds: number;
  active: boolean;
  http_method: string;
  request_path?: string | null;
  request_body?: Record<string, any> | null;
  created_at: string;
  updated_at: string;
}

export interface CreateProbeRequest {
  name: string;
  deployment_id: string;
  interval_seconds: number;
  http_method?: string;
  request_path?: string | null;
  request_body?: Record<string, any> | null;
}

export interface ProbeResult {
  id: string;
  probe_id: string;
  executed_at: string;
  success: boolean;
  response_time_ms: number | null;
  status_code: number | null;
  error_message: string | null;
  response_data: any | null;
  metadata: any | null;
}

export interface ProbeStatistics {
  total_executions: number;
  successful_executions: number;
  failed_executions: number;
  success_rate: number;
  avg_response_time_ms: number | null;
  min_response_time_ms: number | null;
  max_response_time_ms: number | null;
  p50_response_time_ms: number | null;
  p95_response_time_ms: number | null;
  p99_response_time_ms: number | null;
  last_execution: string | null;
  last_success: string | null;
  last_failure: string | null;
}

export interface FileObject {
  id: string;
  object: "file";
  bytes: number;
  created_at: number;
  expires_at?: number;
  filename: string;
  purpose:
    | "batch"
    | "batch_output"
    | "batch_error"
    | "fine-tune"
    | "assistants"
    | "vision"
    | "user_data"
    | "evals";
  created_by_email?: string;
  context_name?: string;
  context_type?: string;
  source?: string;
  source_name?: string;
}

export interface FileListResponse {
  object: "list";
  data: FileObject[];
  first_id: string;
  last_id: string;
  has_more: boolean;
}

export interface FileUploadRequest {
  file: File;
  purpose: string;
  filename?: string;
  expires_after?: {
    anchor: "created_at";
    seconds: number;
  };
}

export interface FileDeleteResponse {
  id: string;
  object: "file";
  deleted: boolean;
}

export interface FilesListQuery {
  after?: string;
  limit?: number;
  order?: "asc" | "desc";
  purpose?: string;
  search?: string;
  own?: boolean;
  member_id?: string;
}

export interface ModelCostBreakdown {
  model: string;
  request_count: number;
  estimated_input_tokens: number;
  estimated_output_tokens: number;
  estimated_cost: string;
}

export interface FileCostEstimate {
  file_id: string;
  total_requests: number;
  total_estimated_input_tokens: number;
  total_estimated_output_tokens: number;
  total_estimated_cost: string;
  models: ModelCostBreakdown[];
}

export interface BatchRequestCounts {
  total: number;
  completed: number;
  failed: number;
}

export interface BatchUsage {
  input_tokens: number;
  input_tokens_details?: {
    cached_tokens?: number;
    text_tokens?: number;
    audio_tokens?: number;
  };
  output_tokens: number;
  output_tokens_details?: {
    text_tokens?: number;
    audio_tokens?: number;
    reasoning_tokens?: number;
  };
  total_tokens: number;
}

export interface BatchError {
  code: string;
  line?: number;
  message: string;
  param?: string;
}

export interface BatchErrors {
  object: "list";
  data: BatchError[];
}

export type BatchStatus =
  | "validating"
  | "failed"
  | "in_progress"
  | "finalizing"
  | "completed"
  | "expired"
  | "cancelling"
  | "cancelled";

export interface Batch {
  id: string;
  object: "batch";
  endpoint: string;
  errors?: BatchErrors | null;
  input_file_id: string;
  completion_window: string;
  status: BatchStatus;
  output_file_id?: string | null;
  error_file_id?: string | null;
  created_at: number;
  in_progress_at?: number | null;
  expires_at?: number | null;
  finalizing_at?: number | null;
  completed_at?: number | null;
  failed_at?: number | null;
  expired_at?: number | null;
  cancelling_at?: number | null;
  cancelled_at?: number | null;
  request_counts: BatchRequestCounts;
  metadata?: Record<string, string>;
  model?: string | null;
  usage?: BatchUsage;
  analytics?: BatchAnalytics;
  dwext?: BatchDwExt;
}

export interface BatchDwExt {
  source?: string;
  source_name?: string;
  source_id?: string;
  source_file?: string;
  sync_id?: string;
}

export interface BatchListResponse {
  object: "list";
  data: Batch[];
  first_id?: string;
  last_id?: string;
  has_more: boolean;
}

export interface BatchCreateRequest {
  input_file_id: string;
  endpoint: string;
  completion_window: string;
  metadata?: Record<string, string>;
  api_key_id?: string;
  output_expires_after?: {
    anchor: "created_at";
    seconds: number;
  };
}

export interface BatchesListQuery {
  after?: string;
  limit?: number;
  search?: string;
  include?: string;
  member_id?: string;
  status?: string;
  created_after?: string;
  created_before?: string;
  active_first?: boolean;
  completion_window?: string;
}


export type RequestStatus =
  | "pending"
  | "in_progress"
  | "completed"
  | "failed"
  | "cancelled";

export interface BatchRequest {
  id: string;
  batch_id: string;
  custom_id: string;
  status: RequestStatus;
  request: {
    method: string;
    url: string;
    body: any;
  };
  response?: {
    status_code: number;
    body: any;
  } | null;
  error?: {
    code: string;
    message: string;
  } | null;
  created_at: number;
  started_at?: number | null;
  completed_at?: number | null;
  usage?: {
    prompt_tokens: number;
    completion_tokens: number;
    total_tokens: number;
  };
}

export interface BatchRequestsListResponse {
  object: "list";
  data: BatchRequest[];
  has_more: boolean;
  total: number;
}

export interface BatchRequestsListQuery {
  limit?: number;
  skip?: number;
  status?: RequestStatus;
}

export interface FileRequest {
  custom_id: string;
  method: string;
  url: string;
  body: any;
}

export interface FileRequestsListResponse {
  object: "list";
  data: FileRequest[];
  has_more: boolean;
  total: number;
}

export interface FileRequestsListQuery {
  limit?: number;
  skip?: number;
}

export interface BatchAnalytics {
  total_requests: number;
  total_prompt_tokens: number;
  total_completion_tokens: number;
  total_reasoning_tokens?: number | null;
  total_tokens: number;
  avg_duration_ms?: number | null;
  avg_ttfb_ms?: number | null;
  total_cost?: string | null;
}

export type BatchResultStatus =
  | "pending"
  | "in_progress"
  | "completed"
  | "failed"
  | "cancelled";

export interface BatchResultItem {
  id: string;
  custom_id: string | null;
  model: string;
  input_body: Record<string, unknown>;
  response_body: Record<string, unknown> | null;
  error: string | null;
  status: BatchResultStatus;
}


export type DaemonStatus = "initializing" | "running" | "dead";

export interface DaemonStats {
  requests_processed: number;
  requests_failed: number;
  requests_in_flight: number;
}

export interface DaemonConfig {
  claim_batch_size: number;
  default_model_concurrency: number;
  model_concurrency_limits: Record<string, number>;
  claim_interval_ms: number;
  min_retries?: number | null;
  stop_before_deadline_ms?: number | null;
  max_retries?: number | null;
  backoff_ms: number;
  backoff_factor: number;
  max_backoff_ms: number;
  first_chunk_timeout_ms: number;
  chunk_timeout_ms: number;
  body_timeout_ms: number;
  status_log_interval_ms?: number | null;
  heartbeat_interval_ms: number;
  claim_timeout_ms: number;
  processing_timeout_ms: number;
  pending_request_counts_timeout_ms: number;
}

export interface Daemon {
  id: string;
  status: DaemonStatus;
  hostname: string;
  pid: number;
  version: string;
  started_at: number;
  last_heartbeat?: number | null;
  stopped_at?: number | null;
  stats: DaemonStats;
  config: DaemonConfig;
}

export interface DaemonsListResponse {
  daemons: Daemon[];
}

export interface DaemonsQuery {
  status?: DaemonStatus;
}


export type WebhookScope = "own" | "platform";

export interface Webhook {
  id: string;
  user_id: string;
  url: string;
  enabled: boolean;
  event_types?: string[] | null;
  description?: string | null;
  scope: WebhookScope;
  created_at: string;
  updated_at: string;
  disabled_at?: string | null;
}

export interface WebhookWithSecret extends Webhook {
  secret: string;
}

export interface WebhookCreateRequest {
  url: string;
  event_types?: string[];
  description?: string;
  scope?: WebhookScope;
}

export interface WebhookUpdateRequest {
  url?: string;
  enabled?: boolean;
  event_types?: string[] | null;
  description?: string | null;
}


export interface Connection {
  id: string;
  kind: string;
  provider: string;
  name: string;
  created_at: number;
  updated_at: number;
}

export interface ConnectionCreateRequest {
  provider: string;
  name: string;
  config: Record<string, unknown>;
  kind?: string;
}

export interface ConnectionTestResponse {
  ok: boolean;
  provider: string;
  message?: string | null;
  scope?: Record<string, unknown> | null;
}

export interface SyncOperation {
  id: string;
  connection_id: string;
  status: string;
  strategy: string;
  files_found: number;
  files_skipped: number;
  files_ingested: number;
  files_failed: number;
  batches_created: number;
  error_summary?: unknown | null;
  started_at?: number | null;
  completed_at?: number | null;
  created_at: number;
}

export interface SyncEntry {
  id: string;
  external_key: string;
  external_size_bytes?: number | null;
  status: string;
  file_id?: string | null;
  batch_id?: string | null;
  template_count?: number | null;
  error?: string | null;
  skipped_lines?: number;
  validation_errors?: Array<{
    template_index: number;
    line: number;
    error: string;
  }> | null;
  created_at: number;
  updated_at: number;
}

export interface ExternalFileListResponse {
  data: ExternalFile[];
  has_more: boolean;
  next_cursor?: string | null;
}

export interface ExternalFile {
  key: string;
  size_bytes?: number | null;
  last_modified?: number | null;
  display_name?: string | null;
}

export interface TriggerSyncRequest {
  strategy?: string;
  file_keys?: string[];
  endpoint?: string;
  completion_window?: string;
  force?: boolean;
}

export interface SyncedKey {
  key: string;
  last_modified?: number | null;
  status: string;
}


export type OrgMemberRole = "owner" | "admin" | "member";

export type Modality = "realtime" | "batch";

export interface SchedulingTolerationPreset {
  id: string;
  label: string;
  description: string;
}

export interface OrganizationSummary {
  id: string;
  name: string;
  role: string;
  zero_data_retention: boolean;
  can_manage_keys: boolean;
}

export interface PendingEmailChange {
  new_email: string;
  expires_at: string;
  new_email_confirmed_at?: string | null;
  old_email_confirmed_at?: string | null;
}

export interface Organization extends User {
  member_count?: number;
  pending_email_change?: PendingEmailChange;
  disabled_modalities?: Modality[];
  members_can_create_keys?: boolean;
  toleration_preset?: string | null;
}

export interface OrganizationMember {
  id: string;
  user?: User;
  role: string;
  status: "active" | "pending";
  created_at: string;
  invite_email?: string;
  can_manage_keys: boolean;
}

export interface OrganizationCreateRequest {
  name: string;
  email: string;
  display_name?: string;
  owner_id?: string;
  members_can_create_keys?: boolean;
}

export interface OrganizationUpdateRequest {
  display_name?: string;
  email?: string;
  batch_notifications_enabled?: boolean;
  low_balance_threshold?: number | null;
  zero_data_retention?: boolean;
  self_hosted_only?: boolean;
  disabled_modalities?: Modality[];
  members_can_create_keys?: boolean;
  toleration_preset?: string | null;
}

export interface InviteMemberRequest {
  email: string;
  role?: OrgMemberRole;
  can_manage_keys?: boolean;
}

export interface InviteMemberResponse {
  id: string;
  email: string;
  role: string;
  status: string;
  created_at: string;
  expires_at: string;
}

export interface InviteDetailsResponse {
  org_name: string;
  role: string;
  inviter_name?: string;
  expires_at: string;
}

export interface UpdateMemberRoleRequest {
  role: OrgMemberRole;
  can_manage_keys?: boolean;
}

export interface OrganizationsQuery {
  skip?: number;
  limit?: number;
  search?: string;
  include?: string;
}

export interface SetActiveOrganizationRequest {
  organization_id: string | null;
}

export interface SetActiveOrganizationResponse {
  active_organization_id: string | null;
}


export type AsyncRequestStatus =
  | "pending"
  | "claimed"
  | "processing"
  | "completed"
  | "failed"
  | "canceled";

export interface AsyncRequest {
  id: string;
  batch_id: string | null;
  model: string;
  status: AsyncRequestStatus;
  created_at: string;
  completed_at: string | null;
  failed_at: string | null;
  duration_ms: number | null;
  response_status: number | null;
  service_tier: string | null;
  prompt_tokens: number | null;
  completion_tokens: number | null;
  reasoning_tokens: number | null;
  total_tokens: number | null;
  total_cost: number | null;
  created_by_email: string | null;
}

export interface AsyncRequestDetail extends AsyncRequest {
  body: string;
  response_body: string | null;
  error: string | null;
  created_by: string | null;
}

export interface AsyncRequestsListQuery {
  skip?: number;
  limit?: number;
  service_tiers?: string;
  status?: string;
  model?: string;
  member_id?: string;
  created_after?: string;
  created_before?: string;
  active_first?: boolean;
}
