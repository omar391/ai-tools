export interface CodexAuth {
  auth_mode: string;
  OPENAI_API_KEY: string | null;
  tokens: {
    id_token: string;
    access_token: string;
    refresh_token: string | null;
    account_id: string;
  };
  last_refresh: string;
}

export interface AuthSummary {
  email: string;
  accountId: string;
  planType: string;
}

export interface DeviceLoginPayload {
  type: "chatgptAuthTokens";
  accessToken: string;
  chatgptAccountId: string;
  chatgptPlanType: string | null;
}

export interface JsonRpcRequest<TParams> {
  jsonrpc: "2.0";
  id: string;
  method: string;
  params: TParams;
}

export interface CodexDesktopMcpRequest<TParams> {
  type: "mcp-request";
  hostId: string;
  request: JsonRpcRequest<TParams>;
}

export interface UsageWindow {
  used_percent: number;
  limit_window_seconds: number;
  reset_after_seconds: number;
  reset_at: number;
}

export interface UsageRateLimit {
  allowed: boolean;
  limit_reached: boolean;
  primary_window: UsageWindow | null;
  secondary_window: UsageWindow | null;
}

export interface UsageCredits {
  has_credits: boolean;
  unlimited: boolean;
  balance: number | null;
  approx_local_messages: number | null;
  approx_cloud_messages: number | null;
}

export interface UsageResponse {
  user_id: string;
  account_id: string;
  email: string;
  plan_type: string;
  rate_limit: UsageRateLimit | null;
  code_review_rate_limit: UsageRateLimit | null;
  additional_rate_limits: unknown;
  credits: UsageCredits | null;
  promo: unknown;
}

export interface QuotaAssessment {
  usage: UsageResponse;
  usable: boolean;
  summary: string;
  blocker: string | null;
  primaryQuotaLeftPercent: number | null;
}

export type CodexSignalKind = "rate_limits_updated" | "usage_limit_reached";

export interface CodexLogSignal {
  id: number;
  ts: number;
  kind: CodexSignalKind;
  target: string;
  body: string;
}

export interface RotationResult {
  summary: AuthSummary;
  loginPayload: DeviceLoginPayload;
}

export interface RotationDecision {
  lastSignalId: number | null;
  signals: CodexLogSignal[];
  assessment: QuotaAssessment | null;
  assessmentError: string | null;
  shouldRotate: boolean;
  reason: string | null;
  rotationCommand: "next" | "create" | null;
  rotationArgs: string[];
}
