
export type TelemetryEventType =
  | "exception"
  | "event"
  | "measurement"
  | "identify"
  | "context";

export interface TelemetryExceptionPayload {
  message: string;
  name: string;
  stack?: string;
  source:
    | "react-error-boundary"
    | "react-route-boundary"
    | "window-error"
    | "unhandled-rejection"
    | "manual";
  componentStack?: string;
  route?: string;
  context?: Record<string, unknown>;
}

export interface TelemetryEventPayload {
  name: string;
  properties?: Record<string, unknown>;
}

export interface TelemetryMeasurementPayload {
  name: string;
  value: number;
  unit?: string;
  properties?: Record<string, unknown>;
}

export interface TelemetryIdentifyPayload {
  id: string | null;
  traits?: Record<string, unknown>;
}

export interface TelemetryContextPayload {
  context: Record<string, unknown>;
}

export type TelemetryDetail =
  | { type: "exception"; payload: TelemetryExceptionPayload }
  | { type: "event"; payload: TelemetryEventPayload }
  | { type: "measurement"; payload: TelemetryMeasurementPayload }
  | { type: "identify"; payload: TelemetryIdentifyPayload }
  | { type: "context"; payload: TelemetryContextPayload };

export const TELEMETRY_EVENT_NAME = "dashboard:telemetry";

function dispatch(detail: TelemetryDetail) {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.dispatchEvent(
      new CustomEvent<TelemetryDetail>(TELEMETRY_EVENT_NAME, { detail }),
    );
  } catch (err) {
    if (import.meta.env.DEV) {
      console.warn("telemetry dispatch failed", err);
    }
  }
}

function normalizeError(error: unknown): {
  message: string;
  name: string;
  stack?: string;
} {
  if (error instanceof Error) {
    return {
      message: error.message,
      name: error.name,
      stack: error.stack,
    };
  }
  return {
    message: typeof error === "string" ? error : "Unknown error",
    name: "NonError",
  };
}

export function captureException(
  error: unknown,
  options: {
    source: TelemetryExceptionPayload["source"];
    componentStack?: string;
    context?: Record<string, unknown>;
  },
): void {
  const normalized = normalizeError(error);
  const route =
    typeof window !== "undefined" ? window.location?.pathname : undefined;

  dispatch({
    type: "exception",
    payload: {
      ...normalized,
      source: options.source,
      componentStack: options.componentStack,
      route,
      context: options.context,
    },
  });
}

export function trackEvent(
  name: string,
  properties?: Record<string, unknown>,
): void {
  dispatch({ type: "event", payload: { name, properties } });
}

export function recordMeasurement(
  name: string,
  value: number,
  options: { unit?: string; properties?: Record<string, unknown> } = {},
): void {
  dispatch({
    type: "measurement",
    payload: { name, value, unit: options.unit, properties: options.properties },
  });
}

export function identifyUser(
  id: string | null,
  traits?: Record<string, unknown>,
): void {
  dispatch({ type: "identify", payload: { id, traits } });
}

export function setTelemetryContext(context: Record<string, unknown>): void {
  dispatch({ type: "context", payload: { context } });
}
