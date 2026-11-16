export interface FeatureFlags {
  demo: boolean;
}

export interface DemoConfig {
  customResponse?: string;
  includeUserMessage?: boolean;
}

export interface AppSettings {
  apiBaseUrl: string;
  features: FeatureFlags;
  demoConfig?: DemoConfig;
  paymentProviderUrl?: string;
}
