export interface ConnectionRequest {
  hostname: string;
  username: string;
  password: string;
  port: number;
}

export interface ConnectionStatus {
  success: boolean;
  message: string;
  cluster_info?: Record<string, string>;
}

export interface DemoInfo {
  id: string;
  name: string;
  description: string;
  keywords: string[];
}

export interface DemoStep {
  id: number;
  title: string;
  description: string;
  command?: string;
  api_call?: string;
  expected_result: string;
}

export type PrerequisiteStatus = 'pass' | 'fail' | 'warn' | 'unknown';

export interface Prerequisite {
  name: string;
  description: string;
  status: PrerequisiteStatus;
  message: string;
  fix_command?: string;
}

export interface CommandResult {
  command: string;
  stdout: string;
  stderr: string;
  exit_code: number;
  success: boolean;
}

export interface PrerequisitesResponse {
  demo_id: string;
  prerequisites: Prerequisite[];
  all_passed: boolean;
}