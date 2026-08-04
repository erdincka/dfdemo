import type {
  ConnectionRequest,
  ConnectionStatus,
  DemoInfo,
  DemoStep,
  PrerequisitesResponse,
  CommandResult,
} from './types';

const BASE_URL = '/api';

async function request<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${BASE_URL}${url}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`);
  }
  return response.json();
}

export const api = {
  // Connection
  async connect(req: ConnectionRequest): Promise<ConnectionStatus> {
    return request('/connect', { method: 'POST', body: JSON.stringify(req) });
  },

  async disconnect(): Promise<void> {
    return request('/disconnect', { method: 'POST' });
  },

  async connectionStatus(): Promise<{ connected: boolean; hostname: string; username: string }> {
    return request('/connection/status');
  },

  // Demos
  async listDemos(): Promise<{ demos: DemoInfo[] }> {
    return request('/demos');
  },

  async getDemoSteps(demoId: string): Promise<{ demo_id: string; steps: DemoStep[] }> {
    return request(`/demos/${demoId}/steps`);
  },

  async checkPrerequisites(demoId: string): Promise<PrerequisitesResponse> {
    return request(`/demos/${demoId}/prerequisites`);
  },

  async setupPrerequisite(demoId: string, prereqName: string): Promise<CommandResult> {
    return request(`/demos/${demoId}/setup`, {
      method: 'POST',
      body: JSON.stringify({ demo_id: demoId, prerequisite_name: prereqName }),
    });
  },

  async setupAll(demoId: string): Promise<{ results: CommandResult[] }> {
    return request(`/demos/${demoId}/setup-all`, { method: 'POST' });
  },

  async runStep(demoId: string, stepId: number, params: Record<string, unknown> = {}): Promise<CommandResult> {
    return request(`/demos/${demoId}/run-step`, {
      method: 'POST',
      body: JSON.stringify({ demo_id: demoId, step_id: stepId, params }),
    });
  },
};