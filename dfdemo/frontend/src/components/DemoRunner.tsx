import { useState, useEffect, useCallback } from 'react';
import type { DemoInfo, DemoStep, CommandResult } from '../types';
import { api } from '../api';

interface Props {
  demo: DemoInfo;
  onBack: () => void;
}

export default function DemoRunner({ demo, onBack }: Props) {
  const [steps, setSteps] = useState<DemoStep[]>([]);
  const [currentStep, setCurrentStep] = useState(0);
  const [results, setResults] = useState<Record<number, CommandResult>>({});
  const [running, setRunning] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchSteps = async () => {
      try {
        const result = await api.getDemoSteps(demo.id);
        setSteps(result.steps);
      } catch (err) {
        console.error('Failed to fetch steps:', err);
      } finally {
        setLoading(false);
      }
    };
    fetchSteps();
  }, [demo.id]);

  const runStep = useCallback(async (stepId: number) => {
    setRunning(true);
    try {
      const result = await api.runStep(demo.id, stepId);
      setResults((prev) => ({ ...prev, [stepId]: result }));
    } catch (err) {
      console.error('Step execution failed:', err);
      setResults((prev) => ({
        ...prev,
        [stepId]: {
          command: '',
          stdout: '',
          stderr: `Execution error: ${err instanceof Error ? err.message : 'Unknown'}`,
          exit_code: 1,
          success: false,
        },
      }));
    } finally {
      setRunning(false);
    }
  }, [demo.id]);

  const handleRunAll = async () => {
    for (const step of steps) {
      await runStep(step.id);
      setCurrentStep(step.id);
    }
  };

  const completedCount = Object.values(results).filter((r) => r.success).length;

  if (loading) {
    return (
      <div className="text-center">
        <span className="spinner" style={{ borderColor: 'var(--primary)', borderTopColor: 'var(--primary)' }} />
        <p className="mt-1 text-muted">Loading demo steps...</p>
      </div>
    );
  }

  return (
    <div>
      <button className="btn btn-outline btn-sm mb-2" onClick={onBack}>
        ← Back to Demos
      </button>

      <div className="card">
        <h2 className="card-title">🚀 {demo.name}</h2>
        <p className="text-muted mb-2">{demo.description}</p>

        <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem' }}>
          <button
            className="btn btn-primary"
            onClick={handleRunAll}
            disabled={running}
          >
            {running ? (
              <>
                <span className="spinner" /> Running...
              </>
            ) : (
              '▶️ Run All Steps'
            )}
          </button>
          <span className="text-muted" style={{ alignSelf: 'center' }}>
            {completedCount}/{steps.length} steps completed
          </span>
        </div>
      </div>

      <div className="card">
        <ol className="step-list">
          {steps.map((step) => {
            const result = results[step.id];
            const isActive = currentStep === step.id;
            const isCompleted = result?.success;

            return (
              <li
                key={step.id}
                className={`step-item ${isActive ? 'active' : ''} ${isCompleted ? 'completed' : ''}`}
              >
                <div className="step-number">
                  {isCompleted ? '✓' : step.id}
                </div>
                <div className="step-content">
                  <h4>{step.title}</h4>
                  <p>{step.description}</p>

                  {step.command && (
                    <div className="step-command">$ {step.command}</div>
                  )}
                  {step.api_call && (
                    <div className="step-command">📡 {step.api_call}</div>
                  )}

                  <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', marginTop: '0.5rem' }}>
                    <button
                      className="btn btn-sm btn-primary"
                      onClick={() => {
                        setCurrentStep(step.id);
                        runStep(step.id);
                      }}
                      disabled={running}
                    >
                      {running && isActive ? (
                        <>
                          <span className="spinner" /> Running...
                        </>
                      ) : result ? (
                        '🔄 Re-run'
                      ) : (
                        '▶️ Run'
                      )}
                    </button>
                    {result && (
                      <span style={{ fontSize: '0.85rem' }}>
                        {result.success ? (
                          <span style={{ color: 'var(--success)' }}>✅ Success</span>
                        ) : (
                          <span style={{ color: 'var(--danger)' }}>❌ Failed</span>
                        )}
                      </span>
                    )}
                  </div>

                  {result && (
                    <div className="mt-1">
                      <div className="command-output">
                        {result.command && (
                          <div className="cmd-line">$ {result.command}</div>
                        )}
                        {result.stdout && <div>{result.stdout}</div>}
                        {result.stderr && (
                          <div className={result.success ? '' : 'error-line'}>
                            {result.stderr}
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              </li>
            );
          })}
        </ol>
      </div>
    </div>
  );
}