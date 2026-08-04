import { useState, useEffect, useCallback } from 'react';
import type { DemoInfo, Prerequisite, CommandResult } from '../types';
import { api } from '../api';

interface Props {
  demo: DemoInfo;
  onPassed: () => void;
  onBack: () => void;
}

export default function PrerequisiteCheck({ demo, onPassed, onBack }: Props) {
  const [prereqs, setPrereqs] = useState<Prerequisite[]>([]);
  const [allPassed, setAllPassed] = useState(false);
  const [loading, setLoading] = useState(true);
  const [fixing, setFixing] = useState<string | null>(null);
  const [fixResults, setFixResults] = useState<Record<string, CommandResult>>({});

  const checkPrereqs = useCallback(async () => {
    setLoading(true);
    try {
      const result = await api.checkPrerequisites(demo.id);
      setPrereqs(result.prerequisites);
      setAllPassed(result.all_passed);
    } catch (err) {
      console.error('Failed to check prerequisites:', err);
    } finally {
      setLoading(false);
    }
  }, [demo.id]);

  useEffect(() => {
    checkPrereqs();
  }, [checkPrereqs]);

  const handleFix = async (prereq: Prerequisite) => {
    setFixing(prereq.name);
    try {
      const result = await api.setupPrerequisite(demo.id, prereq.name);
      setFixResults((prev) => ({ ...prev, [prereq.name]: result }));
      // Re-check after fix
      await checkPrereqs();
    } catch (err) {
      console.error('Fix failed:', err);
    } finally {
      setFixing(null);
    }
  };

  const handleFixAll = async () => {
    setFixing('__all__');
    try {
      await api.setupAll(demo.id);
      await checkPrereqs();
    } catch (err) {
      console.error('Fix all failed:', err);
    } finally {
      setFixing(null);
    }
  };

  const statusIcon = (status: string) => {
    switch (status) {
      case 'pass': return '✅';
      case 'fail': return '❌';
      case 'warn': return '⚠️';
      default: return '❓';
    }
  };

  const failingCount = prereqs.filter((p) => p.status === 'fail').length;

  return (
    <div>
      <button className="btn btn-outline btn-sm mb-2" onClick={onBack}>
        ← Back to Demos
      </button>

      <div className="card">
        <h2 className="card-title">🔍 Prerequisites: {demo.name}</h2>
        <p className="text-muted mb-2">
          Checking what's needed to run this demo...
        </p>

        {loading ? (
          <div className="text-center">
            <span className="spinner" style={{ borderColor: 'var(--primary)', borderTopColor: 'var(--primary)' }} />
            <p className="mt-1 text-muted">Checking prerequisites...</p>
          </div>
        ) : (
          <>
            <ul className="prereq-list">
              {prereqs.map((prereq) => (
                <li key={prereq.name} className="prereq-item">
                  <span className="prereq-icon">{statusIcon(prereq.status)}</span>
                  <div className="prereq-info">
                    <div className="name">{prereq.description}</div>
                    <div className="message">{prereq.message}</div>
                    {fixResults[prereq.name] && (
                      <div className="mt-1">
                        {fixResults[prereq.name].success ? (
                          <span className="alert alert-success" style={{ padding: '0.25rem 0.5rem', fontSize: '0.8rem' }}>
                            Fixed successfully
                          </span>
                        ) : (
                          <span className="alert alert-error" style={{ padding: '0.25rem 0.5rem', fontSize: '0.8rem' }}>
                            Fix failed: {fixResults[prereq.name].stderr}
                          </span>
                        )}
                      </div>
                    )}
                  </div>
                  <div className="prereq-action">
                    {prereq.status === 'fail' && prereq.fix_command && (
                      <button
                        className="btn btn-sm btn-primary"
                        onClick={() => handleFix(prereq)}
                        disabled={fixing !== null}
                      >
                        {fixing === prereq.name ? '⏳' : '🔧 Fix'}
                      </button>
                    )}
                    {prereq.status === 'warn' && (
                      <span className="text-muted" style={{ fontSize: '0.8rem' }}>
                        Manual setup may be needed
                      </span>
                    )}
                  </div>
                </li>
              ))}
            </ul>

            <div className="mt-2" style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
              {failingCount > 0 && (
                <button
                  className="btn btn-primary"
                  onClick={handleFixAll}
                  disabled={fixing !== null}
                >
                  {fixing === '__all__' ? (
                    <>
                      <span className="spinner" /> Setting up...
                    </>
                  ) : (
                    `🔧 Fix All (${failingCount} issue${failingCount > 1 ? 's' : ''})`
                  )}
                </button>
              )}

              {allPassed && (
                <button className="btn btn-success" onClick={onPassed}>
                  ✅ All Ready — Start Demo
                </button>
              )}

              <button className="btn btn-outline" onClick={checkPrereqs} disabled={loading}>
                🔄 Re-check
              </button>
            </div>

            {!allPassed && failingCount === 0 && (
              <div className="alert alert-warning mt-2">
                Some prerequisites have warnings. You can still proceed, but some features
                may require manual setup. Check the warnings above.
              </div>
            )}

            {!allPassed && failingCount > 0 && (
              <div className="alert alert-info mt-2">
                {failingCount} prerequisite(s) need to be fixed before the demo can run.
                Click "Fix" on individual items or "Fix All" to set them up automatically.
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}