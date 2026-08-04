import { useState, useCallback } from 'react';
import type { ConnectionStatus, DemoInfo } from './types';
import { api } from './api';
import ConnectionForm from './components/ConnectionForm';
import DemoSelector from './components/DemoSelector';
import PrerequisiteCheck from './components/PrerequisiteCheck';
import DemoRunner from './components/DemoRunner';
import './App.css';

type AppView = 'connect' | 'demos' | 'prerequisites' | 'demo';

function App() {
  const [view, setView] = useState<AppView>('connect');
  const [connection, setConnection] = useState<ConnectionStatus | null>(null);
  const [demos, setDemos] = useState<DemoInfo[]>([]);
  const [selectedDemo, setSelectedDemo] = useState<DemoInfo | null>(null);

  const handleConnect = useCallback(async (status: ConnectionStatus) => {
    setConnection(status);
    if (status.success) {
      const result = await api.listDemos();
      setDemos(result.demos);
      setView('demos');
    }
  }, []);

  const handleDisconnect = useCallback(async () => {
    await api.disconnect();
    setConnection(null);
    setDemos([]);
    setSelectedDemo(null);
    setView('connect');
  }, []);

  const handleSelectDemo = useCallback((demo: DemoInfo) => {
    setSelectedDemo(demo);
    setView('prerequisites');
  }, []);

  const handlePrereqsPassed = useCallback(() => {
    setView('demo');
  }, []);

  const handleBackToDemos = useCallback(() => {
    setSelectedDemo(null);
    setView('demos');
  }, []);

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-left">
          <h1>🏗️ dfdemo</h1>
          <span className="subtitle">HPE Data Fabric Demo Runner</span>
        </div>
        <div className="header-right">
          {connection?.success && (
            <>
              <span className="connection-badge connected">
                ● {connection.cluster_info?.hostname || 'Connected'}
              </span>
              <button className="btn btn-sm btn-outline" onClick={handleDisconnect}>
                Disconnect
              </button>
            </>
          )}
        </div>
      </header>

      <main className="app-main">
        {view === 'connect' && <ConnectionForm onConnect={handleConnect} />}

        {view === 'demos' && (
          <DemoSelector demos={demos} onSelect={handleSelectDemo} />
        )}

        {view === 'prerequisites' && selectedDemo && (
          <PrerequisiteCheck
            demo={selectedDemo}
            onPassed={handlePrereqsPassed}
            onBack={handleBackToDemos}
          />
        )}

        {view === 'demo' && selectedDemo && (
          <DemoRunner demo={selectedDemo} onBack={handleBackToDemos} />
        )}
      </main>

      <footer className="app-footer">
        <span>
          Powered by <a href="https://docs.ezmeral.hpe.com/datafabric-customer-managed/81/index.html" target="_blank" rel="noreferrer">HPE Ezmeral Data Fabric</a>
        </span>
      </footer>
    </div>
  );
}

export default App;