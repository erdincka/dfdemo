import { useState } from 'react';
import type { ConnectionStatus } from '../types';
import { api } from '../api';

interface Props {
  onConnect: (status: ConnectionStatus) => void;
}

export default function ConnectionForm({ onConnect }: Props) {
  const [hostname, setHostname] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [port, setPort] = useState('22');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      const result = await api.connect({
        hostname,
        username,
        password,
        port: parseInt(port, 10),
      });
      onConnect(result);
      if (!result.success) {
        setError(result.message);
      }
    } catch (err) {
      setError(`Connection failed: ${err instanceof Error ? err.message : 'Unknown error'}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="card" style={{ maxWidth: 480, margin: '2rem auto' }}>
      <h2 className="card-title">🔗 Connect to Data Fabric Cluster</h2>
      <p className="text-muted mb-2">
        Enter the connection details for your HPE Data Fabric (MapR) cluster.
        The app will test SSH connectivity before proceeding.
      </p>

      {error && <div className="alert alert-error">{error}</div>}

      <form onSubmit={handleSubmit}>
        <div className="form-group">
          <label htmlFor="hostname">Cluster Hostname / IP</label>
          <input
            id="hostname"
            type="text"
            value={hostname}
            onChange={(e) => setHostname(e.target.value)}
            placeholder="e.g., mapr-cluster.example.com"
            required
          />
        </div>

        <div className="form-group">
          <label htmlFor="port">SSH Port</label>
          <input
            id="port"
            type="number"
            value={port}
            onChange={(e) => setPort(e.target.value)}
            placeholder="22"
            required
          />
        </div>

        <div className="form-group">
          <label htmlFor="username">Username</label>
          <input
            id="username"
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="e.g., mapr"
            required
          />
        </div>

        <div className="form-group">
          <label htmlFor="password">Password</label>
          <input
            id="password"
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="••••••••"
            required
          />
        </div>

        <button type="submit" className="btn btn-primary" disabled={loading} style={{ width: '100%' }}>
          {loading ? (
            <>
              <span className="spinner" /> Connecting...
            </>
          ) : (
            '🔌 Test Connection'
          )}
        </button>
      </form>

      <div className="mt-2 text-muted" style={{ fontSize: '0.8rem' }}>
        <strong>Note:</strong> The credentials must have sufficient privileges to create users,
        volumes, and tables on the cluster. Sudo access is recommended for automated setup.
      </div>
    </div>
  );
}