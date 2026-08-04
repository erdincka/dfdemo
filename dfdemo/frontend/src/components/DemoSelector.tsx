import type { DemoInfo } from '../types';

interface Props {
  demos: DemoInfo[];
  onSelect: (demo: DemoInfo) => void;
}

export default function DemoSelector({ demos, onSelect }: Props) {
  return (
    <div>
      <h2 className="mb-2">📋 Available Demos</h2>
      <p className="text-muted mb-2">
        Select a demo to check its prerequisites and get started.
      </p>

      <div className="demo-grid">
        {demos.map((demo) => (
          <div key={demo.id} className="demo-card" onClick={() => onSelect(demo)}>
            <h3>{demo.name}</h3>
            <p>{demo.description}</p>
            <div className="keywords">
              {demo.keywords.map((kw) => (
                <span key={kw} className="keyword">{kw}</span>
              ))}
            </div>
          </div>
        ))}
      </div>

      {demos.length === 0 && (
        <div className="alert alert-info">No demos available.</div>
      )}
    </div>
  );
}