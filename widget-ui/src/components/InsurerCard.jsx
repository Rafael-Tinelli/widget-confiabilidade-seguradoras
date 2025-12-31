import React from 'react';
import { ShieldCheck, AlertCircle, CheckCircle2 } from 'lucide-react';

export default function InsurerCard({ insurer }) {
  const score = insurer.data.score || 0;
  const solvency = insurer.data.components?.solvency || 0;
  const reputation = insurer.data.components?.reputation || 0;

  let scoreColor = 'text-red-600 bg-red-50 border-red-100';
  let label = 'ATENÇÃO';
  if (score >= 60) { scoreColor = 'text-yellow-600 bg-yellow-50 border-yellow-100'; label = 'BOM'; }
  if (score >= 80) { scoreColor = 'text-green-600 bg-green-50 border-green-100'; label = 'EXCELENTE'; }

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4 hover:shadow-md transition-shadow flex flex-col md:flex-row gap-4 items-center">
      
      {/* Identidade */}
      <div className="flex-1 w-full flex items-center gap-3">
        <div className="w-10 h-10 rounded-full bg-gray-100 flex items-center justify-center font-bold text-gray-400 text-sm">
          {insurer.name.substring(0, 2)}
        </div>
        <div>
          <h3 className="font-bold text-[#373739] text-lg leading-tight">{insurer.name}</h3>
          <div className="flex gap-2 mt-1">
            <span className="text-[10px] px-2 py-0.5 bg-blue-50 text-blue-700 rounded border border-blue-100 font-semibold">
              {insurer.segment}
            </span>
            {insurer.flags?.openInsuranceParticipant && (
              <span className="text-[10px] px-2 py-0.5 bg-purple-50 text-purple-700 rounded border border-purple-100 flex items-center gap-1">
                <CheckCircle2 className="w-3 h-3" /> OPIN
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Métricas */}
      <div className="flex-1 w-full grid grid-cols-2 gap-4">
        <div>
          <div className="flex justify-between text-xs text-gray-500 mb-1">
            <span>Financeiro</span>
            <strong>{solvency.toFixed(0)}</strong>
          </div>
          <div className="w-full bg-gray-100 rounded-full h-1.5">
            <div className="bg-blue-500 h-1.5 rounded-full" style={{ width: `${solvency}%` }}></div>
          </div>
        </div>
        <div>
          <div className="flex justify-between text-xs text-gray-500 mb-1">
            <span>Reputação</span>
            <strong>{reputation.toFixed(0)}</strong>
          </div>
          <div className="w-full bg-gray-100 rounded-full h-1.5">
            <div className="bg-orange-400 h-1.5 rounded-full" style={{ width: `${reputation}%` }}></div>
          </div>
        </div>
      </div>

      {/* Score */}
      <div className="w-full md:w-auto flex items-center justify-between md:justify-center gap-4 border-t md:border-t-0 md:border-l border-gray-100 pt-3 md:pt-0 md:pl-4">
        <div className="text-right md:text-center">
          <div className={`text-xs font-bold px-2 py-0.5 rounded-full border inline-block mb-1 ${scoreColor}`}>
            {label}
          </div>
          <div className="text-3xl font-black text-[#373739] leading-none">
            {score.toFixed(1)}
          </div>
        </div>
      </div>

    </div>
  );
}
