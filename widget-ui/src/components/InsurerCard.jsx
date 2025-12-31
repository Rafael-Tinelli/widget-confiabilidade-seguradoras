import React from 'react';
import { ShieldCheck, AlertCircle, CheckCircle2 } from 'lucide-react';

export default function InsurerCard({ insurer }) {
  // Garante valores numéricos
  const score = Number(insurer.data?.score) || 0;
  const solvency = Number(insurer.data?.components?.solvency) || 0;
  const reputation = Number(insurer.data?.components?.reputation) || 0;
  const premiums = Number(insurer.data?.premiums) || 0;

  // Formata moeda (para debug visual)
  const formattedPremiums = new Intl.NumberFormat('pt-BR', { 
    style: 'currency', 
    currency: 'BRL',
    notation: "compact"
  }).format(premiums);

  let scoreColor = 'text-red-600 bg-red-50 border-red-100';
  let label = 'EM ANÁLISE';
  
  if (score > 0) {
    label = 'ATENÇÃO';
    if (score >= 50) { scoreColor = 'text-yellow-600 bg-yellow-50 border-yellow-100'; label = 'REGULAR'; }
    if (score >= 70) { scoreColor = 'text-blue-600 bg-blue-50 border-blue-100'; label = 'BOM'; }
    if (score >= 85) { scoreColor = 'text-green-600 bg-green-50 border-green-100'; label = 'EXCELENTE'; }
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4 hover:shadow-md transition-shadow flex flex-col md:flex-row gap-4 items-center">
      
      {/* Identidade */}
      <div className="flex-1 w-full flex items-center gap-3 overflow-hidden">
        <div className="w-10 h-10 shrink-0 rounded-full bg-gray-100 flex items-center justify-center font-bold text-gray-400 text-sm">
          {insurer.name ? insurer.name.substring(0, 2).toUpperCase() : '??'}
        </div>
        <div className="min-w-0">
          <h3 className="font-bold text-[#373739] text-lg leading-tight truncate" title={insurer.name}>
            {insurer.name}
          </h3>
          <div className="text-xs text-gray-400 font-mono mt-0.5">
            CNPJ: {insurer.cnpj}
          </div>
          <div className="flex gap-2 mt-1">
            <span className="text-[10px] px-2 py-0.5 bg-blue-50 text-blue-700 rounded border border-blue-100 font-semibold">
              {insurer.segment || 'S4'}
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
            <span className="flex items-center gap-1" title={`Prêmios: ${formattedPremiums}`}>
               Financeiro
               {premiums === 0 && <span className="text-red-400 text-[10px]">(Sem dados)</span>}
            </span>
            <strong>{solvency > 0 ? solvency.toFixed(0) : '-'}</strong>
          </div>
          <div className="w-full bg-gray-100 rounded-full h-1.5">
            <div className={`h-1.5 rounded-full ${solvency > 0 ? 'bg-blue-500' : 'bg-gray-200'}`} style={{ width: `${solvency}%` }}></div>
          </div>
        </div>
        <div>
          <div className="flex justify-between text-xs text-gray-500 mb-1">
            <span>Reputação</span>
            <strong>{reputation > 0 ? reputation.toFixed(0) : '-'}</strong>
          </div>
          <div className="w-full bg-gray-100 rounded-full h-1.5">
            <div className={`h-1.5 rounded-full ${reputation > 0 ? 'bg-orange-400' : 'bg-gray-200'}`} style={{ width: `${reputation}%` }}></div>
          </div>
        </div>
      </div>

      {/* Score */}
      <div className="w-full md:w-auto flex items-center justify-between md:justify-center gap-4 border-t md:border-t-0 md:border-l border-gray-100 pt-3 md:pt-0 md:pl-4">
        <div className="text-right md:text-center min-w-[80px]">
          <div className={`text-[10px] font-bold px-2 py-0.5 rounded-full border inline-block mb-1 ${scoreColor}`}>
            {label}
          </div>
          <div className="text-3xl font-black text-[#373739] leading-none">
            {score > 0 ? score.toFixed(1) : '--'}
          </div>
        </div>
      </div>

    </div>
  );
}
