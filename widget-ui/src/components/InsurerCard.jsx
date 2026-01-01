import React from 'react';
import { ShieldCheck, AlertCircle, CheckCircle2 } from 'lucide-react';

export default function InsurerCard({ insurer }) {
  // --- BLINDAGEM DE DADOS ---
  // Garante que não quebre se vier undefined
  const data = insurer.data || {};
  const components = data.components || {};
  const flags = insurer.flags || {};

  // Score Geral
  const score = Number(data.score) || 0;
  
  // Solvência (Sempre existe, pois vem da SUSEP)
  const solvency = Number(components.solvency) || 0;
  
  // Reputação (Pode ser null se não houver match no Consumidor.gov)
  // A API agora retorna null explicitamente quando não há dados.
  const rawReputation = components.reputation;
  const hasReputation = rawReputation !== null && rawReputation !== undefined;
  const reputation = hasReputation ? Number(rawReputation) : 0;

  // Prêmios (Para tooltip)
  const premiums = Number(data.premiums) || 0;
  const formattedPremiums = new Intl.NumberFormat('pt-BR', { 
    style: 'currency', 
    currency: 'BRL',
    notation: "compact"
  }).format(premiums);

  // --- LÓGICA DE CORES E LABELS ---
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
      
      {/* 1. Identidade da Seguradora */}
      <div className="flex-1 w-full flex items-center gap-3 overflow-hidden">
        {/* Avatar com Iniciais */}
        <div className="w-10 h-10 shrink-0 rounded-full bg-gray-100 flex items-center justify-center font-bold text-gray-400 text-sm border border-gray-200">
          {insurer.name ? insurer.name.substring(0, 2).toUpperCase() : '??'}
        </div>
        
        <div className="min-w-0">
          <h3 className="font-bold text-[#373739] text-lg leading-tight truncate" title={insurer.name}>
            {insurer.name}
          </h3>
          <div className="text-xs text-gray-400 font-mono mt-0.5">
            CNPJ: {insurer.cnpj}
          </div>
          
          {/* Badges de Segmento e Inovação */}
          <div className="flex gap-2 mt-1">
            <span className="text-[10px] px-2 py-0.5 bg-blue-50 text-blue-700 rounded border border-blue-100 font-semibold" title="Porte da Seguradora (S1=Grande, S4=Pequena)">
              {insurer.segment || 'S4'}
            </span>
            {flags.openInsuranceParticipant && (
              <span className="text-[10px] px-2 py-0.5 bg-purple-50 text-purple-700 rounded border border-purple-100 flex items-center gap-1" title="Participante do Open Insurance">
                <CheckCircle2 className="w-3 h-3" /> OPIN
              </span>
            )}
          </div>
        </div>
      </div>

      {/* 2. Métricas (Barras de Progresso) */}
      <div className="flex-1 w-full grid grid-cols-2 gap-4">
        
        {/* Coluna A: Solidez Financeira */}
        <div>
          <div className="flex justify-between text-xs text-gray-500 mb-1">
            <span className="flex items-center gap-1" title={`Faturamento Aprox: ${formattedPremiums}`}>
               Financeiro
               {premiums === 0 && <span className="text-red-400 text-[10px] ml-1">(Sem dados)</span>}
            </span>
            <strong>{solvency > 0 ? solvency.toFixed(0) : '-'}</strong>
          </div>
          <div className="w-full bg-gray-100 rounded-full h-1.5">
            <div 
              className={`h-1.5 rounded-full transition-all duration-500 ${solvency > 50 ? 'bg-blue-500' : 'bg-blue-300'}`} 
              style={{ width: `${solvency}%` }}
            ></div>
          </div>
        </div>

        {/* Coluna B: Reputação (Com tratamento de N/A) */}
        <div>
          <div className="flex justify-between text-xs text-gray-500 mb-1">
            <span title="Baseado no Consumidor.gov.br">Reputação</span>
            <strong>
              {hasReputation ? (
                reputation.toFixed(0)
              ) : (
                <span className="text-[10px] text-gray-400 font-normal border border-gray-200 px-1 rounded bg-gray-50">N/A</span>
              )}
            </strong>
          </div>
          
          <div className="w-full bg-gray-100 rounded-full h-1.5 relative overflow-hidden">
            {hasReputation ? (
              // Barra Normal
              <div 
                className={`h-1.5 rounded-full transition-all duration-500 ${reputation >= 60 ? 'bg-orange-400' : 'bg-red-400'}`} 
                style={{ width: `${reputation}%` }}
              ></div>
            ) : (
              // Barra "Sem Dados" (Hachurada/Listrada para indicar ausência)
              <div 
                className="w-full h-full opacity-30"
                style={{ 
                  backgroundImage: 'repeating-linear-gradient(45deg, #ccc 0, #ccc 5px, transparent 5px, transparent 10px)' 
                }}
              ></div>
            )}
          </div>
        </div>

      </div>

      {/* 3. Score Final em Destaque */}
      <div className="w-full md:w-auto flex items-center justify-between md:justify-center gap-4 border-t md:border-t-0 md:border-l border-gray-100 pt-3 md:pt-0 md:pl-4">
        <div className="text-right md:text-center min-w-[80px]">
          <div className={`text-[10px] font-bold px-2 py-0.5 rounded-full border inline-block mb-1 whitespace-nowrap ${scoreColor}`}>
            {label}
          </div>
          <div className="text-3xl font-black text-[#373739] leading-none tracking-tight">
            {score > 0 ? score.toFixed(1) : '--'}
          </div>
        </div>
      </div>

    </div>
  );
}
