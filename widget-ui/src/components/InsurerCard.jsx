import React from 'react';
import { CheckCircle2 } from 'lucide-react';

export default function InsurerCard({ insurer }) {
  // --- BLINDAGEM E NORMALIZAÇÃO DE DADOS ---
  const data = insurer.data || {};
  const components = data.components || {};
  const flags = insurer.flags || {};

  // 1. TRADUÇÃO DO SCORE (O JSON novo usa 'financial_score')
  // Se 'score' não existir, usa 'financial_score'.
  const score = Number(data.score) || Number(data.financial_score) || 0;
  
  // 2. TRADUÇÃO DA SOLVÊNCIA (O JSON novo usa 'components.financial.value')
  const financialComp = components.financial || {};
  // Tenta ler do formato antigo (solvency) ou novo (financial.value)
  const solvency = Number(components.solvency) || Number(financialComp.value) || 0;
  
  // 3. TRADUÇÃO DA REPUTAÇÃO (O JSON novo retorna um OBJETO, não um número direto)
  const repObj = components.reputation;
  let reputationScore = 0;
  let hasReputation = false;

  if (typeof repObj === 'number') {
    // Formato antigo (número direto)
    reputationScore = repObj;
    hasReputation = true;
  } else if (repObj && typeof repObj === 'object') {
    // Novo formato (objeto com detalhes)
    // Tenta pegar a nota em várias chaves possíveis do scraper
    const rawNota = repObj.satisfaction_avg || repObj.overallSatisfaction || repObj.nota || 0;
    
    if (rawNota > 0) {
      hasReputation = true;
      // Normaliza para escala 0-100
      // Se a nota for pequena (ex: 4.5 de 5), multiplica por 20.
      // Se for média (ex: 8.5 de 10), multiplica por 10.
      reputationScore = rawNota <= 5 ? rawNota * 20 : rawNota * 10;
    }
  }

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
  
  // Ajuste de sensibilidade: Se tiver score financeiro alto, já não é "Em Análise"
  if (score > 0 || solvency > 0) {
    if (score === 0 && solvency > 0) {
        // Caso onde só tem financeiro mas não reputação
        label = 'DADOS PARCIAIS';
        scoreColor = 'text-gray-600 bg-gray-50 border-gray-100';
    } else if (score < 50) {
        label = 'ATENÇÃO';
        scoreColor = 'text-red-600 bg-red-50 border-red-100';
    } else if (score < 70) { 
        label = 'REGULAR'; 
        scoreColor = 'text-yellow-600 bg-yellow-50 border-yellow-100';
    } else if (score < 85) { 
        label = 'BOM'; 
        scoreColor = 'text-blue-600 bg-blue-50 border-blue-100';
    } else { 
        label = 'EXCELENTE'; 
        scoreColor = 'text-green-600 bg-green-50 border-green-100';
    }
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4 hover:shadow-md transition-shadow flex flex-col md:flex-row gap-4 items-center">
      
      {/* 1. Identidade da Seguradora */}
      <div className="flex-1 w-full flex items-center gap-3 overflow-hidden">
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
          
          <div className="flex gap-2 mt-1">
            <span className="text-[10px] px-2 py-0.5 bg-blue-50 text-blue-700 rounded border border-blue-100 font-semibold">
              {insurer.segment || 'S4'}
            </span>
            {flags.openInsuranceParticipant && (
              <span className="text-[10px] px-2 py-0.5 bg-purple-50 text-purple-700 rounded border border-purple-100 flex items-center gap-1">
                <CheckCircle2 className="w-3 h-3" /> OPIN
              </span>
            )}
          </div>
        </div>
      </div>

      {/* 2. Métricas */}
      <div className="flex-1 w-full grid grid-cols-2 gap-4">
        
        {/* Financeiro */}
        <div>
          <div className="flex justify-between text-xs text-gray-500 mb-1">
            <span title={`Faturamento: ${formattedPremiums}`}>Financeiro</span>
            <strong>{solvency > 0 ? solvency.toFixed(0) : '-'}</strong>
          </div>
          <div className="w-full bg-gray-100 rounded-full h-1.5">
            <div 
              className={`h-1.5 rounded-full transition-all duration-500 ${solvency > 50 ? 'bg-blue-500' : 'bg-blue-300'}`} 
              style={{ width: `${solvency}%` }}
            ></div>
          </div>
        </div>

        {/* Reputação */}
        <div>
          <div className="flex justify-between text-xs text-gray-500 mb-1">
            <span>Reputação</span>
            <strong>
              {hasReputation ? (
                reputationScore.toFixed(0)
              ) : (
                <span className="text-[10px] text-gray-400 px-1 rounded bg-gray-50">N/A</span>
              )}
            </strong>
          </div>
          
          <div className="w-full bg-gray-100 rounded-full h-1.5 relative overflow-hidden">
            {hasReputation ? (
              <div 
                className={`h-1.5 rounded-full transition-all duration-500 ${reputationScore >= 60 ? 'bg-orange-400' : 'bg-red-400'}`} 
                style={{ width: `${reputationScore}%` }}
              ></div>
            ) : (
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

      {/* 3. Score Final */}
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
