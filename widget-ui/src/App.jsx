import { useState, useEffect, useMemo } from 'react';
import { Search, ShieldCheck, Award, ChevronLeft, ChevronRight } from 'lucide-react';
import InsurerCard from './components/InsurerCard';

const API_URL = '/api/v1/insurers.json'; 

function App() {
  const [insurers, setInsurers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortKey, setSortKey] = useState('score'); // 'score' | 'premiums'
  
  // Paginação
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 10;

  useEffect(() => {
    fetch(API_URL)
      .then(res => res.json())
      .then(data => {
        const rawList = data.insurers || [];
        // 1. CORREÇÃO DE DUPLICATAS
        const uniqueList = Array.from(new Map(rawList.map(item => [item.cnpj, item])).values());
        setInsurers(uniqueList);
        setLoading(false);
      })
      .catch(err => {
        console.error("Erro carregando dados:", err);
        setLoading(false);
      });
  }, []);

  useEffect(() => {
    setCurrentPage(1);
  }, [searchTerm, sortKey]);

  const processedData = useMemo(() => {
    let res = [...insurers];

    // 2. BUSCA
    if (searchTerm) {
      const t = searchTerm.toLowerCase();
      res = res.filter(i => 
        (i.name && i.name.toLowerCase().includes(t)) || 
        (i.cnpj && i.cnpj.includes(t))
      );
    }

    // 3. ORDENAÇÃO CORRIGIDA (Lê financial_score ou score antigo)
    res.sort((a, b) => {
      const dataA = a.data || {};
      const dataB = b.data || {};

      // Compatibilidade: Lê financial_score (novo) ou score (antigo)
      const scoreA = Number(dataA.financial_score) || Number(dataA.score) || 0;
      const scoreB = Number(dataB.financial_score) || Number(dataB.score) || 0;
      
      const premA  = Number(dataA.premiums) || 0;
      const premB  = Number(dataB.premiums) || 0;

      if (sortKey === 'score') {
        if (scoreB !== scoreA) return scoreB - scoreA; // Maior nota primeiro
        return premB - premA; // Desempate por prêmio
      }
      
      if (sortKey === 'premiums') {
        return premB - premA;
      }
      
      return 0;
    });

    return res;
  }, [insurers, searchTerm, sortKey]);

  // 4. PAGINAÇÃO
  const totalPages = Math.ceil(processedData.length / itemsPerPage);
  const paginatedData = processedData.slice(
    (currentPage - 1) * itemsPerPage,
    currentPage * itemsPerPage
  );

  if (loading) return (
    <div className="flex justify-center items-center h-64">
      <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#3498db]"></div>
    </div>
  );

  return (
    <div className="w-full max-w-[1100px] mx-auto px-4 pt-24 pb-12 font-sans text-[#373739]">
      
      <div className="text-center mb-10">
        <h1 className="text-3xl md:text-4xl font-bold text-[#3498db] mb-3">
          Ranking de Confiabilidade
        </h1>
        <p className="text-lg text-gray-600 max-w-2xl mx-auto">
          Analisando <strong>{insurers.length}</strong> seguradoras oficiais (SUSEP).
        </p>
      </div>

      <div className="bg-white p-4 rounded-xl shadow-sm border border-gray-100 mb-8 sticky top-20 z-40">
        <div className="flex flex-col md:flex-row gap-4 justify-between items-center">
          
          <div className="relative w-full md:w-1/2">
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <Search className="h-5 w-5 text-gray-400" />
            </div>
            <input 
              type="text" 
              placeholder="Buscar seguradora..." 
              className="block w-full pl-10 pr-3 py-3 border border-gray-300 rounded-lg bg-gray-50 focus:outline-none focus:ring-2 focus:ring-[#3498db] transition"
              value={searchTerm}
              onChange={e => setSearchTerm(e.target.value)}
            />
          </div>

          <div className="flex items-center gap-2 w-full md:w-auto justify-end overflow-x-auto">
            <span className="text-sm font-medium text-gray-500 hidden md:block mr-2">
              Ordenar:
            </span>
            <button 
              onClick={() => setSortKey('score')}
              className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-bold transition-all border ${
                sortKey === 'score' 
                  ? 'bg-[#72f951] text-[#373739] border-[#72f951]' 
                  : 'bg-white text-gray-600 border-gray-200 hover:bg-gray-50'
              }`}
            >
              <Award className="w-4 h-4" /> Nota
            </button>
            <button 
              onClick={() => setSortKey('premiums')}
              className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-bold transition-all border ${
                sortKey === 'premiums' 
                  ? 'bg-[#72f951] text-[#373739] border-[#72f951]' 
                  : 'bg-white text-gray-600 border-gray-200 hover:bg-gray-50'
              }`}
            >
              <ShieldCheck className="w-4 h-4" /> Porte
            </button>
          </div>
        </div>
      </div>

      <div className="flex justify-between items-center mb-2 px-2 text-sm text-gray-500">
        <span>Mostrando {paginatedData.length} de {processedData.length} resultados</span>
        <span>Página {currentPage} de {totalPages || 1}</span>
      </div>

      <div className="space-y-4 min-h-[400px]">
        {paginatedData.map(ins => (
          <InsurerCard key={ins.cnpj} insurer={ins} />
        ))}
        
        {paginatedData.length === 0 && (
          <div className="text-center py-20 text-gray-500">
            Nenhuma seguradora encontrada.
          </div>
        )}
      </div>

      {totalPages > 1 && (
        <div className="flex justify-center gap-2 mt-8">
          <button
            onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
            disabled={currentPage === 1}
            className="p-2 rounded-lg border border-gray-300 disabled:opacity-50 hover:bg-gray-100"
          >
            <ChevronLeft className="w-5 h-5" />
          </button>
          
          <span className="flex items-center px-4 font-bold text-gray-700">
            {currentPage}
          </span>

          <button
            onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
            disabled={currentPage === totalPages}
            className="p-2 rounded-lg border border-gray-300 disabled:opacity-50 hover:bg-gray-100"
          >
            <ChevronRight className="w-5 h-5" />
          </button>
        </div>
      )}

    </div>
  );
}

export default App;
