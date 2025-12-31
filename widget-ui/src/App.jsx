import { useState, useEffect, useMemo } from 'react';
import { Search, ShieldCheck, AlertCircle, Award, ChevronDown, CheckCircle2, Filter } from 'lucide-react';
import InsurerCard from './components/InsurerCard';

const API_URL = '/api/v1/insurers.json'; 

function App() {
  const [insurers, setInsurers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortKey, setSortKey] = useState('score'); 

  useEffect(() => {
    fetch(API_URL)
      .then(res => res.json())
      .then(data => {
        setInsurers(data.insurers || []);
        setLoading(false);
      })
      .catch(err => {
        console.error("Erro carregando dados:", err);
        setLoading(false);
      });
  }, []);

  const filtered = useMemo(() => {
    let res = insurers;
    if (searchTerm) {
      const t = searchTerm.toLowerCase();
      res = res.filter(i => i.name.toLowerCase().includes(t) || i.cnpj.includes(t));
    }
    return res.sort((a, b) => {
      if (sortKey === 'score') return (b.data.score || 0) - (a.data.score || 0);
      if (sortKey === 'premiums') return (b.data.premiums || 0) - (a.data.premiums || 0);
      return 0;
    });
  }, [insurers, searchTerm, sortKey]);

  if (loading) return (
    <div className="flex justify-center items-center h-64">
      <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div>
    </div>
  );

  return (
    // 1. AJUSTE DE ESPAÇAMENTO: 'pt-24' para descolar do menu superior
    <div className="w-full max-w-[1100px] mx-auto px-4 pt-24 pb-12 font-sans text-[#373739]">
      
      {/* 2. ÁREA DO TÍTULO (H1) - Fora do Box */}
      <div className="text-center mb-10">
        <h1 className="text-3xl md:text-4xl font-bold text-[#3498db] mb-3">
          Ranking de Confiabilidade
        </h1>
        <p className="text-lg text-gray-600 max-w-2xl mx-auto">
          Analise e compare a saúde financeira, reputação e qualidade de {insurers.length} seguradoras com dados oficiais.
        </p>
      </div>

      {/* 3. BARRA DE CONTROLES (Novo Box Dedicado) */}
      <div className="bg-white p-4 rounded-xl shadow-card border border-gray-100 mb-8 sticky top-20 z-40">
        <div className="flex flex-col md:flex-row gap-4 justify-between items-center">
          
          {/* Campo de Busca Melhorado */}
          <div className="relative w-full md:w-1/2">
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <Search className="h-5 w-5 text-gray-400" />
            </div>
            <input 
              type="text" 
              placeholder="Buscar por nome (ex: Porto, Youse) ou CNPJ..." 
              className="block w-full pl-10 pr-3 py-3 border border-gray-300 rounded-lg leading-5 bg-gray-50 placeholder-gray-500 focus:outline-none focus:bg-white focus:ring-2 focus:ring-[#3498db] focus:border-[#3498db] transition duration-150 ease-in-out sm:text-sm"
              value={searchTerm}
              onChange={e => setSearchTerm(e.target.value)}
            />
          </div>

          {/* Filtros Visuais */}
          <div className="flex items-center gap-2 w-full md:w-auto justify-end overflow-x-auto">
            <span className="text-sm font-medium text-gray-500 hidden md:block mr-2">
              Ordenar por:
            </span>
            
            <button 
              onClick={() => setSortKey('score')}
              className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-bold transition-all shadow-sm border ${
                sortKey === 'score' 
                  ? 'bg-[#72f951] text-[#373739] border-[#72f951] ring-2 ring-[#72f951] ring-opacity-30' 
                  : 'bg-white text-gray-600 border-gray-200 hover:bg-gray-50'
              }`}
            >
              <Award className="w-4 h-4" />
              Melhor Nota
            </button>
            
            <button 
              onClick={() => setSortKey('premiums')}
              className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-bold transition-all shadow-sm border ${
                sortKey === 'premiums' 
                  ? 'bg-[#72f951] text-[#373739] border-[#72f951] ring-2 ring-[#72f951] ring-opacity-30' 
                  : 'bg-white text-gray-600 border-gray-200 hover:bg-gray-50'
              }`}
            >
              <ShieldCheck className="w-4 h-4" />
              Maior Porte (S1)
            </button>
          </div>
        </div>
      </div>

      {/* 4. RESULTADOS */}
      <div className="flex justify-between items-center mb-4 px-2">
        <span className="text-sm font-medium text-gray-500">
          Mostrando {filtered.length} resultados
        </span>
        {/* Aqui podemos colocar paginação no futuro */}
      </div>

      <div className="space-y-4 min-h-[400px]">
        {filtered.map(ins => (
          <InsurerCard key={ins.id} insurer={ins} />
        ))}
        
        {filtered.length === 0 && (
          <div className="flex flex-col items-center justify-center py-20 bg-white rounded-xl border border-dashed border-gray-300">
            <Search className="w-12 h-12 text-gray-300 mb-4" />
            <p className="text-lg text-gray-500">Nenhuma seguradora encontrada para "{searchTerm}"</p>
            <button 
              onClick={() => setSearchTerm('')}
              className="mt-4 text-[#3498db] font-medium hover:underline"
            >
              Limpar busca
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
