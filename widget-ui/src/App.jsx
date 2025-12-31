import { useState, useEffect, useMemo } from 'react';
import { Search, ShieldCheck, AlertCircle, Award, ChevronDown, CheckCircle2 } from 'lucide-react';
import InsurerCard from './components/InsurerCard';

// Em produção, aponta para o JSON gerado no backend
const API_URL = '/api/v1/insurers.json'; 

function App() {
  const [insurers, setInsurers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortKey, setSortKey] = useState('score'); 

  useEffect(() => {
    // Tenta carregar do caminho relativo padrão
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

  if (loading) return <div className="p-8 text-center text-gray-500">Carregando mercado...</div>;

  return (
    <div className="w-full max-w-[1100px] mx-auto p-4 text-[#373739]">
      
      {/* Header / Filtros */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100 mb-6">
        <div className="flex flex-col md:flex-row justify-between items-center gap-4">
          <div>
            <h2 className="text-2xl font-bold text-[#3498db]">Ranking de Confiabilidade</h2>
            <p className="text-sm text-gray-500">Compare {insurers.length} seguradoras com dados oficiais.</p>
          </div>
          
          <div className="relative w-full md:w-80">
            <input 
              type="text" 
              placeholder="Buscar seguradora..." 
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-full focus:outline-none focus:border-[#3498db]"
              value={searchTerm}
              onChange={e => setSearchTerm(e.target.value)}
            />
            <Search className="absolute left-3 top-2.5 text-gray-400 w-4 h-4" />
          </div>
        </div>

        <div className="flex gap-2 mt-4 overflow-x-auto pb-1">
          <button 
            onClick={() => setSortKey('score')}
            className={`flex items-center gap-1 px-4 py-1.5 rounded-full text-sm font-medium transition-colors ${sortKey === 'score' ? 'bg-[#72f951] text-[#373739]' : 'bg-gray-100 text-gray-600'}`}
          >
            <Award className="w-4 h-4" /> Melhor Nota
          </button>
          <button 
            onClick={() => setSortKey('premiums')}
            className={`flex items-center gap-1 px-4 py-1.5 rounded-full text-sm font-medium transition-colors ${sortKey === 'premiums' ? 'bg-[#72f951] text-[#373739]' : 'bg-gray-100 text-gray-600'}`}
          >
            <ShieldCheck className="w-4 h-4" /> Maior Porte
          </button>
        </div>
      </div>

      {/* Lista */}
      <div className="space-y-3">
        {filtered.slice(0, 50).map(ins => (
          <InsurerCard key={ins.id} insurer={ins} />
        ))}
        {filtered.length === 0 && <div className="text-center p-8 text-gray-500">Nenhum resultado.</div>}
      </div>
    </div>
  );
}

export default App;
