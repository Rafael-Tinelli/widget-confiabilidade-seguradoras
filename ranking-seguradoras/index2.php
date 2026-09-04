<?php
/**
 * Ranking/Comparador de Seguradoras v2 — §19.7 candidato final de staging
 * URL: https://sanida.com.br/ranking-seguradoras/index2.php
 *
 * Frontend baseado no contrato público fechado de busca/perfil v2:
 * search_index.json -> profiles/*.json.
 * O PHP não recalcula metodologia.
 */
header('X-Robots-Tag: noindex, follow', true);

$config = include $_SERVER['DOCUMENT_ROOT'] . "/PHP/config-site.php";

$page_title = "Consulta de Seguradoras SUSEP: compare confiabilidade | Sanida";
$page_desc  = "Consulte seguradoras autorizadas pela SUSEP, identifique marcas e quem assume o risco, compare capital, liquidez e reclamações e veja rankings por critério.";
$canonical  = "https://sanida.com.br/ranking-seguradoras/";
$page_robots = "noindex, follow";

$rk2_public_base = "/ranking-seguradoras/data/v2/public";
$rk2_page_url    = "/ranking-seguradoras/index2.php";
?>
<!DOCTYPE html>
<html lang="pt-BR">
<?php
include $_SERVER['DOCUMENT_ROOT'] . "/PHP/head-global.php";

$rk2_schema = [
    '@context' => 'https://schema.org',
    '@graph' => [
        [
            '@type' => 'WebPage',
            '@id' => $canonical . '#webpage',
            'url' => $canonical,
            'name' => $page_title,
            'description' => $page_desc,
            'isPartOf' => ['@id' => 'https://sanida.com.br/#website'],
            'about' => ['@id' => 'https://sanida.com.br/#insuranceagency'],
            'mainEntity' => ['@id' => $canonical . '#app'],
            'inLanguage' => 'pt-BR',
        ],
        [
            '@type' => 'WebApplication',
            '@id' => $canonical . '#app',
            'name' => 'Consulta e comparação de seguradoras Sanida',
            'description' => $page_desc,
            'url' => $canonical,
            'operatingSystem' => 'Web',
            'applicationCategory' => 'FinanceApplication',
            'isAccessibleForFree' => true,
            'publisher' => ['@id' => 'https://sanida.com.br/#insuranceagency'],
            'inLanguage' => 'pt-BR',
        ],
    ],
];
?>
<script type="application/ld+json"><?php echo json_encode(
    $rk2_schema,
    JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_HEX_TAG | JSON_HEX_AMP
); ?></script>
<link rel="stylesheet" href="/ranking-seguradoras/assets/ranking-v2.css?v=15">
<script src="/ranking-seguradoras/assets/ranking-v2.js?v=15" defer></script>
<script src="/JS/formulario.js" defer></script>
<script src="/JS/efeitos-visuais.js" defer></script>
</head>

<body>
<?php include $_SERVER['DOCUMENT_ROOT'] . "/PHP/header-menu.php"; ?>

<main
  class="rk2"
  data-rk2-root
  aria-busy="true"
  data-public-base="<?php echo htmlspecialchars($rk2_public_base, ENT_QUOTES, 'UTF-8'); ?>"
  data-page-url="<?php echo htmlspecialchars($rk2_page_url, ENT_QUOTES, 'UTF-8'); ?>"
>
  <section class="rk2-hero" id="consultar" aria-labelledby="rk2-title">
    <div class="rk2-shell rk2-hero__grid">
      <div class="rk2-hero__copy">
        <div class="rk2-eyebrow">
          <span class="rk2-eyebrow__dot" aria-hidden="true"></span>
          Consulta SUSEP · dados públicos · comparação
        </div>
        <h1 id="rk2-title">Consulte as seguradoras SUSEP e compare sinais de confiabilidade</h1>
        <p class="rk2-hero__lead">
          Pesquise pelo nome que você conhece. A ferramenta identifica se corresponde a uma seguradora autorizada pela SUSEP,
          uma marca ou identidade de mercado, uma empresa histórica ou um participante do Sandbox. Depois, separa situação regulatória,
          capital, liquidez e reclamações para mostrar o que os dados permitem afirmar.
        </p>
        <div class="rk2-hero__meta" aria-live="polite">
          <span id="rk2-population">Carregando catálogo público…</span>
          <a href="#confiabilidade">Como interpretar a confiabilidade?</a>
        </div>
      </div>

      <div class="rk2-search-panel" aria-label="Consultar empresa">
        <p class="rk2-search-panel__label">Consultar seguradora, marca ou empresa</p>
        <form class="rk2-search" id="rk2-search-form" role="search" autocomplete="off">
          <div class="rk2-search__box">
            <svg class="rk2-search__icon" viewBox="0 0 24 24" aria-hidden="true">
              <path d="m21 21-4.35-4.35m2.35-5.65a8 8 0 1 1-16 0 8 8 0 0 1 16 0Z"/>
            </svg>
            <label class="rk2-sr-only" for="rk2-search-input">Digite o nome, CNPJ ou código SUSEP</label>
            <input
              id="rk2-search-input"
              type="search"
              inputmode="search"
              placeholder="Ex.: Youse, Loovi, HDI…"
              aria-autocomplete="list"
              aria-controls="rk2-search-suggestions"
              aria-expanded="false"
              aria-describedby="rk2-search-help"
              maxlength="120"
              disabled
              aria-disabled="true"
            >
            <button class="rk2-search__clear" id="rk2-search-clear" type="button" hidden aria-label="Limpar busca">×</button>
            <button class="rk2-search__submit" type="submit" disabled>Buscar</button>
          </div>
          <div
            class="rk2-suggestions"
            id="rk2-search-suggestions"
            role="listbox"
            aria-label="Correspondências encontradas"
            hidden
          ></div>
        </form>
        <p class="rk2-search__help" id="rk2-search-help">
          Não sabe o nome exato? Digite como aparece no aplicativo, proposta ou apólice. Você confirma a empresa antes de abrir a análise.
        </p>
        <div class="rk2-search-panel__links" aria-label="Atalhos">
          <a href="#comparar">Comparar seguradoras <span aria-hidden="true">→</span></a>
          <a href="#lista">Lista de seguradoras <span aria-hidden="true">→</span></a>
          <a href="#explorar">Rankings por critério <span aria-hidden="true">→</span></a>
        </div>
      </div>

      <div class="rk2-active-context" id="rk2-active-context" hidden aria-live="polite">
        <div class="rk2-active-context__identity">
          <span>Consultando</span>
          <strong id="rk2-active-name">Empresa selecionada</strong>
          <small id="rk2-active-meta"></small>
        </div>
        <div class="rk2-active-context__actions">
          <button type="button" class="rk2-context-link" id="rk2-context-change">Trocar empresa</button>
          <button type="button" class="rk2-context-close" id="rk2-context-close" aria-label="Fechar consulta atual">×</button>
        </div>
      </div>
    </div>
  </section>

  <nav class="rk2-local-nav" aria-label="Navegação da ferramenta">
    <div class="rk2-shell rk2-local-nav__inner">
      <a href="#consultar">Consultar</a>
      <a href="#comparar">Comparar</a>
      <a href="#lista">Lista de seguradoras</a>
      <a href="#explorar">Rankings por critério</a>
      <a href="#metodologia">Metodologia</a>
    </div>
  </nav>

  <section class="rk2-result" id="rk2-result" hidden aria-live="polite"></section>

  <noscript>
    <div class="rk2-shell rk2-noscript">
      A consulta interativa exige JavaScript. A lista, os critérios de interpretação e a metodologia permanecem descritos nesta página;
      para abrir perfis e comparações, ative o JavaScript no navegador.
    </div>
  </noscript>

  <section class="rk2-section" id="comparar" aria-labelledby="rk2-compare-title">
    <div class="rk2-shell">
      <div class="rk2-section-head">
        <span class="rk2-kicker">Comparação entre seguradoras</span>
        <h2 id="rk2-compare-title">Compare seguradoras lado a lado</h2>
        <p>
          Escolha de duas a quatro seguradoras. Os mesmos sinais são apresentados em paralelo, sem somar pontos,
          preencher ausências com zero ou declarar uma vencedora automática.
        </p>
      </div>

      <div class="rk2-compare-picker">
        <div class="rk2-compare-picker__top">
          <label for="rk2-compare-search">Adicionar seguradora</label>
          <div class="rk2-compare-picker__status">
            <span id="rk2-compare-count">0 de 4 selecionadas</span>
            <button
              type="button"
              id="rk2-compare-share"
              class="rk2-btn rk2-btn--ghost rk2-btn--compact rk2-share-btn"
              disabled
              aria-disabled="true"
              title="Selecione ao menos duas seguradoras para compartilhar a comparação"
            >
              <svg class="rk2-share-btn__icon" viewBox="0 0 24 24" width="16" height="16" aria-hidden="true" focusable="false">
                <path d="M18 8a3 3 0 1 0-2.83-4A3 3 0 0 0 15 5c0 .18.02.36.05.53l-6.1 3.05A3 3 0 0 0 7 8a3 3 0 1 0 1.95 5.28l6.1 3.05A3 3 0 0 0 15 17a3 3 0 1 0 .9-2.14L9.8 11.8c.13-.52.13-1.08 0-1.6l6.1-3.05A3 3 0 0 0 18 8Z" fill="currentColor"/>
              </svg>
              <span>Compartilhar comparação</span>
            </button>
            <button type="button" id="rk2-compare-clear" class="rk2-text-action" hidden>Limpar comparação</button>
          </div>
        </div>
        <div class="rk2-mini-search">
          <input
            id="rk2-compare-search"
            type="search"
            placeholder="Digite um nome…"
            autocomplete="off"
            maxlength="120"
            aria-autocomplete="list"
            aria-controls="rk2-compare-results"
            aria-expanded="false"
            aria-label="Buscar seguradora para adicionar à comparação"
            disabled
            aria-disabled="true"
          >
          <div
            class="rk2-mini-results"
            id="rk2-compare-results"
            role="listbox"
            aria-label="Seguradoras encontradas para comparação"
            hidden
          ></div>
        </div>
        <div class="rk2-compare-chips" id="rk2-compare-chips"></div>
      </div>

      <div class="rk2-compare-empty" id="rk2-compare-empty">
        Adicione pelo menos duas seguradoras para visualizar a comparação.
      </div>
      <div class="rk2-compare-grid" id="rk2-compare-grid" hidden></div>
    </div>
  </section>

  <section class="rk2-section rk2-section--soft" id="lista" aria-labelledby="rk2-list-title">
    <div class="rk2-shell">
      <div class="rk2-section-head">
        <span class="rk2-kicker">Seguradoras autorizadas</span>
        <h2 id="rk2-list-title">Lista de seguradoras autorizadas pela SUSEP</h2>
        <p>
          Consulte as seguradoras identificadas no cadastro regulatório usado pela ferramenta. Quando ainda não há dados suficientes
          para uma análise completa, a empresa continua pesquisável e o limite é informado com clareza.
        </p>
      </div>

      <div class="rk2-list-tools">
        <label class="rk2-field" for="rk2-list-search">
          <span>Filtrar por nome</span>
          <input id="rk2-list-search" type="search" placeholder="Nome da seguradora…" maxlength="120" disabled aria-disabled="true">
        </label>
        <div class="rk2-segmented" role="group" aria-label="Filtrar por cobertura da avaliação">
          <button type="button" class="is-active" data-list-filter="all">Todas</button>
          <button type="button" data-list-filter="eligible">Avaliação comparável</button>
          <button type="button" data-list-filter="incomplete">Avaliação limitada</button>
        </div>
      </div>

      <div class="rk2-list-status" id="rk2-list-status" aria-live="polite"></div>
      <div class="rk2-list" id="rk2-list"></div>

      <nav class="rk2-pagination" id="rk2-list-pagination" aria-label="Paginação da lista de seguradoras">
        <button type="button" id="rk2-list-prev" aria-label="Página anterior">←</button>
        <div class="rk2-pagination__text">
          <strong id="rk2-list-page">Página 1 de 1</strong>
          <span id="rk2-list-range">0 seguradoras</span>
        </div>
        <button type="button" id="rk2-list-next" aria-label="Próxima página">→</button>
      </nav>
    </div>
  </section>

  <section class="rk2-section" id="explorar" aria-labelledby="rk2-explore-title">
    <div class="rk2-shell">
      <div class="rk2-section-head">
        <span class="rk2-kicker">Ranking de seguradoras por critério</span>
        <h2 id="rk2-explore-title">Ranking de seguradoras: maiores, capital, liquidez e reclamações</h2>
        <p>
          Cada lista responde a uma pergunta específica: quais são as maiores seguradoras, quais apresentaram maior relação de capital,
          maior liquidez ou menor/maior pressão relativa de reclamações. Os critérios não são somados para fabricar um ranking geral de “melhores seguradoras”.
        </p>
      </div>

      <div class="rk2-explore-grid" id="rk2-explore-grid">
        <div class="rk2-loading-card">Carregando opções de exploração…</div>
      </div>

      <div class="rk2-board-panel" id="rk2-board-panel" hidden></div>

      <details class="rk2-collections" id="rk2-collections">
        <summary>Ver outros grupos sem ordem de melhor ou pior</summary>
        <div class="rk2-collections__body" id="rk2-collections-body">
          Carregando coleções semânticas…
        </div>
      </details>
    </div>
  </section>

  <section class="rk2-section rk2-section--trust" id="confiabilidade" aria-labelledby="rk2-trust-title">
    <div class="rk2-shell">
      <div class="rk2-section-head rk2-section-head--left">
        <span class="rk2-kicker">Como ler o resultado</span>
        <h2 id="rk2-trust-title">Como saber se uma seguradora é confiável?</h2>
        <p>
          A ferramenta separa identidade, situação regulatória, sinais financeiros e reclamações porque cada informação responde
          a uma pergunta diferente — e nenhuma delas, sozinha, garante como será a experiência futura do consumidor.
        </p>
        <div class="rk2-trust-answer">
          <strong>Resposta curta:</strong>
          <p>
            Para avaliar se uma seguradora é confiável, primeiro confirme quem é a empresa e sua situação na SUSEP. Depois, leia capital,
            liquidez e reclamações como sinais independentes. A Sanida não transforma falta de dado em nota ruim, nem declara uma vencedora automática.
          </p>
        </div>
      </div>

      <div class="rk2-trust-grid">
        <article>
          <span>1</span>
          <h3>Quem é a entidade?</h3>
          <p>Marca, seguradora e participante Sandbox não são tratados como sinônimos. Relações de risco e sucessão aparecem quando documentadas.</p>
        </article>
        <article>
          <span>2</span>
          <h3>Há sinal financeiro atual?</h3>
          <p>Capital e liquidez são lidos separadamente. Ausência de dado permanece ausência e não é convertida em desempenho neutro.</p>
        </article>
        <article>
          <span>3</span>
          <h3>Como estão as reclamações?</h3>
          <p>O volume só é comparado ao tamanho da operação quando há dados compatíveis para fazer essa conta sem distorção.</p>
        </article>
      </div>
    </div>
  </section>

  <section class="rk2-section rk2-section--method" id="metodologia" aria-labelledby="rk2-method-title">
    <div class="rk2-shell rk2-method">
      <div class="rk2-section-head rk2-section-head--left">
        <span class="rk2-kicker">Como a leitura funciona</span>
        <h2 id="rk2-method-title">Da situação na SUSEP aos sinais de capital, liquidez e reclamações</h2>
        <p>
          A ferramenta começa confirmando quem é a empresa e sua situação regulatória. Depois lê sinais diferentes sem misturá-los em uma nota única: capital, liquidez e reclamações respondem perguntas distintas.
        </p>
      </div>

      <div class="rk2-method-grid rk2-method-grid--expanded">
        <details>
          <summary>Primeiro: quem estamos avaliando e qual é sua situação na SUSEP?</summary>
          <p>A análise começa pela identidade. Nome comercial, razão social e empresa que assume o risco podem não ser a mesma coisa. Por isso a ferramenta confirma a pessoa jurídica, o CNPJ, o código SUSEP e a situação regulatória antes de interpretar qualquer número.</p>
          <p><strong>A SUSEP é a referência para autorização e supervisão.</strong> Informações cadastrais complementares ajudam a distinguir empresas atuais, históricas, sucessoras, marcas e outras relações relevantes. Uma marca não herda automaticamente a avaliação financeira ou as reclamações de outra empresa apenas por estar ligada a ela.</p>
          <p>Essa etapa evita um erro básico: atribuir capital, liquidez ou reclamações à empresa errada.</p>
        </details>

        <details>
          <summary>Foto ou filme: em que momento a empresa está sendo observada?</summary>
          <p><strong>Capital e liquidez são uma foto.</strong> Os valores exibidos pertencem a uma competência financeira de referência e mostram como esses indicadores estavam naquele mês.</p>
          <p><strong>O filme aparece quando o histórico acrescenta contexto.</strong> A metodologia observa a trajetória operacional e a estabilidade dos dados quando há meses suficientes. Em reclamações, a leitura principal usa uma janela de até 12 meses e pode observar persistência e tendência separadamente.</p>
          <p>Assim, uma foto desfavorável não é automaticamente tratada como tendência permanente; da mesma forma, uma boa fotografia isolada não apaga um histórico que mereça atenção.</p>
        </details>

        <details>
          <summary>Capital: o que é PLA/CMR e quem define o mínimo?</summary>
          <p>O <strong>Capital Mínimo Requerido (CMR)</strong> é uma exigência prudencial aplicável às seguradoras supervisionadas. Ele não é escolhido pela Sanida nem livremente pela empresa. Pelas regras prudenciais, o CMR corresponde ao maior entre o capital-base e o capital de risco; este último considera exposições como subscrição, crédito, mercado e risco operacional.</p>
          <p>O <strong>PLA/CMR</strong> compara o patrimônio prudencial considerado na análise com esse mínimo. A relação é simples: patrimônio prudencial dividido pelo capital mínimo requerido.</p>
          <p><strong>Como interpretar:</strong> 1,0 significa igualdade com o requisito; abaixo de 1,0 significa que o patrimônio observado ficou aquém do mínimo; acima de 1,0 significa que o requisito foi atendido. A metodologia não transforma excesso de capital em mérito ilimitado.</p>
          <p><strong>Por que importa:</strong> capital é uma margem de proteção contra perdas inesperadas e faz parte da avaliação de solvência. Uma insuficiência é um alerta relevante, mas não permite concluir, sozinha, que a seguradora esteja insolvente ou que deixará de pagar um sinistro específico.</p>
          <p><strong>Fonte e referência:</strong> informações prudenciais e regras de capital publicadas pela SUSEP.</p>
        </details>

        <details>
          <summary>Liquidez: o que o ILT mede e por que ele é diferente do capital?</summary>
          <p>O <strong>Índice de Liquidez Total (ILT)</strong> compara recursos realizáveis no curto e no longo prazo com compromissos também de curto e longo prazo. Ele responde a uma pergunta diferente da de capital: <strong>os recursos considerados pela fórmula acompanham o conjunto de obrigações?</strong></p>
          <p><strong>Como interpretar:</strong> 1,0 é o ponto de equilíbrio aritmético da relação. Abaixo de 1,0 há pressão entre recursos e compromissos; acima de 1,0 a referência aritmética está atendida. Esse 1,0 não é um limite prudencial oficial da SUSEP.</p>
          <p><strong>Por que importa:</strong> uma empresa pode cumprir o requisito de capital e, ainda assim, apresentar uma relação de liquidez que mereça atenção. Capital e liquidez são, portanto, sinais complementares e não compensatórios.</p>
          <p>A SUSEP também define o <strong>Índice de Liquidez Corrente (ILC)</strong>, mais voltado ao curto prazo. Nesta metodologia, o ILT é o sinal principal de liquidez e o ILC permanece como diagnóstico complementar.</p>
          <p><strong>Fonte e referência:</strong> balanços das supervisionadas e a metodologia de índices econômico-financeiros publicada pela SUSEP.</p>
        </details>

        <details>
          <summary>Reclamações: por que o tamanho da operação entra na conta?</summary>
          <p>Contar reclamações sem considerar o tamanho da empresa pode ser enganoso. Uma seguradora com muito mais negócios tende a ter mais oportunidades de receber reclamações. Por isso a pergunta central é: <strong>“o número observado é alto ou baixo para uma operação desse tamanho?”</strong></p>
          <p>A medida de tamanho usada é o <strong>prêmio direto de seguros</strong>. Neste contexto, “prêmio” é o valor contabilizado da atividade de seguros; não é recompensa, nem representa diretamente número de clientes, apólices ou sinistros.</p>
          <p>Para cada mês comparável, a referência é construída proporcionalmente:</p>
          <p><code>reclamações esperadas = reclamações do mercado × participação da seguradora no volume de seguros</code></p>
          <p>Depois, as reclamações observadas são comparadas com essa referência. Uma razão de 1,0 significa coincidência; acima de 1 indica mais reclamações do que a referência proporcional ao tamanho; abaixo de 1 indica menos.</p>
          <p>A própria SUSEP também utiliza o princípio de ponderar reclamações pelo volume arrecadado em seus indicadores. A ferramenta da Sanida segue a mesma lógica de proporcionalidade, mas não reproduz o indicador da SUSEP: usa alinhamento mensal, escopo de seguros e critérios próprios de comparabilidade.</p>
        </details>

        <details>
          <summary>Quando uma diferença de reclamações é suficiente para sustentar uma conclusão?</summary>
          <p>A razão observadas/esperadas mostra a <strong>direção</strong> da diferença, mas não diz sozinha se essa diferença é consistente. Dez reclamações contra uma referência de oito, por exemplo, têm peso estatístico muito diferente de mil contra oitocentas.</p>
          <p>Por isso a metodologia considera a quantidade de eventos, a cobertura temporal e a incerteza própria de contagens. Tecnicamente, é calculado um intervalo exato baseado em um modelo de Poisson — um modelo estatístico apropriado para contar eventos ao longo de uma exposição. Na prática, ele serve para evitar que pequenas oscilações sejam apresentadas como evidência forte.</p>
          <p>A leitura anual exige pelo menos <strong>9 meses comparáveis em uma janela de 12</strong>. Reclamações e volume de seguros precisam pertencer aos mesmos meses. Quando essa equivalência não existe, a ferramenta não fabrica uma comparação.</p>
          <p>Persistência e tendência são observadas separadamente quando há dados suficientes; elas não substituem a conclusão da janela principal.</p>
        </details>

        <details>
          <summary>Por que zero, ausência e dado insuficiente são tratados de formas diferentes?</summary>
          <p><strong>Zero é um valor observado.</strong> Ausência significa que a informação necessária não estava disponível ou não podia ser usada. Dado insuficiente significa que existe alguma informação, mas ela não basta para a conclusão pretendida.</p>
          <p>Essa diferença tem consequência prática. Transformar ausência em zero poderia fazer parecer que uma empresa teve zero reclamações, zero volume de seguros ou zero capital quando, na realidade, o dado apenas não estava disponível.</p>
          <p>Mesmo um zero verdadeiro precisa de contexto: zero reclamações não é automaticamente um selo de bom atendimento se a exposição foi pequena ou se o período comparável foi insuficiente.</p>
        </details>

        <details>
          <summary>Fontes, atualização e limites da leitura</summary>
          <p><strong>SUSEP:</strong> identidade regulatória, autorização, informações prudenciais, balanços e volume de seguros. <strong>Consumer.gov.br:</strong> reclamações utilizadas na camada de Conduta. Fontes cadastrais oficiais complementares podem ser usadas para confirmar pessoas jurídicas, sucessões e relações de mercado.</p>
          <p>As bases não são atualizadas no mesmo ritmo. Por isso cada indicador preserva sua própria competência ou janela de observação e a ferramenta evita combinar períodos incompatíveis.</p>
          <p>Os indicadores respondem perguntas diferentes. Capital não compensa automaticamente liquidez; liquidez não apaga uma pressão de reclamações; menos reclamações não prova melhor atendimento; e nenhum sinal isolado é tratado como veredito geral sobre a seguradora.</p>
          <p>A metodologia não produz uma nota geral nem escolhe uma “melhor seguradora” a partir de uma média de indicadores.</p>
        </details>
      </div>

      <p class="rk2-method__note">A metodologia Sanida interpreta dados públicos para ajudar na decisão. Ela não é certificação da SUSEP, não prevê o futuro e não transforma um único indicador em veredito sobre a seguradora.</p>
    </div>
  </section>

  <div class="rk2-toast" id="rk2-toast" role="status" aria-live="polite" hidden></div>
</main>

<section id="sessao4" aria-label="Produtos e serviços da Sanida">
  <?php include $_SERVER['DOCUMENT_ROOT'] . "/PHP/section-produtos.php"; ?>
</section>

<section id="sessao5" aria-label="Contato com a Sanida">
  <?php include $_SERVER['DOCUMENT_ROOT'] . "/PHP/section-formulario.php"; ?>
</section>

<?php include $_SERVER['DOCUMENT_ROOT'] . "/PHP/footer.php"; ?>
<?php include $_SERVER['DOCUMENT_ROOT'] . "/PHP/btn-whatsapp.php"; ?>
</body>
</html>
