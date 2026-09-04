<?php
// 1. Garante o carregamento da configuração no topo do arquivo
if (!isset($config)) {
    $config = include $_SERVER['DOCUMENT_ROOT'] . '/PHP/config-site.php';
}
$sanida_page_robots = (
    isset($page_robots)
    && is_string($page_robots)
    && trim($page_robots) !== ''
) ? trim($page_robots) : 'index, follow, max-image-preview:large';
?>
<head>
  <script>
    // Passa a lista de produtos do PHP para o JS globalmente
    window.SANIDA_PRODUTOS = <?php echo json_encode($config['produtos']); ?>;
  </script>
  <script async src="https://www.googletagmanager.com/gtag/js?id=G-N7HTF1E4TD"></script>
  <script>
    window.dataLayer = window.dataLayer || [];
    function gtag(){dataLayer.push(arguments);}
    gtag('js', new Date());
    gtag('config', 'G-N7HTF1E4TD');
  </script>

  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">

  <title><?php echo isset($page_title) ? $page_title : 'Sanida Corretora de Seguros'; ?></title>
  <meta name="description" content="<?php echo isset($page_desc) ? $page_desc : ''; ?>">
  <meta name="author" content="Sanida Corretora de Seguros">
  <meta name="robots" content="<?php echo htmlspecialchars($sanida_page_robots, ENT_QUOTES, 'UTF-8'); ?>">

  <?php if(isset($canonical)): ?>
  <link rel="canonical" href="<?php echo $canonical; ?>" />
  <?php endif; ?>

  <meta property="og:locale" content="pt_BR">
  <meta property="og:site_name" content="Sanida Corretora de Seguros">
  <meta property="og:title" content="<?php echo isset($page_title) ? $page_title : 'Sanida Corretora'; ?>">
  <meta property="og:description" content="<?php echo isset($page_desc) ? $page_desc : ''; ?>">
  <meta property="og:url" content="<?php echo isset($canonical) ? $canonical : 'https://sanida.com.br'; ?>">
  <meta property="og:type" content="website">

  <?php if(isset($page_image)): ?>
  <meta property="og:image" content="<?php echo $page_image; ?>">
  <?php else: ?>
  <meta property="og:image" content="https://sanida.com.br/IMG/sanida-seguros-logomarca-og.png">
  <?php endif; ?>

  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="<?php echo isset($page_title) ? $page_title : 'Sanida Corretora'; ?>">
  <meta name="twitter:description" content="<?php echo isset($page_desc) ? $page_desc : ''; ?>">
  <meta name="twitter:image" content="<?php echo isset($page_image) ? $page_image : 'https://sanida.com.br/IMG/sanida-seguros-logomarca-og.png'; ?>">

  <link rel="icon" type="image/png" href="/IMG/favicon85-32-32.png?v=2" sizes="32x32">
  <link rel="icon" type="image/svg+xml" href="/IMG/favicon85-32-32.svg" sizes="any">
  <link rel="icon" type="image/x-icon" href="/IMG/favicon-solid-rounded.ico">
  <link rel="icon" type="image/png" href="/IMG/favicon85-192-192.png" sizes="192x192">
  <link rel="apple-touch-icon" href="/IMG/favicon85-180-180.png">

  <link rel="preload" href="/fonts/blinker-600.woff2" as="font" type="font/woff2" crossorigin>

  <link rel="preload" href="/CSS/css1.css?v=9" as="style">
  <link rel="stylesheet" href="/CSS/css1.css?v=9">

  <link rel="preload" as="style" href="https://unpkg.com/swiper/swiper-bundle.min.css" onload="this.onload=null;this.rel='stylesheet'">
  <noscript><link rel="stylesheet" href="https://unpkg.com/swiper/swiper-bundle.min.css"></noscript>

  <style>.formulario-website {display: none !important;} </style>

  <script src="https://unpkg.com/boxicons@2.1.4/dist/boxicons.js" defer></script>
  <script src="https://unpkg.com/swiper/swiper-bundle.min.js" defer></script>
  <script src="/JS/menu.js?v=2" defer></script>
  <script src="/JS/swiper-produtos.js" defer></script>
