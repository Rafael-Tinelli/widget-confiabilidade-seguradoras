<?php
/**
 * R3 — redirecionamento de estados legados do widget.
 *
 * NÃO instalar no index2.php de teste.
 * No cutover autorizado, incluir no topo do index.php de produção antes de qualquer saída HTML.
 *
 * Objetivo:
 *   /ranking-seguradoras/?q=Loovi
 *   /ranking-seguradoras/?perfil=brand:loovi
 *   /ranking-seguradoras/?comparar=id1,id2
 * deixam de ser URLs HTTP distintas e passam a apontar para o hub + estado de aplicação em fragmento.
 */

$rk2_state = null;

if (isset($_GET['perfil']) && is_string($_GET['perfil']) && trim($_GET['perfil']) !== '') {
    $rk2_state = ['perfil', substr(trim($_GET['perfil']), 0, 180)];
} elseif (isset($_GET['comparar']) && is_string($_GET['comparar']) && trim($_GET['comparar']) !== '') {
    $rk2_state = ['comparar', substr(trim($_GET['comparar']), 0, 760)];
} elseif (isset($_GET['q']) && is_string($_GET['q']) && trim($_GET['q']) !== '') {
    $rk2_state = ['consulta', substr(trim($_GET['q']), 0, 120)];
}

if ($rk2_state !== null) {
    [$rk2_kind, $rk2_value] = $rk2_state;
    $rk2_location = '/ranking-seguradoras/#' . $rk2_kind . '=' . rawurlencode($rk2_value);
    header('Location: ' . $rk2_location, true, 301);
    exit;
}
