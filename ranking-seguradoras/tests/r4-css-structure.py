from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
css = (ROOT / 'assets' / 'ranking-v2.css').read_text(encoding='utf-8')
js = (ROOT / 'assets' / 'ranking-v2.js').read_text(encoding='utf-8')
php = (ROOT / 'index2.php').read_text(encoding='utf-8')

assert css.count('{') == css.count('}'), 'CSS braces are unbalanced'
assert 'R11 / §19.6 R4' in css
assert 'append-only' not in css.lower(), 'R4 must not preserve the old append-only status'
assert len(re.findall(r'^\.rk2-regulatory-identity\{', css, re.M)) == 1, 'regulatory identity base style should be consolidated'
assert len(re.findall(r'^\.rk2-tech-help-trigger\{', css, re.M)) == 1, 'technical helper base style should be consolidated'
assert len(re.findall(r'^\.rk2-tech-help\{', css, re.M)) == 1, 'technical helper base style should be consolidated'
assert len(re.findall(r'^\.rk2-data-group__head\{', css, re.M)) == 1, 'data-group header base style should be consolidated'
assert 'grid-template-columns:repeat(auto-fit,minmax(180px,1fr))' in css
assert 'border-left:1px solid #e5edf3' in css
assert '.rk2-signal{\n  display:flex;\n  flex-direction:column;' in css
assert '.rk2-signal__metric{\n  margin-top:auto;' in css
assert 'grid-template-columns:1fr;\n  gap:0;\n  max-width:940px;' in css, 'methodology should use editorial single-column disclosure layout'
assert 'rk2-data-row__label-text' in js
assert 'rk2-data-row__value' in js
assert 'rk2-tech-help__title' not in js
assert 'ranking-v2.css?v=15' in php and 'ranking-v2.js?v=15' in php
print('R4 CSS/visual structure checks: PASS')
