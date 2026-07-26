(function (root, factory) {
  const api = factory(
    typeof module === 'object' && module.exports
      ? require('./third_party/marked.min.js')
      : root.marked
  );
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.SignalMarkdown = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function (markedApi) {
  'use strict';

  function escapeHtml(value) {
    return String(value || '').replace(/[&<>"']/g, function (char) {
      return {
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
      }[char];
    });
  }

  function safeHref(value) {
    const href = String(value || '').trim().replace(/[\u0000-\u001f\u007f]/g, '');
    if (/^(?:https?:|mailto:)/i.test(href)) return href;
    if (/^(?:#|\/(?!\/)|\.\.?\/)/.test(href)) return href;
    return '';
  }

  function normalizePunctuationStrong(value) {
    return String(value || '').replace(
      /([\p{P}\p{S}])(\*\*)([\p{L}\p{N}])/gu,
      function (_match, punctuation, delimiter, next) {
        return punctuation + delimiter + '&#' + next.codePointAt(0) + ';';
      }
    );
  }

  function decodeNumericEntities(value) {
    return String(value || '').replace(/&#(\d+);/g, function (entity, decimal) {
      const codePoint = Number(decimal);
      if (!Number.isInteger(codePoint) || codePoint < 0 || codePoint > 0x10ffff) return entity;
      return String.fromCodePoint(codePoint);
    });
  }

  const renderer = {
    html(token) {
      const escaped = escapeHtml(token.text || token.raw || '');
      return token.block ? `<p>${escaped.trim()}</p>\n` : escaped;
    },
    link(token) {
      const label = this.parser.parseInline(token.tokens || []);
      const href = safeHref(token.href);
      if (!href) return label;
      const title = token.title ? ` title="${escapeHtml(token.title)}"` : '';
      return `<a href="${escapeHtml(href)}" rel="noopener noreferrer"${title}>${label}</a>`;
    },
    image(token) {
      const src = safeHref(token.href);
      const alt = escapeHtml(token.text || '');
      if (!src || !/^https?:/i.test(src)) return alt;
      const title = token.title ? ` title="${escapeHtml(token.title)}"` : '';
      return `<img src="${escapeHtml(src)}" alt="${alt}" loading="lazy" referrerpolicy="no-referrer"${title}>`;
    }
  };

  let parser = null;
  if (markedApi && typeof markedApi.Marked === 'function') {
    parser = new markedApi.Marked({
      async: false,
      breaks: false,
      gfm: true,
      pedantic: false,
      renderer
    });
  }

  function normalizeHeading(value) {
    return String(value || '')
      .replace(/^\s*#{1,6}\s+/, '')
      .replace(/\s+#+\s*$/, '')
      .replace(/[\*_`~]/g, '')
      .replace(/\s+/g, ' ')
      .trim()
      .toLocaleLowerCase('ko-KR');
  }

  function withoutDuplicateHeading(body, headline) {
    const source = String(body || '').replace(/^\uFEFF/, '').replace(/\r\n?/g, '\n');
    if (!headline) return source.trim();
    const lines = source.split('\n');
    let first = 0;
    while (first < lines.length && !lines[first].trim()) first += 1;
    const match = first < lines.length
      ? lines[first].match(/^\s*#{1,6}\s+(.+?)\s*#*\s*$/)
      : null;
    if (!match || normalizeHeading(match[1]) !== normalizeHeading(headline)) return source.trim();
    lines.splice(0, first + 1);
    while (lines.length && !lines[0].trim()) lines.shift();
    return lines.join('\n').trim();
  }

  function fallbackRender(source) {
    const clean = fallbackPlainText(source);
    return clean ? `<p>${escapeHtml(clean)}</p>` : '';
  }

  function render(body, options) {
    const source = normalizePunctuationStrong(
      withoutDuplicateHeading(body, options && options.headline)
    );
    if (!source) return '';
    if (!parser) return fallbackRender(source);
    try {
      return parser.parse(source);
    } catch (_error) {
      return fallbackRender(source);
    }
  }

  function collectText(token) {
    if (!token) return '';
    if (Array.isArray(token)) return token.map(collectText).join(' ');
    if (token.type === 'space' || token.type === 'br') return ' ';
    if (token.type === 'html') return String(token.text || '').replace(/<[^>]*>/g, ' ');
    if (token.tokens) return collectText(token.tokens);
    if (token.items) return collectText(token.items);
    if (token.type === 'code' || token.type === 'codespan' || token.type === 'text') {
      return String(token.text || '');
    }
    return '';
  }

  function fallbackPlainText(source) {
    return String(source || '')
      .replace(/^\s*#{1,6}\s+/gm, '')
      .replace(/```[\s\S]*?```/g, ' ')
      .replace(/!\[([^\]]*)\]\([^)]*\)/g, '$1')
      .replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
      .replace(/[\*_`~>|]/g, ' ')
      .replace(/^\s*(?:[-+] |\d+[.)]\s+)/gm, '')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function toPlainText(body, options) {
    const source = normalizePunctuationStrong(
      withoutDuplicateHeading(body, options && options.headline)
    );
    if (!source) return '';
    if (!parser) return fallbackPlainText(source);
    try {
      return decodeNumericEntities(collectText(parser.lexer(source))).replace(/\s+/g, ' ').trim();
    } catch (_error) {
      return fallbackPlainText(source);
    }
  }

  return {
    render,
    toPlainText,
    withoutDuplicateHeading,
    ready: Boolean(parser)
  };
});
