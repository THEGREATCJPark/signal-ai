from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


def css_block(html, selector):
    start = html.index(selector)
    open_brace = html.index("{", start)
    close_brace = html.index("}", open_brace)
    return html[open_brace + 1:close_brace]


class FrontendMarkupTest(unittest.TestCase):
    def test_summary_text_uses_nonbreaking_slashes(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        self.assertIn("function formatInlineText", html)
        self.assertIn("&#8288;/&#8288;", html)
        self.assertRegex(html, r"summary-copy p \{[^}]*word-break: keep-all;")

    def test_archive_text_uses_nonbreaking_slashes(self):
        html = (ROOT / "docs" / "archive.html").read_text(encoding="utf-8")
        self.assertIn("function formatInlineText", html)
        self.assertIn("&#8288;/&#8288;", html)
        self.assertIn("<h3>${formatInlineText(a.headline)} ${tags.join(' ')}</h3>", html)
        self.assertIn("<p>${formatInlineText(excerpt(a.body))}</p>", html)
        self.assertRegex(html, r"entry \.content h3 \{[^}]*word-break: keep-all;")

    def test_daily_summary_ribbon_does_not_shift_article_flow(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        stage = css_block(html, ".daily-ribbon-stage {")
        sheet = css_block(html, ".summary-sheet {")
        open_physics = css_block(html, ".summary-physics.open {")
        header_start = html.index("<header class=\"masthead\">")
        stage_start = html.index("<section class=\"daily-ribbon-stage\"")
        header_end = html.index("</header>")
        self.assertLess(header_start, stage_start)
        self.assertLess(stage_start, header_end)
        self.assertIn(".masthead {\n    position: relative;", html)
        self.assertIn("position: absolute;", stage)
        self.assertNotIn("position: fixed;", stage)
        self.assertIn("pointer-events: none;", stage)
        self.assertNotIn("border-bottom", stage)
        self.assertNotIn("min-height", open_physics)
        self.assertIn("position: fixed;", sheet)
        self.assertIn("Don't Die.", html)
        self.assertNotIn("Daily Summary", html)
        self.assertNotIn("updates</span>", html)
        self.assertNotIn("오늘의 모든 업데이트를 한 번에 읽기", html)

    def test_model_focus_summary_does_not_render_left_pull_ribbon(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")

        self.assertIn('id="daily-summary-stage"', html)
        self.assertIn("renderDailyRibbon(data.daily_summary);", html)
        self.assertNotIn('id="model-focus-stage"', html)
        self.assertNotIn("model-ribbon-stage", html)
        self.assertNotIn("renderModelFocusRibbon", html)
        self.assertNotIn("Models", html)

    def test_model_focus_main_card_gets_live_treatment(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        live_card = css_block(html, ".model-focus-live-card {")
        live_card_hover = css_block(html, ".model-focus-live-card:hover {")

        self.assertIn("function isModelFocusArticle(a)", html)
        self.assertIn("const liveClass = isModelFocusArticle(a) ? ' model-focus-live-card' : '';", html)
        self.assertIn("const liveEyebrow = isModelFocusArticle(a) ? `<div class=\"model-live-eyebrow\">", html)
        self.assertIn("LIVE · 최신 모델 상황", html)
        self.assertIn("const liveCta = isModelFocusArticle(a) ? `<div class=\"model-live-cta\">Open live model briefing</div>` : '';", html)
        self.assertIn('<article class="card${liveClass}" data-id="${esc(a.id)}">', html)
        self.assertIn("${liveEyebrow}", html)
        self.assertIn("${liveCta}", html)
        self.assertIn("position: relative;", live_card)
        self.assertIn("overflow: hidden;", live_card)
        self.assertIn("box-shadow:", live_card)
        self.assertIn("background:", live_card_hover)

    def test_model_focus_card_omits_duplicate_body_line(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        self.assertIn("function cardCopy(a)", html)
        self.assertIn("if (isModelFocusArticle(a) && copy === String(a.headline || '').trim()) return '';", html)
        self.assertIn("const copyHtml = cardCopy(a);", html)
        self.assertIn("${copyHtml}", html)
        self.assertNotIn("<p>${formatInlineText(excerpt(a.body, 180))}</p>", html)

    def test_daily_summary_sheet_uses_left_aligned_editorial_text(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        sheet = css_block(html, ".summary-sheet {")
        self.assertIn("text-align: left;", sheet)

    def test_daily_summary_ribbon_uses_pendulum_toggle_and_3d_cloth(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        self.assertNotIn("perspective:", css_block(html, ".daily-ribbon-stage {"))
        self.assertIn("perspective(760px)", html)
        self.assertIn("rotateX(var(--ribbon-fold-x))", html)
        self.assertIn("linear-gradient(115deg", html)
        self.assertIn("swingAngle", html)
        self.assertIn("swingVelocity", html)
        self.assertIn("startPendulum", html)
        self.assertIn("setOpen(!state.open)", html)

    def test_mobile_daily_summary_sheet_starts_near_top_with_breathing_room(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        mobile = html[html.index("@media (max-width: 720px) {\n    .daily-ribbon-stage"):]
        mobile_sheet = css_block(mobile, ".summary-sheet {")
        mobile_open_sheet = css_block(mobile, ".summary-physics.open .summary-sheet {")
        title = css_block(html, ".summary-sheet-title {")
        self.assertIn("z-index: 88;", css_block(html, ".summary-sheet {"))
        self.assertIn("top: max(8px, env(safe-area-inset-top));", mobile_sheet)
        self.assertIn("right: 8px;", mobile_sheet)
        self.assertIn("left: 8px;", mobile_sheet)
        self.assertIn("width: auto;", mobile_sheet)
        self.assertIn("max-height: calc(100vh - max(8px, env(safe-area-inset-top)) - 48px);", mobile_sheet)
        self.assertIn("max-height: calc(100dvh - max(8px, env(safe-area-inset-top)) - max(48px, env(safe-area-inset-bottom)));", mobile_sheet)
        self.assertIn("-webkit-overflow-scrolling: touch;", mobile_sheet)
        self.assertIn("border-radius: 6px;", mobile_sheet)
        self.assertIn("padding: 18px 18px max(56px, calc(34px + env(safe-area-inset-bottom)));", mobile_open_sheet)
        self.assertIn("word-break: keep-all;", title)
        self.assertIn("overflow-wrap: break-word;", title)
        self.assertIn("text-wrap: balance;", title)

    def test_daily_summary_close_uses_pointerdown_on_mobile(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        self.assertIn("function closeSummarySheet(e)", html)
        self.assertIn("e.preventDefault();", html)
        self.assertIn("e.stopPropagation();", html)
        self.assertIn("close.addEventListener('pointerdown', closeSummarySheet);", html)
        self.assertIn("close.addEventListener('click', closeSummarySheet);", html)

    def test_index_uses_created_at_for_article_dates_and_side_order(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        self.assertIn(".sort((a,b) => (b.created_at || b.placed_at || '').localeCompare(a.created_at || a.placed_at || ''))", html)
        self.assertIn("fmtDate(a.created_at || a.placed_at || gen)", html)
        self.assertNotIn("fmtDate(a.placed_at || gen)", html)

    def test_narrow_layout_uses_single_chronological_side_stream(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        mobile = html[html.index("@media (max-width: 1280px) {"):]
        self.assertIn('class="side-flow"', html)
        self.assertIn("const sideFlowHtml", html)
        self.assertIn("${side.map((a, i) => sideCard(a, i + 1, true)).join('')}", html)
        self.assertIn(".side { display: none; }", mobile)
        self.assertIn(".side-flow { display: block; }", mobile)
        self.assertIn(".desktop-flow { display: none; }", mobile)

    def test_wide_layout_expands_sidebars_to_available_width(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        layout = css_block(html, ".layout {")
        self.assertIn("width: 100%;", layout)
        self.assertIn("max-width: none;", layout)
        self.assertIn("grid-template-columns: minmax(260px, 1fr) minmax(0, clamp(760px, 48vw, 920px)) minmax(260px, 1fr);", layout)
        self.assertIn("gap: clamp(28px, 3vw, 68px);", layout)
        self.assertNotIn("300px minmax(0, 820px) 300px", html)

    def test_wide_layout_fills_center_and_sidebars_in_fresh_rows(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        self.assertIn('class="desktop-flow"', html)
        self.assertIn("const leftItems = side.filter((_, i) => i % 3 === 0);", html)
        self.assertIn("const centerExtraItems = side.filter((_, i) => i % 3 === 1);", html)
        self.assertIn("const rightItems = side.filter((_, i) => i % 3 === 2);", html)
        self.assertIn("leftItems.map((a, i) => sideCard(a, i * 3 + 1, true)).join('')", html)
        self.assertIn("centerExtraItems.map((a, i) => sideCard(a, i * 3 + 2, true)).join('')", html)
        self.assertIn("rightItems.map((a, i) => sideCard(a, i * 3 + 3, true)).join('')", html)
        self.assertNotIn("const splitAt = Math.ceil(side.length / 2);", html)

    def test_index_prevents_horizontal_overflow_from_responsive_chrome(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        mobile = html[html.index("@media (max-width: 720px) {\n    .daily-ribbon-stage"):]
        mobile_stage = css_block(mobile, ".daily-ribbon-stage {")
        mobile_sheet = css_block(mobile, ".summary-sheet {")
        self.assertIn("html { scroll-behavior: smooth; overflow-x: hidden; }", html)
        self.assertRegex(html, r"body \{[^}]*overflow-x: hidden;")
        self.assertIn("right: 0;", mobile_stage)
        self.assertIn("transform: translateY(-8px);", mobile_sheet)


if __name__ == "__main__":
    unittest.main()
