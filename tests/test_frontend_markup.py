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


if __name__ == "__main__":
    unittest.main()
