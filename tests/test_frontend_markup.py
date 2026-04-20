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

    def test_mobile_daily_summary_sheet_starts_at_top_and_covers_ribbon(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        mobile = html[html.index("@media (max-width: 720px) {\n    .daily-ribbon-stage"):]
        mobile_sheet = css_block(mobile, ".summary-sheet {")
        title = css_block(html, ".summary-sheet-title {")
        self.assertIn("z-index: 88;", css_block(html, ".summary-sheet {"))
        self.assertIn("top: 0;", mobile_sheet)
        self.assertIn("right: 0;", mobile_sheet)
        self.assertIn("left: 0;", mobile_sheet)
        self.assertIn("width: 100vw;", mobile_sheet)
        self.assertIn("max-height: 100vh;", mobile_sheet)
        self.assertIn("border-radius: 0 0 6px 6px;", mobile_sheet)
        self.assertIn("word-break: keep-all;", title)
        self.assertIn("overflow-wrap: break-word;", title)
        self.assertIn("text-wrap: balance;", title)


if __name__ == "__main__":
    unittest.main()
