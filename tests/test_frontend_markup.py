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
        self.assertIn("position: fixed;", stage)
        self.assertIn("pointer-events: none;", stage)
        self.assertNotIn("border-bottom", stage)
        self.assertNotIn("min-height", open_physics)
        self.assertIn("position: fixed;", sheet)
        self.assertIn("Daily Summary", html)
        self.assertNotIn("오늘의 모든 업데이트를 한 번에 읽기", html)


if __name__ == "__main__":
    unittest.main()
