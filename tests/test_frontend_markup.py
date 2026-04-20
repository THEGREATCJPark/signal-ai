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
        self.assertNotIn("transform:", stage)
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

    def test_daily_summary_candidates_use_spring_toggle_and_material_depth(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        self.assertNotIn("perspective:", css_block(html, ".daily-ribbon-stage {"))
        self.assertIn("transform-style: preserve-3d;", css_block(html, ".physics-engine-card {"))
        self.assertIn("state.swing", html)
        self.assertIn("state.vx", html)
        self.assertIn("drawWrinkleLines", html)
        self.assertIn("drawSpecularBand", html)
        self.assertIn("setOpen(!openState)", html)

    def test_daily_summary_has_ten_visual_3d_candidates(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        self.assertIn("html { scroll-behavior: smooth; overflow-x: hidden; }", html)
        self.assertRegex(html, r"body \{[^}]*overflow-x: hidden;")
        self.assertIn('class="physics-lab-grid"', html)
        self.assertIn("const VISUAL_CANDIDATES = [", html)
        for engine_id in [
            "silk-bookmark",
            "brass-chain",
            "glass-prism",
            "wax-seal",
            "origami-fold",
            "carbon-blade",
            "enamel-pin",
            "thread-loop",
            "chrome-clip",
            "kite-cloth",
        ]:
            self.assertIn(f"id: '{engine_id}'", html)
        self.assertIn('data-design-engine="${candidate.id}"', html)
        self.assertIn("VISUAL_CANDIDATES.map", html)
        self.assertIn("function initVisualCandidateLab", html)
        self.assertIn("function drawVisualCandidate", html)
        self.assertIn("function candidatePalette", html)
        self.assertIn("drawFoldedRibbon", html)
        self.assertIn("drawMetalChain", html)
        self.assertIn("drawGlassTag", html)
        self.assertIn("drawWaxSeal", html)
        self.assertIn("data-engine-status", html)
        self.assertIn("canvas.toDataURL", html)
        self.assertIn("setOpen(!openState)", html)
        self.assertNotIn('data-summary-engine="planck-rope"', html)
        self.assertNotIn('data-summary-engine="three-cloth"', html)
        self.assertNotIn("https://cdn.jsdelivr.net/npm/matter-js@", html)
        self.assertNotIn("https://cdnjs.cloudflare.com/ajax/libs/planck-js/", html)
        self.assertNotIn("await import('https://unpkg.com/three@", html)
        self.assertNotIn('class="ribbon-mode-selector"', html)
        self.assertNotIn('<canvas class="ribbon-cloth-canvas"', html)
        self.assertEqual(html.count('class="physics-engine-card'), 1)

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

    def test_mobile_physics_lab_shows_all_ten_candidates_without_side_scroll(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        mobile = html[html.index("@media (max-width: 720px) {\n    .daily-ribbon-stage"):]
        mobile_stage = css_block(mobile, ".daily-ribbon-stage {")
        mobile_grid = css_block(mobile, ".physics-lab-grid {")
        mobile_card = css_block(mobile, ".physics-engine-card {")
        self.assertIn("height: 636px;", mobile_stage)
        self.assertIn("display: grid;", mobile_grid)
        self.assertIn("grid-template-columns: repeat(2, minmax(0, 1fr));", mobile_grid)
        self.assertNotIn("overflow-x: auto;", mobile_grid)
        self.assertNotIn("scroll-snap-type:", mobile_grid)
        self.assertIn("min-height: 91px;", mobile_card)
        self.assertIn("scroll-snap-align: none;", mobile_card)

    def test_daily_summary_close_uses_pointerdown_on_mobile(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        self.assertIn("function closeSummarySheet(e)", html)
        self.assertIn("e.preventDefault();", html)
        self.assertIn("e.stopPropagation();", html)
        self.assertIn("close.addEventListener('pointerdown', closeSummarySheet);", html)
        self.assertIn("close.addEventListener('click', closeSummarySheet);", html)


if __name__ == "__main__":
    unittest.main()
