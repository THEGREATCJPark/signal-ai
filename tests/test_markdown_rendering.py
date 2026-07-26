import json
from pathlib import Path
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]


class MarkdownRenderingTest(unittest.TestCase):
    def test_front_and_archive_use_shared_markdown_renderer(self):
        for name in ("index.html", "archive.html"):
            html = (ROOT / name).read_text(encoding="utf-8")
            self.assertLess(html.index('src="third_party/marked.min.js"'), html.index('src="markdown-renderer.js"'))
            self.assertLess(html.index('src="markdown-renderer.js"'), html.index('id="articles-data"'))
            self.assertIn("SignalMarkdown.render(a.body, { headline: a.headline })", html)
            self.assertNotIn("function paragraphs(body)", html)

        index = (ROOT / "index.html").read_text(encoding="utf-8")
        self.assertIn("SignalMarkdown.render(summary.body, { headline: title })", index)
        self.assertIn("SignalMarkdown.toPlainText(s, { headline })", index)

    def test_renderer_is_vendored_and_deployed_with_pages(self):
        library = ROOT / "third_party" / "marked.min.js"
        license_file = ROOT / "third_party" / "marked.LICENSE.md"
        workflow = (ROOT / ".github" / "workflows" / "deploy-pages.yml").read_text(encoding="utf-8")

        self.assertGreater(library.stat().st_size, 30_000)
        self.assertIn("marked v15.0.12", library.read_text(encoding="utf-8")[:200])
        self.assertTrue(license_file.is_file())
        self.assertIn("markdown-renderer.js", workflow)
        self.assertIn("third_party/marked.min.js", workflow)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for the renderer behavior test")
    def test_markdown_structure_duplicate_title_and_untrusted_html(self):
        script = r"""
const md = require('./markdown-renderer.js');
const body = `## 최신 모델 위주 정보

소개 **강조**입니다.

### 1. OpenAI

- 첫째
- 둘째

<script>alert('x')</script>

[위험](javascript:alert(1)) [안전](https://example.com)`;
const html = md.render(body, { headline: '최신 모델 위주 정보' });
const plain = md.toPlainText(body, { headline: '최신 모델 위주 정보' });
process.stdout.write(JSON.stringify({ ready: md.ready, html, plain }));
"""
        result = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        rendered = json.loads(result.stdout)

        self.assertTrue(rendered["ready"])
        self.assertNotIn("<h2>최신 모델 위주 정보</h2>", rendered["html"])
        self.assertIn("<strong>강조</strong>", rendered["html"])
        self.assertIn("<h3>1. OpenAI</h3>", rendered["html"])
        self.assertIn("<ul>", rendered["html"])
        self.assertNotIn("<script", rendered["html"])
        self.assertNotIn("javascript:", rendered["html"])
        self.assertIn("&lt;script&gt;", rendered["html"])
        self.assertIn('href="https://example.com"', rendered["html"])
        self.assertFalse(rendered["plain"].startswith("최신 모델 위주 정보"))
        self.assertIn("1. OpenAI", rendered["plain"])

    def test_model_focus_prompt_avoids_duplicate_title_but_allows_structure(self):
        source = (ROOT / "run_hourly.py").read_text(encoding="utf-8")
        rule = "body 안에는 기사 제목을 다시 쓰지 말 것. 내용 구분에는 Markdown `###` 소제목과 목록을 사용해도 됨."
        self.assertEqual(2, source.count(rule))


if __name__ == "__main__":
    unittest.main()
