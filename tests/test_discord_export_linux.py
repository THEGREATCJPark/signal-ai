import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import discord_export_linux


class DiscordExportLinuxTest(unittest.TestCase):
    def test_dce_cmd_finds_home_bin_when_path_omits_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            dce = home / "bin" / "DiscordChatExporter.Cli"
            dce.parent.mkdir()
            dce.write_text("#!/bin/sh\n", encoding="utf-8")
            dce.chmod(dce.stat().st_mode | stat.S_IXUSR)

            with patch.dict(os.environ, {"HOME": str(home), "PATH": "/usr/bin"}, clear=False):
                with patch("discord_export_linux.shutil.which", return_value=None):
                    self.assertEqual(discord_export_linux.dce_cmd(), [str(dce)])


if __name__ == "__main__":
    unittest.main()
