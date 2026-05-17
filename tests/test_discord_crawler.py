import unittest
import sys
from unittest.mock import patch


class DiscordCrawlerPlatformTest(unittest.TestCase):
    def test_windows_exporter_detection_does_not_call_unix_which(self):
        from crawlers import discord

        with patch.object(discord.os, "name", "nt"), \
             patch.object(discord.subprocess, "run") as run:
            self.assertFalse(discord.use_linux_exporter())

        run.assert_not_called()

    def test_windows_export_command_uses_current_python(self):
        from crawlers import discord

        with patch.object(discord, "use_linux_exporter", return_value=False):
            command = discord.export_command("2026-05-16 00:00:00")

        self.assertEqual(sys.executable, command[0])
        self.assertIn("discord_export_text_only.py", command[1])


if __name__ == "__main__":
    unittest.main()
