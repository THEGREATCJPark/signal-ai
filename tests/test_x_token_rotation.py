import importlib
import os
import unittest
from unittest.mock import patch


class XTokenRotationTest(unittest.TestCase):
    def setUp(self):
        self._env = {
            "X_CLIENT_ID": "client-id",
            "X_CLIENT_SECRET": "client-secret",
            "X_REFRESH_TOKEN": "env-refresh",
        }

    def test_uses_stored_refresh_token_before_env_and_persists_rotated_token(self):
        with patch.dict(os.environ, self._env, clear=False):
            import bot.x_poster as x_poster

            x_poster = importlib.reload(x_poster)
            calls = []

            class Response:
                ok = True

                def json(self):
                    return {"access_token": "access-token", "refresh_token": "rotated-refresh"}

            def fake_post(url, auth=None, data=None, timeout=None):
                calls.append({"url": url, "auth": auth, "data": data, "timeout": timeout})
                return Response()

            with patch.object(x_poster, "_load_stored_refresh_token", return_value="stored-refresh"), \
                 patch.object(x_poster, "_save_stored_refresh_token") as save_token, \
                 patch.object(x_poster.requests, "post", side_effect=fake_post):
                token = x_poster._get_access_token()

        self.assertEqual(token, "access-token")
        self.assertEqual(calls[0]["data"]["refresh_token"], "stored-refresh")
        save_token.assert_called_once_with("rotated-refresh")

    def test_skips_persist_when_refresh_token_does_not_rotate(self):
        with patch.dict(os.environ, self._env, clear=False):
            import bot.x_poster as x_poster

            x_poster = importlib.reload(x_poster)

            class Response:
                ok = True

                def json(self):
                    return {"access_token": "access-token", "refresh_token": "env-refresh"}

            with patch.object(x_poster, "_load_stored_refresh_token", return_value=None), \
                 patch.object(x_poster, "_save_stored_refresh_token") as save_token, \
                 patch.object(x_poster.requests, "post", return_value=Response()):
                token = x_poster._get_access_token()

        self.assertEqual(token, "access-token")
        save_token.assert_not_called()


if __name__ == "__main__":
    unittest.main()
