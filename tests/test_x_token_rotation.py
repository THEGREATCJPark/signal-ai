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

    def test_oauth1_secrets_post_without_refreshing_oauth2_token(self):
        env = {
            **self._env,
            "X_API_KEY": "api-key",
            "X_API_SECRET": "api-secret",
            "X_ACCESS_TOKEN": "access-token",
            "X_ACCESS_TOKEN_SECRET": "access-secret",
        }
        with patch.dict(os.environ, env, clear=False):
            import bot.x_poster as x_poster

            x_poster = importlib.reload(x_poster)

            class Response:
                ok = True
                status_code = 200
                text = '{"data":{"id":"1"}}'

                def json(self):
                    return {"data": {"id": "1"}}

                def raise_for_status(self):
                    return None

            with patch.object(x_poster, "_get_access_token") as get_access_token, \
                 patch.object(x_poster.requests, "post", return_value=Response()) as post:
                result = x_poster.post_tweet("hello")

        self.assertEqual(result, {"id": "1"})
        get_access_token.assert_not_called()
        self.assertEqual(post.call_args.args[0], "https://api.twitter.com/2/tweets")
        self.assertIsNotNone(post.call_args.kwargs["auth"])
        self.assertNotIn("Authorization", post.call_args.kwargs.get("headers", {}))

    def test_oauth1_forbidden_retries_once_with_oauth2_user_context(self):
        env = {
            **self._env,
            "X_API_KEY": "api-key",
            "X_API_SECRET": "api-secret",
            "X_ACCESS_TOKEN": "access-token",
            "X_ACCESS_TOKEN_SECRET": "access-secret",
        }
        with patch.dict(os.environ, env, clear=False):
            import bot.x_poster as x_poster

            x_poster = importlib.reload(x_poster)

            class ForbiddenResponse:
                ok = False
                status_code = 403
                text = '{"title":"Forbidden"}'

                def raise_for_status(self):
                    raise AssertionError("fallback response should be used")

            class CreatedResponse:
                ok = True
                status_code = 201
                text = '{"data":{"id":"2"}}'

                def json(self):
                    return {"data": {"id": "2"}}

                def raise_for_status(self):
                    return None

            with patch.object(x_poster, "_get_access_token", return_value="oauth2-access") as get_access_token, \
                 patch.object(x_poster.requests, "post", side_effect=[ForbiddenResponse(), CreatedResponse()]) as post:
                result = x_poster.post_tweet("hello")

        self.assertEqual(result, {"id": "2"})
        get_access_token.assert_called_once()
        self.assertEqual(post.call_count, 2)
        self.assertIsNotNone(post.call_args_list[0].kwargs["auth"])
        self.assertEqual(
            post.call_args_list[1].kwargs["headers"]["Authorization"],
            "Bearer oauth2-access",
        )

    def test_oauth1_forbidden_and_invalid_oauth2_retries_v1_status_update(self):
        env = {
            **self._env,
            "X_API_KEY": "api-key",
            "X_API_SECRET": "api-secret",
            "X_ACCESS_TOKEN": "access-token",
            "X_ACCESS_TOKEN_SECRET": "access-secret",
        }
        with patch.dict(os.environ, env, clear=False):
            import bot.x_poster as x_poster

            x_poster = importlib.reload(x_poster)

            class ForbiddenResponse:
                ok = False
                status_code = 403
                text = '{"title":"Forbidden"}'

                def raise_for_status(self):
                    raise AssertionError("v1 fallback response should be used")

            class V1CreatedResponse:
                ok = True
                status_code = 200
                text = '{"id_str":"3","text":"hello"}'

                def json(self):
                    return {"id_str": "3", "text": "hello"}

                def raise_for_status(self):
                    return None

            with patch.object(x_poster, "_get_access_token", side_effect=RuntimeError("invalid refresh")), \
                 patch.object(x_poster.requests, "post", side_effect=[ForbiddenResponse(), V1CreatedResponse()]) as post:
                result = x_poster.post_tweet("hello")

        self.assertEqual(result, {"id": "3", "text": "hello"})
        self.assertEqual(post.call_count, 2)
        self.assertEqual(post.call_args_list[1].args[0], "https://api.twitter.com/1.1/statuses/update.json")
        self.assertEqual(post.call_args_list[1].kwargs["data"], {"status": "hello"})

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
