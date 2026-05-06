import importlib
import os
import unittest
from unittest.mock import patch


class XOAuth1PublishTest(unittest.TestCase):
    def test_oauth1_secrets_post_successfully_without_fallbacks(self):
        env = {
            "X_API_KEY": "api-key",
            "X_API_SECRET": "api-secret",
            "X_ACCESS_TOKEN": "access-token",
            "X_ACCESS_TOKEN_SECRET": "access-secret",
            "X_CLIENT_ID": "client-id",
            "X_CLIENT_SECRET": "client-secret",
            "X_REFRESH_TOKEN": "refresh-token",
        }
        with patch.dict(os.environ, env, clear=False):
            import bot.x_poster as x_poster

            x_poster = importlib.reload(x_poster)

            class Response:
                ok = True
                status_code = 201
                text = '{"data":{"id":"1"}}'

                def json(self):
                    return {"data": {"id": "1"}}

                def raise_for_status(self):
                    return None

            with patch.object(x_poster.requests, "post", return_value=Response()) as post:
                result = x_poster.post_tweet("hello")

        self.assertEqual(result, {"id": "1"})
        self.assertEqual(post.call_count, 1)
        self.assertEqual(post.call_args.args[0], "https://api.x.com/2/tweets")
        self.assertIsNotNone(post.call_args.kwargs["auth"])
        self.assertNotIn("Authorization", post.call_args.kwargs.get("headers", {}))
        self.assertNotIn("data", post.call_args.kwargs)

    def test_oauth1_v2_forbidden_retries_oauth2_user_context_before_v1(self):
        env = {
            "X_API_KEY": "api-key",
            "X_API_SECRET": "api-secret",
            "X_ACCESS_TOKEN": "access-token",
            "X_ACCESS_TOKEN_SECRET": "access-secret",
            "X_CLIENT_ID": "client-id",
            "X_CLIENT_SECRET": "client-secret",
            "X_REFRESH_TOKEN": "refresh-token",
        }
        with patch.dict(os.environ, env, clear=False):
            import bot.x_poster as x_poster

            x_poster = importlib.reload(x_poster)

            class ForbiddenResponse:
                ok = False
                status_code = 403
                text = '{"title":"Forbidden"}'

                def raise_for_status(self):
                    raise AssertionError("OAuth2 fallback should be used")

            class SuccessResponse:
                ok = True
                status_code = 201
                text = '{"data":{"id":"2"}}'

                def json(self):
                    return {"data": {"id": "2"}}

                def raise_for_status(self):
                    return None

            with patch.object(x_poster, "_get_access_token", return_value="oauth2-access") as get_access_token, \
                 patch.object(x_poster.requests, "post", side_effect=[ForbiddenResponse(), SuccessResponse()]) as post:
                result = x_poster.post_tweet("hello")

        self.assertEqual({"id": "2"}, result)
        get_access_token.assert_called_once()
        self.assertEqual(post.call_count, 2)
        self.assertIsNotNone(post.call_args_list[0].kwargs["auth"])
        self.assertEqual(post.call_args_list[1].args[0], "https://api.x.com/2/tweets")
        self.assertEqual(post.call_args_list[1].kwargs["headers"]["Authorization"], "Bearer oauth2-access")
        self.assertNotIn("auth", post.call_args_list[1].kwargs)

    def test_oauth1_v2_forbidden_retries_oauth1_v1_status_update(self):
        env = {
            "X_API_KEY": "api-key",
            "X_API_SECRET": "api-secret",
            "X_ACCESS_TOKEN": "access-token",
            "X_ACCESS_TOKEN_SECRET": "access-secret",
            "X_CLIENT_ID": "client-id",
            "X_CLIENT_SECRET": "client-secret",
            "X_REFRESH_TOKEN": "refresh-token",
        }
        with patch.dict(os.environ, env, clear=False):
            import bot.x_poster as x_poster

            x_poster = importlib.reload(x_poster)

            class ForbiddenResponse:
                ok = False
                status_code = 403
                text = '{"title":"Forbidden"}'

                def raise_for_status(self):
                    raise RuntimeError("oauth1 failed")

            class SuccessResponse:
                ok = True
                status_code = 200
                text = '{"id_str":"2","text":"hello"}'

                def json(self):
                    return {"id_str": "2", "text": "hello"}

                def raise_for_status(self):
                    return None

            with patch.object(x_poster, "_get_access_token", side_effect=RuntimeError("invalid refresh")), \
                 patch.object(x_poster.requests, "post", side_effect=[ForbiddenResponse(), SuccessResponse()]) as post:
                result = x_poster.post_tweet("hello")

        self.assertEqual({"id": "2", "text": "hello"}, result)
        self.assertEqual(post.call_count, 2)
        self.assertEqual(post.call_args_list[0].args[0], "https://api.x.com/2/tweets")
        self.assertEqual(post.call_args_list[1].args[0], "https://api.twitter.com/1.1/statuses/update.json")
        self.assertEqual(post.call_args_list[1].kwargs["data"], {"status": "hello"})

    def test_uses_stored_refresh_token_before_env_and_persists_rotated_token(self):
        env = {
            "X_CLIENT_ID": "client-id",
            "X_CLIENT_SECRET": "client-secret",
            "X_REFRESH_TOKEN": "env-refresh",
        }
        with patch.dict(os.environ, env, clear=False):
            import bot.x_poster as x_poster

            x_poster = importlib.reload(x_poster)

            class Response:
                ok = True
                status_code = 200
                text = '{"access_token":"access-token","refresh_token":"rotated-refresh"}'

                def json(self):
                    return {
                        "access_token": "access-token",
                        "refresh_token": "rotated-refresh",
                        "scope": "tweet.read tweet.write users.read offline.access",
                    }

            with patch.object(x_poster, "_load_stored_refresh_token", return_value="stored-refresh"), \
                 patch.object(x_poster, "_save_stored_refresh_token") as save_token, \
                 patch.object(x_poster.requests, "post", return_value=Response()) as post:
                token = x_poster._get_access_token()

        self.assertEqual(token, "access-token")
        self.assertEqual(post.call_args.args[0], "https://api.x.com/2/oauth2/token")
        self.assertEqual(post.call_args.kwargs["data"]["refresh_token"], "stored-refresh")
        save_token.assert_called_once_with("rotated-refresh", "env-refresh")

    def test_env_refresh_token_change_ignores_stale_stored_token(self):
        env = {
            "X_CLIENT_ID": "client-id",
            "X_CLIENT_SECRET": "client-secret",
            "X_REFRESH_TOKEN": "new-env-refresh",
        }
        with patch.dict(os.environ, env, clear=False):
            import bot.x_poster as x_poster

            x_poster = importlib.reload(x_poster)
            stale_hash = x_poster._refresh_token_hash("old-env-refresh")

            class Response:
                ok = True
                status_code = 200
                text = '{"access_token":"access-token","refresh_token":"rotated-refresh"}'

                def json(self):
                    return {"access_token": "access-token", "refresh_token": "rotated-refresh"}

            with patch.object(
                x_poster,
                "_load_pipeline_state",
                return_value={"refresh_token": "stored-refresh", "seed_hash": stale_hash},
            ), patch.object(x_poster, "_save_stored_refresh_token") as save_token, \
                 patch.object(x_poster.requests, "post", return_value=Response()) as post:
                token = x_poster._get_access_token()

        self.assertEqual(token, "access-token")
        self.assertEqual(post.call_args.kwargs["data"]["refresh_token"], "new-env-refresh")
        save_token.assert_called_once_with("rotated-refresh", "new-env-refresh")


if __name__ == "__main__":
    unittest.main()
