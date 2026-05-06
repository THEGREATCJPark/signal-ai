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
        self.assertEqual(post.call_args.args[0], "https://api.twitter.com/2/tweets")
        self.assertIsNotNone(post.call_args.kwargs["auth"])
        self.assertNotIn("Authorization", post.call_args.kwargs.get("headers", {}))
        self.assertNotIn("data", post.call_args.kwargs)

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

            with patch.object(x_poster.requests, "post", side_effect=[ForbiddenResponse(), SuccessResponse()]) as post:
                result = x_poster.post_tweet("hello")

        self.assertEqual({"id": "2", "text": "hello"}, result)
        self.assertEqual(post.call_count, 2)
        self.assertEqual(post.call_args_list[0].args[0], "https://api.twitter.com/2/tweets")
        self.assertEqual(post.call_args_list[1].args[0], "https://api.twitter.com/1.1/statuses/update.json")
        self.assertEqual(post.call_args_list[1].kwargs["data"], {"status": "hello"})


if __name__ == "__main__":
    unittest.main()
