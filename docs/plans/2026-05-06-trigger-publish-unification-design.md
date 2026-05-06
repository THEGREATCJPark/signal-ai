# Trigger Publish Unification Design

## Goal

Unify X trigger publishing with the Telegram article shape, keep trigger text compact, auto-publish official high-signal posts, and make daily X publishing use the same per-article path that trigger approvals already use.

## Design

Trigger articles keep a single canonical article shape: title, short Korean summary, and source URL. Telegram continues to render that shape as HTML through `bot.formatter.format_article`. X renders the same content as plain text with a strict five-line cap and the normal 280-character tweet limit.

Daily X publishing should stop using a single multi-title digest tweet. Instead, it should call `post_article()` for each unpublished top article, matching the trigger approval path and marking only successfully posted articles as published.

Hourly X trigger scans continue to create GitHub review issues. For official high-signal posts, the scan also auto-publishes immediately and labels the issue as auto-published. The first auto-publish gate is conservative: the account category must be `official`, and the tweet or summary must contain launch/model/product terms such as GPT, ChatGPT, rollout, release, API, model, Claude, or Gemini.

## Error Handling

Auto-publish failures should not prevent issue creation or cursor saving. The scan should record failure in the issue comment when possible, and keep the review path available.

## Tests

Add tests for compact trigger text, daily per-article X publishing, auto-publish classification, and scan-time auto-publish side effects.
