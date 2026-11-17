# OpenRouter provider schema

`provider-monitor-schema-v2.openapi.json` is OpenRouter's published schema for
provider model documents, copied unchanged from
<https://openrouter.ai/docs/assets/provider-monitor-schema-v2.openapi.json>
(version 2.4, fetched 2026-11-17).

`dwctl/src/openrouter/feed.rs` validates the documents the OpenRouter feed emits
against it. To update, replace the file with the current download and run the
`openrouter::feed` tests.
