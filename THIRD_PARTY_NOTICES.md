# Third-party notices

`dot_bridge/wecom.py` adapts the WeCom AI-bot WebSocket protocol handling and QR
enrollment flow from Hermes Agent (`plugins/platforms/wecom/adapter.py`, commit
0670ba45240b734c1e6f6d1d5ead87233f37df49). `dot_bridge/openai_auth.py` follows
the Codex device-login and refresh request shapes used by Hermes Agent
`hermes_cli/auth_codex.py`. Hermes Agent is distributed under the MIT License:

```text
MIT License

Copyright (c) 2025 Nous Research

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

No code is included from the following projects; ChatGPT dot request shapes and the
live-update protocol were learned from reading them:

- chatgpt-dot-cli v0.2.3 (https://gitlab.com/aa22396584/chatgpt-dot-cli)
- pi-plugins, pi-desk dot host (https://github.com/gunba/pi-plugins)
- beeper/chatgpt-dots (https://github.com/beeper/chatgpt-dots)
