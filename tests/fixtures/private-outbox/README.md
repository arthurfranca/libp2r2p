These snapshots were captured from the installed `libp2r2p@0.11.24` session using its public enqueue API and an in-memory outbox adapter. They contain synthetic text and public identifiers only.

- `initial`: durable intent before local persistence; the quoted context is already local.
- `upload`: first of two file chunks is published and all local copies are saved.
- `partial`: local copies are saved and the context publication has succeeded; only the main text remains.

The upgrade test reads these original shapes without adding new publication fields, then verifies identity, local-save and remote-publication progress in the current pipeline.
