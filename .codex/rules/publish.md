# Publish Rules

- Do not publish `.local/`.
- Do not publish real wallets, private keys, or `.env` files.
- Do not publish client-specific OCIDs or tenancy details.
- Before publishing, run the repository validation scripts and inspect `workspace/oci-mirror/` for redacted-only content.
