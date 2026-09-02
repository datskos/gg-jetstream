#!/usr/bin/env python3
"""Build a deterministic .ggjet account manifest from an arb catalog."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
BASE58_VALUES = {character: index for index, character in enumerate(BASE58_ALPHABET)}


def decode_pubkey(value: str) -> bytes:
    number = 0
    for character in value:
        try:
            digit = BASE58_VALUES[character]
        except KeyError as error:
            raise ValueError(f"invalid base58 character {character!r}") from error
        number = number * 58 + digit

    encoded = (
        number.to_bytes((number.bit_length() + 7) // 8, byteorder="big")
        if number
        else b""
    )
    decoded = b"\0" * (len(value) - len(value.lstrip("1"))) + encoded
    if len(decoded) != 32:
        raise ValueError(f"decoded length is {len(decoded)}, expected 32")
    return decoded


def load_catalog(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as source:
        catalog = json.load(source)
    if not isinstance(catalog, dict):
        raise ValueError("catalog root must be a JSON object")
    if not isinstance(catalog.get("entries"), dict):
        raise ValueError("catalog must contain a top-level entries object")
    return catalog


def build_manifest(catalog: dict[str, Any], source_name: str) -> dict[str, Any]:
    unique_accounts: set[str] = set()
    entries = catalog["entries"]
    for entry_name, entry in entries.items():
        if not isinstance(entry, dict):
            raise ValueError(f"entry {entry_name!r} must be a JSON object")
        required_accounts = entry.get("requiredAccounts")
        if not isinstance(required_accounts, list):
            raise ValueError(f"entry {entry_name!r} is missing requiredAccounts")
        for account in required_accounts:
            if not isinstance(account, str):
                raise ValueError(
                    f"entry {entry_name!r} has a non-string required account"
                )
            unique_accounts.add(account)

    accounts = sorted(unique_accounts)
    digest = hashlib.sha256()
    for ordinal, account in enumerate(accounts):
        try:
            digest.update(decode_pubkey(account))
        except ValueError as error:
            raise ValueError(
                f"invalid account at sorted ordinal {ordinal} ({account!r}): {error}"
            ) from error

    return {
        "version": 1,
        "source": source_name,
        "sourceVersion": catalog.get("version"),
        "sourceGeneratedAtUnix": catalog.get("generatedAtUnix"),
        "accountCount": len(accounts),
        "accountSetSha256": digest.hexdigest(),
        "accounts": accounts,
    }


def write_new_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    created = False
    try:
        with path.open("x", encoding="utf-8") as destination:
            created = True
            json.dump(value, destination, indent=2)
            destination.write("\n")
    except BaseException:
        if created:
            path.unlink(missing_ok=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("catalog", type=Path, help="source arb catalog JSON")
    parser.add_argument("output", type=Path, help="new normalized manifest path")
    arguments = parser.parse_args()

    try:
        catalog = load_catalog(arguments.catalog)
        manifest = build_manifest(catalog, arguments.catalog.name)
        write_new_json(arguments.output, manifest)
    except (OSError, ValueError) as error:
        parser.exit(1, f"error: {error}\n")
    print(
        f"wrote {arguments.output}: accounts={manifest['accountCount']} "
        f"accountSetSha256={manifest['accountSetSha256']}"
    )


if __name__ == "__main__":
    main()
