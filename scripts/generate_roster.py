"""Deterministic provider-roster events. No corpus is shipped.

Every field is a pure function of the record number and a fixed salt, so a
clone reproduces the exact same stream without downloading anything. The same
An experiment whose input cannot be regenerated cannot be re-run by anybody
else, and a corpus in a git repository is a thing nobody reviews.

The stream is a ROSTER FEED, not random rows. That matters for the
experiments:

  A provider appears more than once. Several sources carry the same NPI, and
  later records CORRECT earlier ones. That is what makes upsert-on-natural-key
  a real alternative to append rather than a stylistic choice, and experiment 1
  turns on the difference.

  The key is the NPI. Kafka orders within a partition only, so keying by NPI is
  what keeps a provider's corrections in order once the topic has more than one
  partition. An unkeyed roster feed is a bug that only shows up under load.

Usage:  python3 generate_roster.py <count> [--start N] [--corrections-from N]
Writes JSONL to stdout, one event per line, prefixed with the Kafka key and a
'|' separator so kafka-console-producer can parse it with parse.key=true.
"""

import argparse
import hashlib
import json
import sys

SALT = "roster-ingest-guarantees/v1"

SOURCES = ["payer_feed", "license_board", "site_scrape", "clearinghouse"]
CREDENTIALS = ["MD", "DO", "NP", "PA"]
SPECIALTIES = ["family_medicine", "cardiology", "dermatology", "pediatrics",
               "orthopedics", "psychiatry"]
STATES = ["TX", "CA", "NY", "FL", "IL", "WA"]
CITIES = ["austin", "dallas", "houston", "san_diego", "brooklyn", "tampa"]
FIRST = ["robert", "katherine", "james", "maria", "david", "linda",
         "michael", "susan"]
LAST = ["nguyen", "patel", "okafor", "hernandez", "kowalski", "silva",
        "andersson", "yamamoto"]
NETWORK = ["in_network", "out_of_network", "pending"]


def h(n, field):
    """A stable integer for (record, field). One hash, many independent draws."""
    digest = hashlib.md5(f"{SALT}:{field}:{n}".encode()).hexdigest()
    return int(digest[:8], 16)


def pick(n, field, options):
    return options[h(n, field) % len(options)]


def npi_for(provider_id):
    """A 10 digit NPI. Distinct providers get distinct NPIs by construction."""
    return str(1000000000 + (h(provider_id, "npi") % 900000000))


def event(seq, provider_count, correcting=False):
    """One roster event.

    Provider_count is deliberately smaller than the event count. Providers
    recur: the table is not a log of distinct entities, it is a stream of
    assertions about a smaller set of them.
    """
    provider_id = h(seq, "provider") % provider_count
    npi = npi_for(provider_id)
    # A correction reuses the provider but carries a later revision, so the
    # upsert path has something to collapse and the append path does not.
    revision = 2 if correcting else 1
    return npi, {
        "npi": npi,
        "source": pick(seq, "source", SOURCES),
        "first_name": pick(provider_id, "first", FIRST),
        "last_name": pick(provider_id, "last", LAST),
        "credential": pick(provider_id, "cred", CREDENTIALS),
        "city": pick(provider_id, "city", CITIES),
        "state": pick(provider_id, "state", STATES),
        "zip": str(70000 + (h(provider_id, "zip") % 29999)),
        "specialty": pick(provider_id, "spec", SPECIALTIES),
        "network_status": pick(seq, "net", NETWORK),
        "revision": revision,
        "seq": seq,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("count", type=int)
    ap.add_argument("--start", type=int, default=1)
    ap.add_argument("--providers", type=int, default=2000)
    ap.add_argument("--corrections-from", type=int, default=None,
                    help="sequence number at which events become corrections")
    args = ap.parse_args()

    out = sys.stdout
    for i in range(args.start, args.start + args.count):
        correcting = (args.corrections_from is not None
                      and i >= args.corrections_from)
        key, rec = event(i, args.providers, correcting)
        out.write(f"{key}|{json.dumps(rec, separators=(',', ':'))}\n")


if __name__ == "__main__":
    main()
