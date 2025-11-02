#!/usr/bin/env python3
"""Interactive prune script for placeholder services and masters.

Usage (dry-run preview):
  python3 bot/scripts/prune_placeholders.py

To actually delete, run and type YES when prompted, or pass --yes to skip prompt.

IMPORTANT: Make a DB backup before running. This script runs DELETEs.
"""
from __future__ import annotations
import asyncio
import argparse
import os
import sys
from typing import List, Set
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy import text

try:
    from bot.app.core.db import get_session
    from bot.app.domain.models import Master, Service
except Exception as e:
    print("This script must be run from project root with project on PYTHONPATH and env configured.", file=sys.stderr)
    raise


PLACEHOLDER_SERVICES: List[str] = ["haircut", "color", "nails", "brows"]


def env_id_set(name: str) -> Set[int]:
    raw = os.getenv(name) or ""
    return {int(x) for x in raw.split(",") if x.strip().isdigit()}


async def preview_candidates(keep_masters: Set[int]) -> tuple[List[str], List[int]]:
    # admin/master ids from env are used only to avoid accidental deletion
    admin_ids = env_id_set("ADMIN_IDS")
    master_ids = env_id_set("MASTER_IDS")
    svc_candidates: List[str] = []
    master_candidates: List[int] = []

    async with get_session() as s:
        print("=== Services preview ===")
        for sid in PLACEHOLDER_SERVICES:
            r = await s.execute(text("SELECT id, name, created_at FROM services WHERE id = :sid"), {"sid": sid})
            row = r.first()
            if not row:
                print(f"{sid}: MISSING")
                continue
            b = await s.execute(text("SELECT COUNT(*) FROM bookings WHERE service_id = :sid"), {"sid": sid})
            bcount = b.scalar_one() or 0
            print(f"{sid}: FOUND name={row[1]!r} created_at={row[2]!s} bookings={bcount}")
            if bcount == 0:
                svc_candidates.append(sid)

        print("\n=== Masters preview ===")
        mres = await s.execute(text("SELECT telegram_id, name FROM masters ORDER BY telegram_id"))
        for tg, name in mres.all():
            b = await s.execute(text("SELECT COUNT(*) FROM bookings WHERE master_id = :tg"), {"tg": tg})
            bcount = b.scalar_one() or 0
            sc = await s.execute(text("SELECT COUNT(*) FROM master_services WHERE master_telegram_id = :tg"), {"tg": tg})
            scount = sc.scalar_one() or 0
            print(f"{tg}\t{name}\tbookings={bcount}\tservices={scount}")
            # Candidate if no bookings and no service links and not in admin/master ids
            if bcount == 0 and scount == 0 and int(tg) not in admin_ids and int(tg) not in master_ids:
                # Respect explicit keep list: never mark kept masters for deletion
                if int(tg) not in keep_masters:
                    master_candidates.append(int(tg))

    return svc_candidates, master_candidates


async def perform_deletion(s: AsyncSession, svcs: List[str], masters: List[int], force_masters: List[int] | None = None) -> None:
    # s is an active session (SQLAlchemy AsyncSession)
    # If force_masters provided, delete their bookings and dependent rows first,
    # then delete the master records. This avoids FK constraint violations.
    forced_set = set(force_masters or [])
    if forced_set:
        for ftg in forced_set:
            print(f"Force-deleting master {ftg} and all related bookings...")
            # Collect booking ids for this master
            bid_rows = await s.execute(text("SELECT id FROM bookings WHERE master_id = :tg"), {"tg": ftg})
            booking_ids = [row[0] for row in bid_rows.all()]
            if booking_ids:
                # Delete dependent rows referencing bookings
                await s.execute(text("DELETE FROM booking_ratings WHERE booking_id = ANY(:bids)"), {"bids": booking_ids})
                await s.execute(text("DELETE FROM booking_items WHERE booking_id = ANY(:bids)"), {"bids": booking_ids})
                # Delete bookings themselves
                await s.execute(text("DELETE FROM bookings WHERE id = ANY(:bids)"), {"bids": booking_ids})
                print(f"Deleted {len(booking_ids)} bookings for master {ftg}")
            # Now delete master-related rows and master record
            await s.execute(text("DELETE FROM master_client_notes WHERE master_telegram_id = :tg"), {"tg": ftg})
            await s.execute(text("DELETE FROM master_profiles WHERE master_telegram_id = :tg"), {"tg": ftg})
            await s.execute(text("DELETE FROM master_services WHERE master_telegram_id = :tg"), {"tg": ftg})
            await s.execute(text("DELETE FROM masters WHERE telegram_id = :tg"), {"tg": ftg})

    # Delete services and their links
    for sid in svcs:
        print(f"Deleting service {sid} and related rows...")
        await s.execute(text("DELETE FROM master_services WHERE service_id = :sid"), {"sid": sid})
        await s.execute(text("DELETE FROM service_profiles WHERE service_id = :sid"), {"sid": sid})
        await s.execute(text("DELETE FROM services WHERE id = :sid"), {"sid": sid})

    # Delete remaining masters that are not forced (they should be safe — no bookings)
    for tg in masters:
        if tg in forced_set:
            # already handled above
            continue
        print(f"Deleting master {tg} and related rows...")
        await s.execute(text("DELETE FROM master_client_notes WHERE master_telegram_id = :tg"), {"tg": tg})
        await s.execute(text("DELETE FROM master_profiles WHERE master_telegram_id = :tg"), {"tg": tg})
        await s.execute(text("DELETE FROM master_services WHERE master_telegram_id = :tg"), {"tg": tg})
        await s.execute(text("DELETE FROM masters WHERE telegram_id = :tg"), {"tg": tg})


async def main_async(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prune placeholder services and masters (interactive)")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt and delete")
    parser.add_argument("--keep", type=str, default="342342342,6969151921", help="Comma-separated master telegram_ids to keep")
    parser.add_argument("--force-masters", type=str, default="", help="Comma-separated master telegram_ids to force-delete (includes their bookings)")
    parser.add_argument("--nuke", action="store_true", help="Full wipe: delete ALL bookings, masters and services (IRREVERSIBLE). Requires typing YES or --yes")
    parser.add_argument("--purge-services", type=str, default="", help="Comma-separated service names to purge (deletes master_services, service_profiles, and services). Requires --yes or interactive YES")
    args = parser.parse_args(argv)

    # Parse keep list
    keep_set = {int(x) for x in args.keep.split(",") if x.strip().isdigit()}

    svc_candidates, master_candidates = await preview_candidates(keep_set)

    # Parse force masters list
    force_list: List[int] = []
    if args.force_masters:
        force_list = [int(x) for x in args.force_masters.split(",") if x.strip().isdigit()]

    # Combine forced masters into deletion list if not already present
    for fm in force_list:
        if fm not in master_candidates:
            master_candidates.append(fm)

    print("\nCandidates to delete:\n services:", svc_candidates, "\n masters:", master_candidates)
    if not svc_candidates and not master_candidates:
        print("No candidates to delete. Exiting.")
        return 0

    if not args.yes:
        conf = input("Type YES to DELETE the above candidates (irreversible): ")
        if conf.strip() != "YES":
            print("Aborted by user.")
            return 0

    # If user requested targeted purge by service names, perform it and exit
    purge_names: List[str] = []
    if args.purge_services:
        purge_names = [x.strip() for x in args.purge_services.split(",") if x.strip()]

    async with get_session() as s:
        async with s.begin():
            if purge_names:
                print("Targeted purge for service names:", purge_names)
                if not args.yes:
                    conf = input("Type YES to DELETE the above services and their links (irreversible): ")
                    if conf.strip() != "YES":
                        print("Aborted by user.")
                        return 0
                # Find matching service ids
                r = await s.execute(text("SELECT id, name FROM services WHERE name = ANY(:names)"), {"names": purge_names})
                rows = r.all()
                if not rows:
                    print("No services matched the provided names. Exiting.")
                    return 0
                ids = [row[0] for row in rows]
                found_names = [row[1] for row in rows]
                missing = [n for n in purge_names if n not in found_names]
                if missing:
                    print("Warning: the following names were not found and will be skipped:", missing)
                print(f"Purging service ids={ids} names={found_names}")
                # Delete links and profiles then services
                await s.execute(text("DELETE FROM master_services WHERE service_id = ANY(:ids)"), {"ids": ids})
                await s.execute(text("DELETE FROM service_profiles WHERE service_id = ANY(:ids)"), {"ids": ids})
                await s.execute(text("DELETE FROM services WHERE id = ANY(:ids)"), {"ids": ids})
                print("Targeted purge completed.")
                return 0

            if args.nuke:
                # Ask again for nuke to ensure user is explicit
                if not args.yes:
                    conf2 = input("Type YES to PROCEED WITH FULL WIPE (this will delete ALL bookings, masters and services): ")
                    if conf2.strip() != "YES":
                        print("Aborted by user.")
                        return 0
                # perform full wipe
                print("Performing FULL WIPE: deleting booking dependents, bookings, master/service data...")
                # Delete booking-dependent rows first
                await s.execute(text("DELETE FROM booking_ratings"))
                await s.execute(text("DELETE FROM booking_items"))
                await s.execute(text("DELETE FROM bookings"))
                # Delete master-related auxiliary tables
                await s.execute(text("DELETE FROM master_client_notes"))
                await s.execute(text("DELETE FROM master_profiles"))
                await s.execute(text("DELETE FROM master_services"))
                # Delete service-related auxiliary tables
                await s.execute(text("DELETE FROM service_profiles"))
                # Finally remove services and masters
                await s.execute(text("DELETE FROM services"))
                await s.execute(text("DELETE FROM masters"))
                print("Full wipe completed.")
            else:
                await perform_deletion(s, svc_candidates, master_candidates, force_masters=force_list if force_list else None)

    print("Deletion completed.")
    return 0


def main(argv: List[str] | None = None) -> int:
    return asyncio.run(main_async(argv))


if __name__ == "__main__":
    raise SystemExit(main())
