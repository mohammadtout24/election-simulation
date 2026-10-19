# Import voter phone numbers

The **Phone** column on `/pen` shows Ogero numbers matched to the registered
voters. Voter rows remain available before the phone import; missing numbers
display as a dash.

Use the same Python environment and `DATABASE_URL` as the application. The
import uses its existing pandas, openpyxl and SQLAlchemy dependencies.
Import the 2026 voter registry first, then run from the project root:

```bash
python migrations/import_ogero_phones.py "path/to/Ogero numbers.xlsx" --dry-run
python migrations/import_ogero_phones.py "path/to/Ogero numbers.xlsx"
```

Refresh the voter page after import. `--dry-run` reads the workbook and database
and prints counts without adding columns or changing any records. The regular
import adds the phone column to an existing database if needed and applies all
updates in one transaction. Repeating the import does not duplicate numbers.

The workbook must contain `name` and `phone` columns. A name is matched against
**first name + father's name + family name**, in that order. Arabic presentation
forms, diacritics, keyboard variants and repeated spaces are normalized. Word
boundaries are retained; partial names, reordered names and approximate matches
are not used. Voters missing any of the three name fields are skipped.

The match must identify exactly one voter across the entire registry, including
other towns and pages. Shared names are skipped because the workbook has no
birth dates or registry numbers to distinguish people. A unique name match is
not independent proof that the subscriber and voter are the same person.
Several distinct numbers for a matched name are kept and displayed on separate
lines. Existing phone numbers are preserved. Invalid rows and names with no
match are skipped and included in the printed counts.

Numbers are stored as text. Seven-digit local numbers from numeric Excel cells
get their leading zero restored, and `+961` / `00961` prefixes are accepted.
Re-importing a voter registry file replaces its voters, so run the phone import
again afterward.

Keep the original workbook locally, for example under the ignored `private/`
directory. It contains personal contact data and is not part of the public
repository.

Run the matching, database and template regression checks with:

```bash
python -m unittest discover -s tests -v
```
