from pathlib import Path
import unittest
from unittest.mock import patch

from jinja2 import Environment, FileSystemLoader
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError

from migrations import import_ogero_phones as phones


class PhoneMatchingTests(unittest.TestCase):
    def test_arabic_variants_match_without_collapsing_distinct_names(self):
        self.assertEqual(phones.normalize_name("  أَحْمَد   یاسر ﻻمي "), "احمد ياسر لامي")
        self.assertNotEqual(phones.normalize_name("علي احمد هبة"), phones.normalize_name("علي احمد هبه"))
        self.assertNotEqual(phones.normalize_name("عبد الله احمد مثال"), phones.normalize_name("عبدالله احمد مثال"))

    def test_phone_formats_and_leading_zero(self):
        for value in (1234567, "1234567.0", "01 234 567", "+961 1 234567", "00961-1-234567", "١٢٣٤٥٦٧"):
            with self.subTest(value=value):
                self.assertEqual(phones.normalize_phone(value), "01234567")
        self.assertEqual(phones.normalize_phone("70 123 456"), "70123456")
        for value in (None, "", "nan", "00000000", "123", "-1234567", "01 234 567 ext 9"):
            with self.subTest(value=value):
                self.assertEqual(phones.normalize_phone(value), "")

    def test_multiple_numbers_are_deduplicated_and_invalid_rows_counted(self):
        book, stats = phones.build_phonebook([
            ("علي احمد مثال", "1234567"), ("علي أحمد مثال", "01234567"),
            ("علي احمد مثال", "1987654"), ("", "1234567"), ("علي", "bad"),
        ])
        self.assertEqual(book, {"علي احمد مثال": {"01234567", "01987654"}})
        self.assertEqual(stats, {"source_rows": 5, "invalid_rows": 2})

    def test_load_checks_headers_and_rejects_empty_phonebook(self):
        with patch.object(phones.pd, "read_excel", return_value=pd.DataFrame({" Name ": ["علي احمد مثال"], "PHONE": ["1234567"]})):
            book, _ = phones.load_phonebook("example.xlsx")
            self.assertEqual(book["علي احمد مثال"], {"01234567"})
        for frame in (pd.DataFrame({"name": ["علي"]}), pd.DataFrame({"name": [""], "phone": [""]})):
            with patch.object(phones.pd, "read_excel", return_value=frame):
                with self.assertRaises(ValueError):
                    phones.load_phonebook("example.xlsx")


class DatabaseImportTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE voters_2026 (
                    id INTEGER PRIMARY KEY, first_name TEXT, father_name TEXT,
                    family_name TEXT, mother_name TEXT, town TEXT
                )
            """))

    def tearDown(self):
        self.engine.dispose()

    def add_voter(self, voter_id, first="علي", father="احمد", family="مثال", town="بلدة"):
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO voters_2026 VALUES (:id, :first, :father, :family, 'ام مثال', :town)
            """), {"id": voter_id, "first": first, "father": father, "family": family, "town": town})

    def rows(self):
        with self.engine.connect() as conn:
            return conn.execute(text("SELECT * FROM voters_2026 ORDER BY id")).mappings().all()

    def test_dry_run_never_changes_schema_or_voters(self):
        self.add_voter(1)
        before = self.rows()
        stats = phones.import_phonebook(self.engine, {"علي احمد مثال": {"01234567"}}, dry_run=True)
        self.assertEqual(stats["updates"], 1)
        self.assertNotIn("phone_number", {c["name"] for c in inspect(self.engine).get_columns("voters_2026")})
        self.assertEqual(self.rows(), before)

    def test_import_only_updates_unique_full_names_and_is_repeatable(self):
        self.add_voter(1, first="عَلِي")
        self.add_voter(2, first="حسن")
        self.add_voter(3, first="حسن", town="بلدة اخرى")
        self.add_voter(4, first="محمد", father=None)
        self.add_voter(5, first="مريم")
        book = {
            "علي احمد مثال": {"01987654", "01234567"},
            "حسن احمد مثال": {"01765432"},
            "محمد مثال": {"01654321"},
            "مثال احمد علي": {"01543210"},
        }
        stats = phones.import_phonebook(self.engine, book)
        self.assertEqual(stats, {"voters_scanned": 5, "matched_voters": 1, "ambiguous_names": 1, "unmatched_names": 2, "updates": 1})
        rows = self.rows()
        self.assertEqual(rows[0]["phone_number"], "01234567, 01987654")
        self.assertTrue(all(r["phone_number"] is None for r in rows[1:]))
        self.assertTrue(all(r["mother_name"] == "ام مثال" for r in rows))
        again = phones.import_phonebook(self.engine, book)
        self.assertEqual(again["updates"], 0)
        self.assertEqual(self.rows(), rows)

    def test_existing_phone_numbers_are_preserved(self):
        self.add_voter(1)
        phones.import_phonebook(self.engine, {"علي احمد مثال": {"03999999"}})
        phones.import_phonebook(self.engine, {"علي احمد مثال": {"01234567"}})
        self.assertEqual(self.rows()[0]["phone_number"], "01234567, 03999999")

    def test_failed_update_rolls_back_all_phone_changes(self):
        self.add_voter(1)
        self.add_voter(2, first="حسن")
        with self.engine.begin() as conn:
            conn.execute(text("ALTER TABLE voters_2026 ADD COLUMN phone_number TEXT"))
            conn.execute(text("""
                CREATE TRIGGER fail_second BEFORE UPDATE ON voters_2026
                WHEN NEW.id = 2 BEGIN SELECT RAISE(ABORT, 'test failure'); END
            """))
        with self.assertRaises(IntegrityError):
            phones.import_phonebook(self.engine, {
                "علي احمد مثال": {"01234567"}, "حسن احمد مثال": {"01765432"},
            })
        self.assertTrue(all(r["phone_number"] is None for r in self.rows()))


class VoterTemplateTests(unittest.TestCase):
    def render(self, voters):
        env = Environment(loader=FileSystemLoader(Path(__file__).resolve().parents[1] / "templates"), autoescape=True)
        return env.get_template("pen.html").render(
            voters=voters, area="بلدة", pen="قلم", year=2022, pen_total=0, lists=[],
            file_id="2022:North2", candidate_id="", slug="tripoli", total_voters=len(voters),
            q="", sect="", sects=[], page=1, pages=1,
        )

    def test_numbers_display_on_separate_lines_with_leading_zero(self):
        html = self.render([{"phone_number": "01234567, 01987654"}])
        self.assertIn("<th>Phone</th>", html)
        self.assertIn('<div class="text-nowrap">01234567</div>', html)
        self.assertIn('<div class="text-nowrap">01987654</div>', html)

    def test_missing_numbers_and_empty_table(self):
        self.assertIn('<span class="text-muted">—</span>', self.render([{}]))
        self.assertIn('colspan="9"', self.render([]))

    def test_phone_values_are_escaped(self):
        html = self.render([{"phone_number": "<script>alert(1)</script>"}])
        self.assertNotIn("<script>", html)
        self.assertIn("&lt;script&gt;", html)


if __name__ == "__main__":
    unittest.main()
