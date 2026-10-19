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
        self.assertEqual(phones.normalize_name("م\u200dح\u0610مد"), "محمد")
        for first, second in (("أحمد", "احمد"), ("إبراهيم", "ابراهيم"),
                              ("آدم", "ادم"), ("على", "علي")):
            with self.subTest(first=first, second=second):
                self.assertEqual(phones.normalize_name(first), phones.normalize_name(second))

    def test_fallback_key_handles_spelling_and_compound_name_spacing(self):
        for first, second in (("فاطمة", "فاطمه"), ("عبد الله", "عبدالله"),
                              ("عبد الرحمن", "عبدالرحمن"), ("أبو شرف", "ابوشرف"),
                              ("رؤوف", "رووف"), ("وائل", "وايل")):
            with self.subTest(first=first, second=second):
                self.assertEqual(phones.variant_name_key(first), phones.variant_name_key(second))
        self.assertNotEqual(phones.variant_name_key("محمد احمد مثال"), phones.variant_name_key("محمود احمد مثال"))
        self.assertNotEqual(phones.variant_name_key("علي احمد مثال"), phones.variant_name_key("علي مثال احمد"))

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
        self.add_voter(1, first="فاطمة", father="عبدالله")
        before = self.rows()
        stats = phones.import_phonebook(self.engine, {"فاطمه عبد الله مثال": {"01234567"}}, dry_run=True)
        self.assertEqual(stats["updates"], 1)
        self.assertEqual(stats["variant_name_matches"], 1)
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
        self.assertEqual(stats, {"voters_scanned": 5, "matched_voters": 1, "exact_name_matches": 1,
                                 "variant_name_matches": 0, "ambiguous_names": 1, "unmatched_names": 2, "updates": 1})
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

    def test_spelling_and_spacing_variants_match_one_voter(self):
        self.add_voter(1, first="فاطمة", father="عبدالله", family="ابو شرف")
        stats = phones.import_phonebook(self.engine, {"فاطمه عبد الله ابوشرف": {"01234567"}})
        self.assertEqual(stats["variant_name_matches"], 1)
        self.assertEqual(stats["matched_voters"], 1)
        self.assertEqual(self.rows()[0]["phone_number"], "01234567")

    def test_variant_collision_in_another_town_is_skipped(self):
        self.add_voter(1, first="فاطمة", father="عبدالله")
        self.add_voter(2, first="فاطمه", father="عبدالله", town="بلدة اخرى")
        stats = phones.import_phonebook(self.engine, {"فاطمه عبد الله مثال": {"01234567"}})
        self.assertEqual(stats["ambiguous_names"], 1)
        self.assertEqual(stats["updates"], 0)
        self.assertTrue(all(r["phone_number"] is None for r in self.rows()))

    def test_exact_match_has_priority_over_another_variant(self):
        self.add_voter(1, first="فاطمة")
        self.add_voter(2, first="فاطمه")
        stats = phones.import_phonebook(self.engine, {"فاطمة احمد مثال": {"01234567"}})
        self.assertEqual(stats["exact_name_matches"], 1)
        self.assertEqual(stats["variant_name_matches"], 0)
        self.assertEqual(self.rows()[0]["phone_number"], "01234567")
        self.assertIsNone(self.rows()[1]["phone_number"])

    def test_contact_aliases_merge_into_one_voter_without_duplicates(self):
        self.add_voter(1, first="فاطمة")
        book = {"فاطمة احمد مثال": {"01234567"}, "فاطمه احمد مثال": {"01234567", "01987654"}}
        stats = phones.import_phonebook(self.engine, book)
        self.assertEqual(stats["matched_voters"], 1)
        self.assertEqual(stats["exact_name_matches"], 1)
        self.assertEqual(stats["variant_name_matches"], 1)
        self.assertEqual(self.rows()[0]["phone_number"], "01234567, 01987654")
        self.assertEqual(phones.import_phonebook(self.engine, book)["updates"], 0)

    def test_different_letters_and_reordered_names_stay_unmatched(self):
        self.add_voter(1, first="محمد")
        stats = phones.import_phonebook(self.engine, {
            "محمود احمد مثال": {"01234567"}, "محمد مثال احمد": {"01987654"},
        })
        self.assertEqual(stats["unmatched_names"], 2)
        self.assertIsNone(self.rows()[0]["phone_number"])

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
