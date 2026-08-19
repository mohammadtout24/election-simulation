// Shared Arabic/English toggle for every data-i18n / data-i18n-placeholder
// tagged element across index.html, detail.html, and compare.html.
(function () {
    const STORAGE_KEY = "site_lang";

    const translations = {
        en: {
            elections: "Elections",
            search_district: "Search district...",
            no_district: "No matching district found.",
            compare_desc: "Compare list votes, seats, and winner changes.",
            open2018: "Open 2018",
            open2022: "Open 2022",
            map: "← Map",
            total_votes: "Total votes",
            winners: "Winners",
            lists: "Lists",
            candidates: "Candidates",
            votes_by_list: "Votes by list",
            list_comparison: "List comparison table",
            search_list: "Search list...",
            compare_years: "Compare 2018/2022",
            scenario_compare: "Scenario Comparison",
            back: "← Back",
            recalculate: "Recalculate Results",
            export_excel: "Export Excel",
            export_pdf: "Export PDF",
            reset: "Reset to Default",
            candidate_search: "Search candidate, list, religion, or district...",
            showing_all: "Showing all candidates",
            live_standings: "Live List Standings",
            what_if_title: "💡 What If Smart Advisor",
            what_if_desc: "Pick a list to see what it needs to win one more seat, and the effect of a certain percentage increase in votes.",
            choose_list: "-- Choose a list --",
            run_advisor: "Run Advisor",
            scenario_subtitle: "Original database result vs current simulated result",
        },
        ar: {
            elections: "الانتخابات",
            search_district: "ابحث عن دائرة...",
            no_district: "لا توجد دائرة مطابقة.",
            compare_desc: "قارن أصوات اللوائح والمقاعد وتغيرات الفائزين.",
            open2018: "فتح 2018",
            open2022: "فتح 2022",
            map: "← الخريطة",
            total_votes: "مجموع الأصوات",
            winners: "الفائزون",
            lists: "اللوائح",
            candidates: "المرشحون",
            votes_by_list: "الأصوات حسب اللائحة",
            list_comparison: "جدول مقارنة اللوائح",
            search_list: "ابحث عن لائحة...",
            compare_years: "مقارنة 2018/2022",
            scenario_compare: "مقارنة السيناريو",
            back: "← رجوع",
            recalculate: "إعادة احتساب النتائج",
            export_excel: "تصدير Excel",
            export_pdf: "تصدير PDF",
            reset: "إعادة التعيين للوضع الافتراضي",
            candidate_search: "ابحث عن مرشح أو لائحة أو طائفة أو دائرة...",
            showing_all: "عرض كل المرشحين",
            live_standings: "ترتيب اللوائح الحالي",
            what_if_title: "💡 المستشار الذكي (ماذا لو)",
            what_if_desc: "اختر لائحة ليحلل النظام ماذا تحتاج لتربح مقعداً إضافياً، وما تأثير زيادة الأصوات بنسبة معينة.",
            choose_list: "-- اختر اللائحة --",
            run_advisor: "تشغيل المستشار",
            scenario_subtitle: "نتيجة قاعدة البيانات الأصلية مقابل النتيجة المحاكاة الحالية",
        },
    };

    function applyLanguage(lang) {
        const dict = translations[lang] || translations.en;

        document.querySelectorAll("[data-i18n]").forEach((el) => {
            const key = el.getAttribute("data-i18n");
            if (dict[key] !== undefined) el.textContent = dict[key];
        });
        document.querySelectorAll("[data-i18n-placeholder]").forEach((el) => {
            const key = el.getAttribute("data-i18n-placeholder");
            if (dict[key] !== undefined) el.placeholder = dict[key];
        });

        document.documentElement.setAttribute("lang", lang);
        localStorage.setItem(STORAGE_KEY, lang);
        const btn = document.getElementById("langToggleBtn");
        if (btn) btn.textContent = lang === "en" ? "العربية" : "English";
    }

    function toggleLanguage() {
        const current = localStorage.getItem(STORAGE_KEY) || "en";
        applyLanguage(current === "en" ? "ar" : "en");
    }
    window.toggleLanguage = toggleLanguage;

    document.addEventListener("DOMContentLoaded", function () {
        applyLanguage(localStorage.getItem(STORAGE_KEY) || "en");
    });
})();
