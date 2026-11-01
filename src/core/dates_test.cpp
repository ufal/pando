// core/dates: EDTF (levels 0 and 1, intervals), the old reading as before, and what each date function
// gives (only what the value determines).
#include "core/dates.h"

#include <cstdio>
#include <string>

using namespace pando;

static int fails = 0, n = 0;

static void eq(const std::string& what, const std::string& got, const std::string& want) {
    ++n;
    if (got != want) {
        ++fails;
        std::printf("FAIL %s: got '%s', want '%s'\n", what.c_str(), got.c_str(), want.c_str());
    }
}

// "source precision | year century decade month week day | earliest latest | flags"
static std::string describe(const std::string& text) {
    const DateValue v = parse_date(text);
    if (!v.ok()) return "none";
    std::string s = v.source == DateSource::Edtf ? "edtf" : "legacy";
    s += std::string(" ") + date_precision_name(v.precision) + " |";
    for (DatePart p : {DatePart::Year, DatePart::Century, DatePart::Decade, DatePart::Month, DatePart::Week,
                       DatePart::Day}) {
        auto x = date_part_value(v, p);
        s += " " + (x ? std::to_string(*x) : std::string("-"));
    }
    s += " |";
    for (DatePart p : {DatePart::Earliest, DatePart::Latest}) {
        auto x = date_part_value(v, p);
        s += " " + (x ? std::to_string(*x) : std::string("-"));
    }
    if (v.uncertain) s += " ?";
    if (v.approximate) s += " ~";
    return s;
}

int main() {
    // calendar arithmetic
    eq("1970-01-01", std::to_string(days_from_civil(1970, 1, 1)), "0");
    eq("2000-03-01", std::to_string(days_from_civil(2000, 3, 1)), "11017");
    for (int64_t d : {-800000LL, -1LL, 0LL, 59LL, 11016LL, 2932896LL}) {
        int64_t y; int m, dd;
        civil_from_days(d, y, m, dd);
        eq("round trip " + std::to_string(d), std::to_string(days_from_civil(y, m, dd)), std::to_string(d));
    }
    eq("ISO week 2021-01-03", std::to_string(iso_week_number(2021, 1, 3)), "53");
    eq("ISO week 1945-12-02", std::to_string(iso_week_number(1945, 12, 2)), "48");

    // EDTF level 0
    eq("1945", describe("1945"), "edtf year | 1945 20 1940 - - - | 1945 1945");
    eq("1945-12", describe("1945-12"), "edtf month | 1945 20 1940 12 - - | 1945 1945");
    eq("1945-12-02", describe("1945-12-02"), "edtf day | 1945 20 1940 12 48 2 | 1945 1945");
    eq("timestamp", describe("1945-12-02T13:02:45Z"), "edtf day | 1945 20 1940 12 48 2 | 1945 1945");
    eq("offset", describe("1945-12-02T23:30:00-05:00"), "edtf day | 1945 20 1940 12 48 2 | 1945 1945");
    eq("0945", describe("0945"), "edtf year | 945 10 940 - - - | 945 945");
    eq("interval", describe("1712-11-23/1734"), "edtf interval | - 18 - - - - | 1712 1734");
    eq("interval in one year", describe("1712-03/1712-05"), "edtf interval | 1712 18 1710 - - - | 1712 1712");
    // level 1
    eq("uncertain", describe("1945?"), "edtf year | 1945 20 1940 - - - | 1945 1945 ?");
    eq("approximate", describe("1945~"), "edtf year | 1945 20 1940 - - - | 1945 1945 ~");
    eq("both", describe("1945-12%"), "edtf month | 1945 20 1940 12 - - | 1945 1945 ? ~");
    eq("a decade", describe("196X"), "edtf decade | - 20 1960 - - - | 1960 1969");
    eq("19XX: two centuries (ordinal)", describe("19XX"), "edtf century | - - - - - - | 1900 1999");
    eq("16XX", describe("16XX"), "edtf century | - - - - - - | 1600 1699");
    eq("month unspecified", describe("1985-XX"), "edtf year | 1985 20 1980 - - - | 1985 1985");
    eq("day unspecified", describe("1985-04-XX"), "edtf month | 1985 20 1980 4 - - | 1985 1985");
    eq("season", describe("1985-21"), "edtf season | 1985 20 1980 - - - | 1985 1985");
    eq("winter: into the next year", describe("1985-24"), "edtf season | - 20 1980 - - - | 1985 1986");
    eq("negative year", describe("-0044"), "edtf year | -44 -1 -50 - - - | -44 -44");
    eq("long year", describe("Y170000"), "edtf year | 170000 1700 170000 - - - | 170000 170000");
    eq("open end", describe("1712/.."), "edtf interval | - - - - - - | 1712 -");
    eq("open start", describe("../1734"), "edtf interval | - - - - - - | - 1734");
    eq("unknown start", describe("/1734"), "edtf interval | - - - - - - | - 1734");
    eq("uncertain end", describe("1712/1734?"), "edtf interval | - 18 - - - - | 1712 1734 ?");

    // not EDTF: the old reading, as before
    eq("slashes", describe("1945/12/02"), "legacy day | 1945 20 1940 12 48 2 | 1945 1945");
    eq("space", describe(" 1945"), "legacy year | 1945 20 1940 - - - | 1945 1945");
    eq("year-year", describe("1712-1734"), "legacy year | 1712 18 1710 - - - | 1712 1712");
    eq("1550s", describe("1550s"), "legacy year | 1550 16 1550 - - - | 1550 1550");
    eq("partial", describe("1945-2-3"), "legacy year | 1945 20 1940 - - - | 1945 1945");
    eq("day out of range", describe("1945-02-30"), "legacy year | 1945 20 1940 - - - | 1945 1945");
    eq("space before a time", describe("1945-12-02 13:00"), "legacy day | 1945 20 1940 12 48 2 | 1945 1945");
    eq("signed", describe("+1945"), "legacy year | 1945 20 1940 - - - | 1945 1945");
    eq("trailing slash", describe("1945/12/02/"), "legacy day | 1945 20 1940 12 48 2 | 1945 1945");
    eq("a reversed interval is not EDTF", describe("1734/1712"), "legacy year | 1734 18 1730 - - - | 1734 1734");

    // no date: nothing is guessed
    for (const char* t : {"", "02-12-1945", "12/02/1945", "2.12.1945", "[1945]", "ca. 1550", "XVI", "945", "XXXX",
                          "19451", "../..", "El dia 2 de desembre de 1945"})
        eq(std::string("none: '") + t + "'", describe(t), "none");

    // labels for grouping and tables
    eq("label year", date_part_label("1945-12-02", DatePart::Year).value_or("-"), "1945");
    eq("label precision", date_part_label("196X", DatePart::Precision).value_or("-"), "decade");
    eq("label precision interval", date_part_label("1712/1734", DatePart::Precision).value_or("-"), "interval");
    eq("label precision legacy", date_part_label("1945/12/02", DatePart::Precision).value_or("-"), "day");
    eq("label none", date_part_label("XVI", DatePart::Year).value_or("-"), "-");
    eq("cached twice", date_part_label("1712/1734", DatePart::Century).value_or("-")
                       + date_part_label("1712/1734", DatePart::Century).value_or("-"), "1818");
    eq("function names", std::string(date_part_function_name(*date_part_of_function("earliest"))), "earliest");
    eq("not a date function", date_part_of_function("strlen") ? "yes" : "no", "no");

    std::printf(fails ? "%d of %d failed\n" : "all %d as expected\n", fails ? fails : n, n);
    return fails ? 1 : 0;
}
