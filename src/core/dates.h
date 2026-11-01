#pragma once
// Dates in attribute values (a text's date, `text_date`), read the same way by every query function
// that takes one: year(), century(), decade(), month(), week(), day(), earliest(), latest() and
// precision() (PANDO-CQL-Reference.md, "Dates").
//
// The canonical form is EDTF (ISO 8601-2, levels 0 and 1): 1945, 1945-12, 1945-12-02, a time after
// the date, 1945?, 1945~, 1945%, 19XX, 196X, 1985-04-XX, seasons (1985-21), Y170000, -0044, and
// intervals 1712/1734, ../1734, 1712/... Each is an interval of days with a precision. Values that
// are not EDTF are read the old way (a leading four-digit year, YYYY-MM-DD or YYYY/MM/DD), marked
// as such, so existing corpora keep working while it shows which values to convert. Nothing is
// guessed: other formats, calendars and prose are converted (and declared) before indexing, by
// flexiconv. A value that is neither gives no date.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace pando {

enum class DateSource { None, Edtf, Legacy };   // Legacy: not EDTF, read the old way

/// How precise the value is as written. Range: an interval (A/B).
enum class DatePrecision { None, Day, Month, Season, Year, Decade, Century, Millennium, Range };

struct DateValue {
    DateSource source = DateSource::None;
    DatePrecision precision = DatePrecision::None;
    /// The first and last day, inclusive: days since 1970-01-01, proleptic Gregorian calendar,
    /// astronomical years (year 0 is 1 BC, -44 is 45 BC). Not set at an open or unknown end.
    int64_t first = 0, last = 0;
    bool open_first = false, open_last = false;   // "../1734", "1712/..", "/1734"
    bool uncertain = false, approximate = false;  // ?, ~ (% is both)
    bool ok() const { return source != DateSource::None; }
};

/// EDTF; else the old reading (source Legacy); else no date.
DateValue parse_date(std::string_view text);
/// The same, each distinct text parsed once per thread.
const DateValue& parse_date_cached(std::string_view text);

/// What a date function asks of a date.
enum class DatePart { Year, Century, Decade, Month, Week, Day, Earliest, Latest, Precision };

/// The function of that name ("year" → Year, …); nullopt for other names.
std::optional<DatePart> date_part_of_function(std::string_view name);
const char* date_part_function_name(DatePart part);

/// The part when the date determines it: year() when the whole interval lies in one year, month()
/// in one month, week() and day() on one day; century() and decade() likewise. earliest() / latest():
/// the year of the first / last day, when that end is known. nullopt otherwise, and for Precision.
/// century() counts ordinally (1501–1600 is the 16th); before year 1 it is negative (-1: the 1st
/// century BC). decade() is the year rounded down to a multiple of ten (-44 → -50).
std::optional<int64_t> date_part_value(const DateValue& date, DatePart part);

/// The part as a grouping or table value: the number, or for Precision the precision's name
/// ("day", "month", "season", "year", "decade", "century", "millennium", "range"); nullopt when
/// the value has none.
std::optional<std::string> date_part_label(std::string_view text, DatePart part);

const char* date_precision_name(DatePrecision p);

/// Calendar arithmetic (proleptic Gregorian, astronomical years).
int64_t days_from_civil(int64_t y, int m, int d);
void civil_from_days(int64_t days, int64_t& y, int& m, int& d);
int days_in_month(int64_t y, int m);
/// The ISO 8601 week number (1–53) of a day.
int iso_week_number(int64_t y, int m, int d);

}  // namespace pando
