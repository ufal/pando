#include "core/dates.h"

#include <cctype>
#include <unordered_map>

namespace pando {

namespace {

bool is_leap(int64_t y) { return (y % 4 == 0) && ((y % 100 != 0) || (y % 400 == 0)); }

bool digits(std::string_view s) {
    if (s.empty()) return false;
    for (char c : s)
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    return true;
}

int two(std::string_view s) { return (s[0] - '0') * 10 + (s[1] - '0'); }

int64_t floor_div(int64_t a, int64_t b) {
    int64_t q = a / b;
    if ((a % b != 0) && ((a < 0) != (b < 0))) --q;
    return q;
}

int64_t century_of(int64_t y) {
    if (y > 0) return (y - 1) / 100 + 1;
    return -((-y) / 100 + 1);   // 0 … -99 (1 … 100 BC): -1
}

int64_t year_of_day(int64_t days) {
    int64_t y; int m, d;
    civil_from_days(days, y, m, d);
    return y;
}

// One EDTF date (no interval): its days, precision and qualifiers.
struct Span {
    int64_t first = 0, last = 0;
    DatePrecision precision = DatePrecision::None;
    bool uncertain = false, approximate = false;
};

// hh:mm[:ss[.f]] with an optional Z or ±hh[:mm]: the time after a date (ignored: the date is the
// local date, as written).
bool edtf_time(std::string_view t) {
    auto num2 = [&](size_t i) { return i + 2 <= t.size() && digits(t.substr(i, 2)); };
    size_t i = 0;
    if (!num2(0) || t.size() < 5 || t[2] != ':' || !num2(3)) return false;
    i = 5;
    if (i < t.size() && t[i] == ':') {
        if (!num2(i + 1)) return false;
        i += 3;
        if (i < t.size() && t[i] == '.') {
            size_t j = i + 1;
            while (j < t.size() && std::isdigit(static_cast<unsigned char>(t[j]))) ++j;
            if (j == i + 1) return false;
            i = j;
        }
    }
    if (i == t.size()) return true;
    if (t[i] == 'Z') return i + 1 == t.size();
    if (t[i] != '+' && t[i] != '-') return false;
    std::string_view z = t.substr(i + 1);
    return (z.size() == 2 && digits(z)) || (z.size() == 4 && digits(z))
           || (z.size() == 5 && z[2] == ':' && digits(z.substr(0, 2)) && digits(z.substr(3)));
}

std::optional<Span> edtf_date(std::string_view s) {
    Span out;
    if (!s.empty() && (s.back() == '?' || s.back() == '~' || s.back() == '%')) {
        const char q = s.back();
        out.uncertain = q == '?' || q == '%';
        out.approximate = q == '~' || q == '%';
        s.remove_suffix(1);
    }
    if (s.empty()) return std::nullopt;
    // Y170000002, Y-170000002: years of more than four digits (year precision only)
    if (s[0] == 'Y') {
        std::string_view n = s.substr(1);
        bool neg = !n.empty() && n[0] == '-';
        if (neg) n.remove_prefix(1);
        if (n.size() < 5 || n.size() > 12 || !digits(n)) return std::nullopt;
        int64_t y = std::stoll(std::string(n));
        if (neg) y = -y;
        out.first = days_from_civil(y, 1, 1);
        out.last = days_from_civil(y, 12, 31);
        out.precision = DatePrecision::Year;
        return out;
    }
    bool neg = s[0] == '-';
    if (neg) s.remove_prefix(1);
    if (s.size() < 4) return std::nullopt;
    // the year: four digits, the last ones may be X (unspecified: 196X a decade, 19XX a century)
    std::string_view ys = s.substr(0, 4);
    size_t nx = 0;
    for (size_t k = 0; k < 4; ++k) {
        const char c = ys[k];
        if (c == 'X') { ++nx; continue; }
        if (!std::isdigit(static_cast<unsigned char>(c)) || nx) return std::nullopt;   // X only at the end
    }
    if (nx == 4) return std::nullopt;
    int64_t lo = 0, hi = 0;
    for (size_t k = 0; k < 4; ++k) {
        lo = lo * 10 + (ys[k] == 'X' ? 0 : ys[k] - '0');
        hi = hi * 10 + (ys[k] == 'X' ? 9 : ys[k] - '0');
    }
    if (neg) { const int64_t t = lo; lo = -hi; hi = -t; }
    std::string_view rest = s.substr(4);
    if (nx) {
        if (!rest.empty()) return std::nullopt;
        out.first = days_from_civil(lo, 1, 1);
        out.last = days_from_civil(hi, 12, 31);
        out.precision = nx == 1 ? DatePrecision::Decade : nx == 2 ? DatePrecision::Century : DatePrecision::Millennium;
        return out;
    }
    const int64_t y = lo;
    if (rest.empty()) {
        out.first = days_from_civil(y, 1, 1);
        out.last = days_from_civil(y, 12, 31);
        out.precision = DatePrecision::Year;
        return out;
    }
    if (rest.size() < 3 || rest[0] != '-') return std::nullopt;
    std::string_view ms = rest.substr(1, 2);
    rest = rest.substr(3);
    if (ms == "XX") {   // month unspecified: the year; a day must be unspecified too
        if (!rest.empty() && rest != "-XX") return std::nullopt;
        out.first = days_from_civil(y, 1, 1);
        out.last = days_from_civil(y, 12, 31);
        out.precision = DatePrecision::Year;
        return out;
    }
    if (!digits(ms)) return std::nullopt;
    const int m = two(ms);
    if (m >= 21 && m <= 24) {   // seasons (meteorological, northern hemisphere): spring, summer, autumn, winter
        if (!rest.empty()) return std::nullopt;
        static const int start[4] = {3, 6, 9, 12};
        const int sm = start[m - 21];
        out.first = days_from_civil(y, sm, 1);
        const int64_t ey = sm == 12 ? y + 1 : y;
        const int em = sm == 12 ? 2 : sm + 2;
        out.last = days_from_civil(ey, em, days_in_month(ey, em));
        out.precision = DatePrecision::Season;
        return out;
    }
    if (m < 1 || m > 12) return std::nullopt;
    if (rest.empty()) {
        out.first = days_from_civil(y, m, 1);
        out.last = days_from_civil(y, m, days_in_month(y, m));
        out.precision = DatePrecision::Month;
        return out;
    }
    if (rest.size() < 3 || rest[0] != '-') return std::nullopt;
    std::string_view ds = rest.substr(1, 2);
    rest = rest.substr(3);
    if (ds == "XX") {
        if (!rest.empty()) return std::nullopt;
        out.first = days_from_civil(y, m, 1);
        out.last = days_from_civil(y, m, days_in_month(y, m));
        out.precision = DatePrecision::Month;
        return out;
    }
    if (!digits(ds)) return std::nullopt;
    const int d = two(ds);
    if (d < 1 || d > days_in_month(y, m)) return std::nullopt;
    if (!rest.empty() && !(rest[0] == 'T' && edtf_time(rest.substr(1)))) return std::nullopt;
    out.first = out.last = days_from_civil(y, m, d);
    out.precision = DatePrecision::Day;
    return out;
}

std::optional<DateValue> parse_edtf(std::string_view s) {
    DateValue v;
    const size_t slash = s.find('/');
    if (slash == std::string_view::npos) {
        auto span = edtf_date(s);
        if (!span) return std::nullopt;
        v.source = DateSource::Edtf;
        v.first = span->first;
        v.last = span->last;
        v.precision = span->precision;
        v.uncertain = span->uncertain;
        v.approximate = span->approximate;
        return v;
    }
    std::string_view a = s.substr(0, slash), b = s.substr(slash + 1);
    if (b.find('/') != std::string_view::npos) return std::nullopt;
    const bool a_open = a.empty() || a == "..", b_open = b.empty() || b == "..";
    if (a_open && b_open) return std::nullopt;
    v.source = DateSource::Edtf;
    v.precision = DatePrecision::Range;
    v.open_first = a_open;
    v.open_last = b_open;
    if (!a_open) {
        auto sa = edtf_date(a);
        if (!sa) return std::nullopt;
        v.first = sa->first;
        v.uncertain |= sa->uncertain;
        v.approximate |= sa->approximate;
    }
    if (!b_open) {
        auto sb = edtf_date(b);
        if (!sb) return std::nullopt;
        v.last = sb->last;
        v.uncertain |= sb->uncertain;
        v.approximate |= sb->approximate;
    }
    if (!a_open && !b_open && v.first > v.last) return std::nullopt;
    return v;
}

// The old reading, exactly as before EDTF: optional whitespace, then a year of four digits not
// followed by a digit (with an optional sign), and from four unsigned digits on, -MM-DD or /MM/DD.
std::optional<int64_t> legacy_year(std::string_view text) {
    size_t i = 0;
    while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) ++i;
    if (i >= text.size()) return std::nullopt;
    bool neg = false;
    if (text[i] == '+' || text[i] == '-') { neg = text[i] == '-'; ++i; }
    if (i + 4 > text.size() || !digits(text.substr(i, 4))) return std::nullopt;
    if (i + 4 < text.size() && std::isdigit(static_cast<unsigned char>(text[i + 4]))) return std::nullopt;
    int64_t y = 0;
    for (size_t k = 0; k < 4; ++k) y = y * 10 + (text[i + k] - '0');
    return neg ? -y : y;
}

std::optional<DateValue> parse_legacy(std::string_view text) {
    DateValue v;
    v.source = DateSource::Legacy;
    size_t i = 0;
    while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) ++i;
    if (i + 4 <= text.size() && digits(text.substr(i, 4))) {
        int64_t y = 0;
        for (size_t k = 0; k < 4; ++k) y = y * 10 + (text[i + k] - '0');
        size_t j = i + 4;
        if (j + 3 <= text.size() && (text[j] == '-' || text[j] == '/') && digits(text.substr(j + 1, 2))) {
            const char sep = text[j];
            const int m = two(text.substr(j + 1, 2));
            if (m >= 1 && m <= 12) {
                j += 3;
                if (j + 3 <= text.size() && text[j] == sep && digits(text.substr(j + 1, 2))) {
                    const int d = two(text.substr(j + 1, 2));
                    if (d >= 1 && d <= days_in_month(y, m)) {
                        v.first = v.last = days_from_civil(y, m, d);
                        v.precision = DatePrecision::Day;
                        return v;
                    }
                } else if (!(j < text.size() && text[j] == sep)) {
                    v.first = days_from_civil(y, m, 1);
                    v.last = days_from_civil(y, m, days_in_month(y, m));
                    v.precision = DatePrecision::Month;
                    return v;
                }
            }
        }
    }
    auto y = legacy_year(text);
    if (!y) return std::nullopt;
    v.first = days_from_civil(*y, 1, 1);
    v.last = days_from_civil(*y, 12, 31);
    v.precision = DatePrecision::Year;
    return v;
}

}  // namespace

int days_in_month(int64_t y, int m) {
    static const int kDays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (m < 1 || m > 12) return 0;
    return (m == 2 && is_leap(y)) ? 29 : kDays[m - 1];
}

// H. Hinnant's days_from_civil / civil_from_days (valid for all years, astronomical numbering).
int64_t days_from_civil(int64_t y, int m, int d) {
    y -= m <= 2;
    const int64_t era = floor_div(y, 400);
    const int64_t yoe = y - era * 400;
    const int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

void civil_from_days(int64_t z, int64_t& y, int& m, int& d) {
    z += 719468;
    const int64_t era = floor_div(z, 146097);
    const int64_t doe = z - era * 146097;
    const int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const int64_t mp = (5 * doy + 2) / 153;
    d = static_cast<int>(doy - (153 * mp + 2) / 5 + 1);
    m = static_cast<int>(mp < 10 ? mp + 3 : mp - 9);
    y = yoe + era * 400 + (m <= 2);
}

int iso_week_number(int64_t y, int m, int d) {
    auto weekday = [](int64_t days) {   // Monday 1 … Sunday 7 (1970-01-01 was a Thursday)
        int64_t w = (days + 3) % 7;
        if (w < 0) w += 7;
        return static_cast<int>(w) + 1;
    };
    auto weeks_in = [&](int64_t yy) {
        const int jan1 = weekday(days_from_civil(yy, 1, 1));
        return (jan1 == 4 || (jan1 == 3 && is_leap(yy))) ? 53 : 52;
    };
    const int64_t days = days_from_civil(y, m, d);
    const int doy = static_cast<int>(days - days_from_civil(y, 1, 1)) + 1;
    const int week = (doy - weekday(days) + 10) / 7;
    if (week < 1) return weeks_in(y - 1);
    if (week > weeks_in(y)) return 1;
    return week;
}

DateValue parse_date(std::string_view text) {
    if (auto e = parse_edtf(text)) return *e;
    if (auto l = parse_legacy(text)) return *l;
    return DateValue{};
}

const DateValue& parse_date_cached(std::string_view text) {
    thread_local std::unordered_map<std::string, DateValue> cache;
    auto it = cache.find(std::string(text));
    if (it != cache.end()) return it->second;
    if (cache.size() >= 1 << 16) cache.clear();
    return cache.emplace(std::string(text), parse_date(text)).first->second;
}

std::optional<DatePart> date_part_of_function(std::string_view name) {
    if (name == "year") return DatePart::Year;
    if (name == "century") return DatePart::Century;
    if (name == "decade") return DatePart::Decade;
    if (name == "month") return DatePart::Month;
    if (name == "week") return DatePart::Week;
    if (name == "day") return DatePart::Day;
    if (name == "earliest") return DatePart::Earliest;
    if (name == "latest") return DatePart::Latest;
    if (name == "precision") return DatePart::Precision;
    return std::nullopt;
}

const char* date_part_function_name(DatePart part) {
    switch (part) {
        case DatePart::Year: return "year";
        case DatePart::Century: return "century";
        case DatePart::Decade: return "decade";
        case DatePart::Month: return "month";
        case DatePart::Week: return "week";
        case DatePart::Day: return "day";
        case DatePart::Earliest: return "earliest";
        case DatePart::Latest: return "latest";
        case DatePart::Precision: return "precision";
    }
    return "";
}

const char* date_precision_name(DatePrecision p) {
    switch (p) {
        case DatePrecision::Day: return "day";
        case DatePrecision::Month: return "month";
        case DatePrecision::Season: return "season";
        case DatePrecision::Year: return "year";
        case DatePrecision::Decade: return "decade";
        case DatePrecision::Century: return "century";
        case DatePrecision::Millennium: return "millennium";
        case DatePrecision::Range: return "range";
        case DatePrecision::None: break;
    }
    return "";
}

std::optional<int64_t> date_part_value(const DateValue& v, DatePart part) {
    if (!v.ok()) return std::nullopt;
    if (part == DatePart::Earliest) {
        if (v.open_first) return std::nullopt;
        return year_of_day(v.first);
    }
    if (part == DatePart::Latest) {
        if (v.open_last) return std::nullopt;
        return year_of_day(v.last);
    }
    if (part == DatePart::Precision || v.open_first || v.open_last) return std::nullopt;
    int64_t y1, y2; int m1, m2, d1, d2;
    civil_from_days(v.first, y1, m1, d1);
    civil_from_days(v.last, y2, m2, d2);
    switch (part) {
        case DatePart::Year:
            if (y1 != y2) return std::nullopt;
            return y1;
        case DatePart::Century:
            if (century_of(y1) != century_of(y2)) return std::nullopt;
            return century_of(y1);
        case DatePart::Decade:
            if (floor_div(y1, 10) != floor_div(y2, 10)) return std::nullopt;
            return floor_div(y1, 10) * 10;
        case DatePart::Month:
            if (y1 != y2 || m1 != m2) return std::nullopt;
            return m1;
        case DatePart::Day:
            if (v.first != v.last) return std::nullopt;
            return d1;
        case DatePart::Week:
            if (v.first != v.last) return std::nullopt;
            return iso_week_number(y1, m1, d1);
        default:
            break;
    }
    return std::nullopt;
}

std::optional<std::string> date_part_label(std::string_view text, DatePart part) {
    const DateValue& v = parse_date_cached(text);
    if (part == DatePart::Precision) {
        if (!v.ok()) return std::nullopt;
        return std::string(date_precision_name(v.precision));
    }
    auto n = date_part_value(v, part);
    if (!n) return std::nullopt;
    return std::to_string(*n);
}

}  // namespace pando
