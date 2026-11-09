#include "corpus/vertical_regions.h"

#include <iostream>

namespace pando {

namespace {
constexpr size_t kMaxListed = 10;   // unclosed regions named one by one; the rest counted
}

void VerticalRegions::emit(const std::string& type, CorpusPos start, CorpusPos end,
                           const std::vector<std::pair<std::string, std::string>>& attrs) {
    // end < start: zero-width (end = start - 1), as JSONL v2's start_pos > end_pos
    if (attrs.empty()) builder_.add_region(type, start, end);
    else builder_.add_region(type, start, end, attrs);
}

void VerticalRegions::note_same_type_open(const std::string& type, CorpusPos pos, size_t line) {
    for (auto& o : stack_)
        if (o.type == type && o.cut < 0) {
            o.cut = pos;
            o.cut_line = line;
        }
}

void VerticalRegions::open(VerticalTag tag, CorpusPos pos, size_t line) {
    note_same_type_open(tag.name, pos, line);
    Open o;
    o.type = std::move(tag.name);
    o.start = pos;
    o.attrs = std::move(tag.attrs);
    o.line = line;
    stack_.push_back(std::move(o));
}

void VerticalRegions::empty(VerticalTag tag, CorpusPos pos, size_t line) {
    note_same_type_open(tag.name, pos, line);
    builder_.declare_zerowidth(tag.name);   // the vertical says so: zerowidth= without a warning
    emit(tag.name, pos, pos - 1, tag.attrs);
}

void VerticalRegions::close(const std::string& type, CorpusPos pos, size_t line) {
    for (size_t i = stack_.size(); i-- > 0;) {
        if (stack_[i].type != type) continue;
        for (size_t j = i + 1; j < stack_.size(); ++j)
            if (stack_[j].outlived_line == 0) {
                stack_[j].outlived = type;
                stack_[j].outlived_line = line;
            }
        emit(type, stack_[i].start, pos - 1, stack_[i].attrs);
        stack_.erase(stack_.begin() + static_cast<std::ptrdiff_t>(i));
        return;
    }
    if (stray_closes_++ == 0) {
        first_stray_line_ = line;
        first_stray_type_ = type;
    }
}

void VerticalRegions::finish(CorpusPos corpus_size) {
    if (!stack_.empty() || stray_closes_ > 0) std::cerr << "\n";   // pando-index's progress line is open
    size_t listed = 0;
    for (const auto& o : stack_) {
        const CorpusPos end = o.cut >= 0 ? o.cut - 1 : corpus_size - 1;
        emit(o.type, o.start, end, o.attrs);
        if (listed++ >= kMaxListed) continue;
        std::cerr << "Warning: " << source_ << ":" << o.line << ": <" << o.type << "> is never closed";
        if (o.outlived_line > 0)
            std::cerr << " (its enclosing <" << o.outlived << "> ends at line " << o.outlived_line << ")";
        if (o.cut >= 0)
            std::cerr << "; ended before the next <" << o.type << "> (line " << o.cut_line << ")";
        else
            std::cerr << "; ended at the end of the corpus";
        std::cerr << ", as Manatee's encodevert does\n";
    }
    if (listed > kMaxListed)
        std::cerr << "Warning: " << source_ << ": " << listed << " regions never closed in all\n";
    stack_.clear();
    if (stray_closes_ > 0)
        std::cerr << "Warning: " << source_ << ":" << first_stray_line_ << ": </" << first_stray_type_
                  << "> closes nothing (ignored)"
                  << (stray_closes_ > 1 ? "; " + std::to_string(stray_closes_) + " such closing tags in all" : "")
                  << "\n";
}

} // namespace pando
