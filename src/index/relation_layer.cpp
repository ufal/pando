#include "index/relation_layer.h"

#include "index/dependency_index.h"
#include "index/positional_attr.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <utility>

namespace pando {

namespace {

// `4:nsubj|7:nsubj:xsubj` → (head, label) pairs; a head with a '.' (empty node) is skipped and counted
std::vector<std::pair<int, std::string>> parse_deps(std::string_view deps, size_t& skipped) {
    std::vector<std::pair<int, std::string>> out;
    if (deps.empty() || deps == "_") return out;
    size_t a = 0;
    while (a <= deps.size()) {
        size_t b = deps.find('|', a);
        if (b == std::string_view::npos) b = deps.size();
        const std::string_view edge = deps.substr(a, b - a);
        const size_t colon = edge.find(':');
        if (colon != std::string_view::npos && colon > 0) {
            const std::string_view head = edge.substr(0, colon);
            if (head.find('.') != std::string_view::npos) {
                ++skipped;
            } else {
                int h = 0;
                bool ok = true;
                for (char c : head) {
                    if (c < '0' || c > '9') { ok = false; break; }
                    h = h * 10 + (c - '0');
                }
                if (ok) out.emplace_back(h, std::string(edge.substr(colon + 1)));
            }
        }
        a = b + 1;
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

void put(FILE* f, const void* data, size_t bytes, const std::string& what) {
    if (bytes && std::fwrite(data, 1, bytes, f) != bytes) throw std::runtime_error("cannot write " + what);
}

}  // namespace

RelationLayerWriter::RelationLayerWriter(const std::string& dir, const std::string& name)
    : base_(dir + "/rel." + name) {
    src_ = std::fopen((base_ + ".src").c_str(), "wb");
    off_ = std::fopen((base_ + ".off").c_str(), "wb");
    tgt_ = std::fopen((base_ + ".tgt").c_str(), "wb");
    lab_ = std::fopen((base_ + ".lab").c_str(), "wb");
    rtgt_ = std::fopen((base_ + ".rtgt").c_str(), "wb");
    rsrc_ = std::fopen((base_ + ".rsrc").c_str(), "wb");
    rlab_ = std::fopen((base_ + ".rlab").c_str(), "wb");
    if (!src_ || !off_ || !tgt_ || !lab_ || !rtgt_ || !rsrc_ || !rlab_) throw std::runtime_error("cannot create " + base_ + ".*");
}

RelationLayerWriter::~RelationLayerWriter() {
    for (FILE* f : {src_, off_, tgt_, lab_, rtgt_, rsrc_, rlab_})
        if (f) std::fclose(f);
}

int32_t RelationLayerWriter::label_id(std::string_view label) {
    auto it = label_ids_.find(std::string(label));
    if (it != label_ids_.end()) return it->second;
    const auto id = static_cast<int32_t>(labels_.size());
    labels_.emplace_back(label);
    label_ids_.emplace(labels_.back(), id);
    return id;
}

void RelationLayerWriter::add_sentence(CorpusPos start, const std::vector<Token>& tokens) {
    const auto n = static_cast<int>(tokens.size());
    // the sentence's deviating edges by head, for the reverse files (heads stay inside the sentence, so the files
    // come out sorted by head over the whole corpus)
    struct Rev { int64_t target, source; int32_t label; };
    std::vector<Rev> rev;
    for (int i = 0; i < n; ++i) {
        const Token& t = tokens[static_cast<size_t>(i)];
        ++tokens_;
        auto edges = parse_deps(t.deps, skipped_empty_);
        // no DEPS given (`_`): the basic edge holds, nothing to store
        if (t.deps.empty() || t.deps == "_") continue;
        // edges to tokens outside the sentence cannot be stored: dropped as the basic tree drops them
        edges.erase(std::remove_if(edges.begin(), edges.end(), [&](const auto& e) { return e.first > n; }), edges.end());
        const bool same = edges.size() == 1 && edges[0].first == t.basic_head && edges[0].second == t.basic_label;
        if (same) continue;
        const CorpusPos pos = start + i;
        ++deviating_;
        put(src_, &pos, sizeof pos, base_ + ".src");
        put(off_, &written_edges_, sizeof written_edges_, base_ + ".off");
        for (const auto& [h, label] : edges) {
            const int64_t target = h == 0 ? int64_t{NO_HEAD} : static_cast<int64_t>(start + h - 1);
            const int32_t id = label_id(label);
            put(tgt_, &target, sizeof target, base_ + ".tgt");
            put(lab_, &id, sizeof id, base_ + ".lab");
            ++written_edges_;
            ++edges_;
            if (h != 0) rev.push_back({target, pos, id});
            if (h != 0) span_ = std::max<int64_t>(span_, target > pos ? target - pos : pos - target);
        }
    }
    std::sort(rev.begin(), rev.end(), [](const Rev& a, const Rev& b) {
        return a.target != b.target ? a.target < b.target : a.source != b.source ? a.source < b.source : a.label < b.label;
    });
    for (const Rev& r : rev) {
        put(rtgt_, &r.target, sizeof r.target, base_ + ".rtgt");
        put(rsrc_, &r.source, sizeof r.source, base_ + ".rsrc");
        put(rlab_, &r.label, sizeof r.label, base_ + ".rlab");
    }
}

void RelationLayerWriter::add_unit(std::vector<Edge> edges) {
    std::sort(edges.begin(), edges.end(), [](const Edge& a, const Edge& b) {
        return a.source != b.source ? a.source < b.source : a.target != b.target ? a.target < b.target : a.label < b.label;
    });
    edges.erase(std::unique(edges.begin(), edges.end(), [](const Edge& a, const Edge& b) {
        return a.source == b.source && a.target == b.target && a.label == b.label;
    }), edges.end());
    struct Rev { int64_t target, source; int32_t label; };
    std::vector<Rev> rev;
    for (size_t i = 0; i < edges.size();) {
        const CorpusPos pos = edges[i].source;
        ++tokens_;
        ++deviating_;
        put(src_, &pos, sizeof pos, base_ + ".src");
        put(off_, &written_edges_, sizeof written_edges_, base_ + ".off");
        for (; i < edges.size() && edges[i].source == pos; ++i) {
            const int64_t target = edges[i].target;
            const int32_t id = label_id(edges[i].label);
            put(tgt_, &target, sizeof target, base_ + ".tgt");
            put(lab_, &id, sizeof id, base_ + ".lab");
            ++written_edges_;
            ++edges_;
            rev.push_back({target, pos, id});
            span_ = std::max<int64_t>(span_, target > pos ? target - pos : pos - target);
        }
    }
    std::sort(rev.begin(), rev.end(), [](const Rev& a, const Rev& b) {
        return a.target != b.target ? a.target < b.target : a.source != b.source ? a.source < b.source : a.label < b.label;
    });
    for (const Rev& r : rev) {
        put(rtgt_, &r.target, sizeof r.target, base_ + ".rtgt");
        put(rsrc_, &r.source, sizeof r.source, base_ + ".rsrc");
        put(rlab_, &r.label, sizeof r.label, base_ + ".rlab");
    }
}

void RelationLayerWriter::finish() {
    if (finished_) return;
    finished_ = true;
    put(off_, &written_edges_, sizeof written_edges_, base_ + ".off");
    for (FILE** f : {&src_, &off_, &tgt_, &lab_, &rtgt_, &rsrc_, &rlab_}) {
        std::fclose(*f);
        *f = nullptr;
    }
    std::ofstream out(base_ + ".labels");
    for (const auto& l : labels_) out << l << "\n";
    if (!out) throw std::runtime_error("cannot write " + base_ + ".labels");
}

bool RelationLayer::open(const std::string& dir, const std::string& name, const DependencyIndex* base,
                         const PositionalAttr* base_label) {
    namespace fs = std::filesystem;
    valid_ = false;
    const std::string b = dir + "/rel." + name;
    std::error_code ec;
    for (const char* ext : {".src", ".off", ".tgt", ".lab", ".labels", ".rtgt", ".rsrc", ".rlab"})
        if (!fs::exists(b + ext, ec)) return false;
    nsrc_ = static_cast<size_t>(fs::file_size(b + ".src", ec)) / sizeof(int64_t);
    const size_t noff = static_cast<size_t>(fs::file_size(b + ".off", ec)) / sizeof(int64_t);
    const size_t nedge = static_cast<size_t>(fs::file_size(b + ".tgt", ec)) / sizeof(int64_t);
    if (noff != nsrc_ + 1 || static_cast<size_t>(fs::file_size(b + ".lab", ec)) / sizeof(int32_t) != nedge) return false;
    nedge_ = nedge;
    nrev_ = static_cast<size_t>(fs::file_size(b + ".rtgt", ec)) / sizeof(int64_t);
    if (static_cast<size_t>(fs::file_size(b + ".rsrc", ec)) / sizeof(int64_t) != nrev_
        || static_cast<size_t>(fs::file_size(b + ".rlab", ec)) / sizeof(int32_t) != nrev_ || nrev_ > nedge)
        return false;
    off_ = MmapFile::open(b + ".off");
    if (nsrc_ > 0) {
        src_ = MmapFile::open(b + ".src");
        tgt_ = MmapFile::open(b + ".tgt");
        lab_ = MmapFile::open(b + ".lab");
        if (!src_.valid() || !off_.valid() || !tgt_.valid() || !lab_.valid()) return false;
    }
    if (nrev_ > 0) {
        rtgt_ = MmapFile::open(b + ".rtgt");
        rsrc_ = MmapFile::open(b + ".rsrc");
        rlab_ = MmapFile::open(b + ".rlab");
        if (!rtgt_.valid() || !rsrc_.valid() || !rlab_.valid()) return false;
    }
    labels_.clear();
    std::ifstream in(b + ".labels");
    for (std::string line; std::getline(in, line);) labels_.push_back(line);
    if (nsrc_ > 0) {
        const auto* lab = lab_.as<int32_t>();
        for (size_t e = 0; e < nedge; ++e)
            if (lab[e] < 0 || static_cast<size_t>(lab[e]) >= labels_.size()) return false;
    }
    if (nrev_ > 0) {
        const auto* rlab = rlab_.as<int32_t>();
        for (size_t e = 0; e < nrev_; ++e)
            if (rlab[e] < 0 || static_cast<size_t>(rlab[e]) >= labels_.size()) return false;
    }
    base_ = base;
    base_label_ = base_label;
    valid_ = true;
    return true;
}

int64_t RelationLayer::find(CorpusPos pos) const {
    if (nsrc_ == 0) return -1;
    const auto* src = src_.as<int64_t>();
    const auto* it = std::lower_bound(src, src + nsrc_, static_cast<int64_t>(pos));
    return it != src + nsrc_ && *it == pos ? it - src : -1;
}

CorpusPos RelationLayer::base_head(CorpusPos pos) const {
    return base_ ? base_->head(pos) : NO_HEAD;
}

std::vector<CorpusPos> RelationLayer::base_children(CorpusPos pos) const {
    return base_ ? base_->children(pos) : std::vector<CorpusPos>{};
}

std::string_view RelationLayer::base_label(CorpusPos pos) const {
    return base_label_ ? base_label_->value_at(pos) : std::string_view();
}

}  // namespace pando
