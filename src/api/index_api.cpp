#include "api/index_api.h"

#include "index/index_upgrade.h"

#include <stdexcept>

namespace pando {

PandoIndexBuilder::PandoIndexBuilder(const std::string& output_dir)
    : output_dir_(output_dir), builder_(output_dir) {}

void PandoIndexBuilder::add_token(
        const std::unordered_map<std::string, std::string>& attrs,
        int sentence_head_id) {
    builder_.add_token(attrs, sentence_head_id);
}

void PandoIndexBuilder::end_sentence() {
    builder_.end_sentence();
}

void PandoIndexBuilder::set_token_deps(const std::string& deps) {
    builder_.set_token_deps(deps);
}

void PandoIndexBuilder::add_region(const std::string& type,
                                   CorpusPos start,
                                   CorpusPos end,
                                   const std::vector<std::pair<std::string, std::string>>& attrs) {
    builder_.add_region(type, start, end, attrs);
}

void PandoIndexBuilder::set_default_within(const std::string& structure) {
    builder_.set_default_within(structure);
}

void PandoIndexBuilder::declare_nested(const std::string& structure) {
    builder_.declare_nested(structure);
}

void PandoIndexBuilder::declare_overlapping(const std::string& structure) {
    builder_.declare_overlapping(structure);
}

void PandoIndexBuilder::declare_zerowidth(const std::string& structure) {
    builder_.declare_zerowidth(structure);
}

void PandoIndexBuilder::declare_multivalue(const std::string& name) {
    builder_.declare_multivalue(name);
}

void PandoIndexBuilder::declare_relation(const std::string& name, const std::string& spec) {
    builder_.declare_relation(name, spec);
}

void PandoIndexBuilder::add_relation(const std::string& layer, CorpusPos source, CorpusPos target,
                                     const std::string& label) {
    builder_.add_relation_edge(layer, source, target, label);
}

void PandoIndexBuilder::finalize() {
    builder_.finalize();
    if (upgrade_index(output_dir_, true) != 0 || measure_declared_equivalences(output_dir_, true) != 0)
        throw std::runtime_error("pando: the derived index files of " + output_dir_ + " could not be written");
}

CorpusPos PandoIndexBuilder::corpus_size() const {
    return builder_.corpus_size();
}

} // namespace pando

