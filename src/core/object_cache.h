#pragma once

// Values derived from an index object (an attribute) once, kept with that object, so they live and die with the
// index that was opened (a key per kind of value). Copies share the cache.

#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace pando {

class ObjectCache {
public:
    // the value under `key`, built by `build()` (a std::shared_ptr<const T>, possibly null) the first time
    template <typename T, typename Build>
    std::shared_ptr<const T> get(const std::string& key, Build&& build) const {
        std::lock_guard<std::mutex> lock(state_->mu);
        auto it = state_->values.find(key);
        if (it == state_->values.end())
            it = state_->values.emplace(key, std::shared_ptr<const void>(build())).first;
        return std::static_pointer_cast<const T>(it->second);
    }

private:
    struct State {
        std::mutex mu;
        std::map<std::string, std::shared_ptr<const void>> values;
    };
    std::shared_ptr<State> state_ = std::make_shared<State>();
};

}  // namespace pando
