#pragma once

// Versioned index directories: build or upgrade a corpus next to the version
// being served, then switch to it in one step, so a server can hot-swap the
// corpus (open the new version, close the old one when its last request is
// done) and never sees half-written files.
//
//   ROOT/
//     current -> versions/<id>      relative symlink, replaced atomically
//     versions/<id>/                a complete index (corpus.info index_id=<id>)
//     versions/.building-<id>/      in progress; renamed to versions/<id> when done
//     .publish.lock                 one publish per ROOT at a time (flock)
//
// `pando-index … --publish ROOT` builds into versions/.building-<id>, and
// `pando-index --upgrade ROOT --publish` copies the current version there and
// upgrades the copy. On success the directory is renamed to versions/<id>,
// `current` is pointed at it, and versions beyond the newest `keep` (default
// 3) are removed — never the current one or the one it replaced, which a
// server may still have open. The symlink is relative, so ROOT can be mounted
// at a different path on each machine.
//
// Every index carries `index_id=` in corpus.info (a new one for every build and
// every --upgrade): hosts compare it to see that a corpus changed. Corpus::open
// resolves symlinks first, so a corpus opened through ROOT/current keeps
// reading the version it opened even after `current` moves on.
//
// Why not in place: index files are memory-mapped and some are opened only
// when a query first needs them; rewriting them under a running server mixes
// versions or, for a truncated file, crashes it (SIGBUS).

#include <string>
#include <vector>

namespace pando {

struct IndexPublish {
    /// `YYYYMMDDTHHMMSSmmmZ-xxxxxxxx` (UTC time to the ms + random): sorts by time, unique.
    static std::string new_index_id();
    /// Set / replace `index_id=` in DIR/corpus.info (atomic rename).
    static bool set_index_id(const std::string& dir, const std::string& id, std::string* err = nullptr);
    /// `index_id=` of DIR/corpus.info, "" when absent.
    static std::string read_index_id(const std::string& dir);

    /// ROOT has a `current` symlink or a `versions/` directory.
    static bool is_root(const std::string& path);
    /// The published root DIR belongs to (DIR is ROOT/versions/<id> or ROOT/current), else "".
    static std::string root_of(const std::string& dir);
    /// The directory ROOT/current points at (canonical), "" when none.
    static std::string current_version(const std::string& root);

    /// One publish of ROOT: takes the lock and makes versions/.building-<id>.
    class Session {
    public:
        explicit Session(const std::string& root);
        ~Session();   // unlocks; removes the building directory unless committed
        Session(const Session&) = delete;
        Session& operator=(const Session&) = delete;

        bool ok() const { return ok_; }
        const std::string& error() const { return err_; }
        const std::string& id() const { return id_; }
        const std::string& building_dir() const { return building_; }

        /// Copy the current version into the building directory (for --upgrade).
        bool copy_current();
        /// Stamp index_id, rename to versions/<id>, point `current` at it, prune
        /// to `keep` versions. `removed` gets the versions deleted.
        bool commit(int keep, std::vector<std::string>* removed = nullptr);

    private:
        std::string root_, id_, building_, err_;
        int lock_fd_ = -1;
        bool ok_ = false, committed_ = false;
    };
};

}  // namespace pando
