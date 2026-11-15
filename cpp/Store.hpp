#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <random>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include <nlohmann/json.hpp>

#include "chat.pb.h"

struct Entry {
    std::string id;
    std::string origin;
    std::string kind;
    double ts = 0;
    int64_t clock = 0;
    std::string user;
    std::string channel;
    std::string text;

    LogEntry toProto() const {
        LogEntry le;
        le.set_id(id);
        le.set_origin(origin);
        le.set_kind(kind);
        le.set_timestamp(ts);
        le.set_clock(clock);
        le.set_user(user);
        le.set_channel(channel);
        le.set_text(text);
        return le;
    }

    static Entry fromProto(const LogEntry& le) {
        Entry e;
        e.id = le.id();
        e.origin = le.origin();
        e.kind = le.kind();
        e.ts = le.timestamp();
        e.clock = le.clock();
        e.user = le.user();
        e.channel = le.channel();
        e.text = le.text();
        return e;
    }

    std::string toJson() const {
        nlohmann::json j = {{"id", id},   {"origin", origin}, {"kind", kind}, {"ts", ts},
                            {"clock", clock}, {"user", user},     {"channel", channel}, {"text", text}};
        return j.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
    }

    static Entry fromJson(const nlohmann::json& j) {
        Entry e;
        e.id = j.value("id", "");
        e.origin = j.value("origin", "");
        e.kind = j.value("kind", "");
        e.ts = j.value("ts", 0.0);
        e.clock = j.value("clock", static_cast<int64_t>(0));
        e.user = j.value("user", "");
        e.channel = j.value("channel", "");
        e.text = j.value("text", "");
        return e;
    }
};

class Store {
public:
    Store(const std::string& dataDir, const std::string& serverName) : serverName_(serverName) {
        std::filesystem::create_directories(dataDir);
        path_ = dataDir + "/" + serverName + ".jsonl";
        std::ifstream in(path_);
        std::string linha;
        while (std::getline(in, linha)) {
            if (linha.find_first_not_of(" \t\r\n") == std::string::npos) {
                continue;
            }
            auto j = nlohmann::json::parse(linha, nullptr, false);
            if (!j.is_discarded()) {
                aplicar(Entry::fromJson(j));
            }
        }
        fd_ = ::open(path_.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
    }

    ~Store() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    const std::string& path() const { return path_; }

    const std::vector<Entry>& entries() const { return entries_; }

    Entry newEntry(const std::string& kind, double ts, int64_t clock, const std::string& user,
                   const std::string& channel, const std::string& text) const {
        Entry e;
        e.id = novoId();
        e.origin = serverName_;
        e.kind = kind;
        e.ts = ts;
        e.clock = clock;
        e.user = user;
        e.channel = channel;
        e.text = text;
        return e;
    }

    bool add(const Entry& e) {
        if (ids_.count(e.id) > 0) {
            return false;
        }
        aplicar(e);
        std::string linha = e.toJson() + "\n";
        if (::write(fd_, linha.data(), linha.size()) < 0) {
            return true;
        }
        ::fsync(fd_);
        return true;
    }

    std::vector<Entry> messages(const std::string& channel) const {
        std::vector<Entry> msgs;
        for (const auto& e : entries_) {
            if (e.kind == "message" && e.channel == channel) {
                msgs.push_back(e);
            }
        }
        std::sort(msgs.begin(), msgs.end(), [](const Entry& a, const Entry& b) {
            if (a.clock != b.clock) return a.clock < b.clock;
            if (a.origin != b.origin) return a.origin < b.origin;
            return a.id < b.id;
        });
        return msgs;
    }

    bool hasChannel(const std::string& name) const { return channels_.count(name) > 0; }

    std::vector<std::string> channelNames() const { return {channels_.begin(), channels_.end()}; }

private:
    static std::string novoId() {
        static std::random_device rd;
        static std::mt19937_64 gen(rd());
        static const char* hex = "0123456789abcdef";
        std::string id(32, '0');
        for (auto& c : id) {
            c = hex[gen() & 0xF];
        }
        return id;
    }

    void aplicar(const Entry& e) {
        entries_.push_back(e);
        ids_.insert(e.id);
        if (e.kind == "channel") {
            channels_.insert(e.channel);
        }
    }

    std::string serverName_;
    std::string path_;
    int fd_ = -1;
    std::vector<Entry> entries_;
    std::unordered_set<std::string> ids_;
    std::set<std::string> channels_;
};
