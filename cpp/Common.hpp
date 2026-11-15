#pragma once

#include <zmq.hpp>

#include <algorithm>
#include <cctype>
#include <memory>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <optional>
#include <string>
#include <thread>

#include "chat.pb.h"

namespace util {

inline std::string env(const std::string& nome, const std::string& padrao) {
    const char* v = std::getenv(nome.c_str());
    return (v == nullptr || *v == '\0') ? padrao : std::string(v);
}

inline double agora() {
    using namespace std::chrono;
    return duration_cast<duration<double>>(system_clock::now().time_since_epoch()).count();
}

inline std::string hora() {
    using namespace std::chrono;
    auto t = system_clock::now();
    std::time_t tt = system_clock::to_time_t(t);
    std::tm tm{};
    localtime_r(&tt, &tm);
    int ms = static_cast<int>(duration_cast<milliseconds>(t.time_since_epoch()).count() % 1000);
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03d", tm.tm_hour, tm.tm_min, tm.tm_sec, ms);
    return buf;
}

inline bool nomeValido(const std::string& s) {
    if (s.empty() || s.size() > 32) {
        return false;
    }
    return std::all_of(s.begin(), s.end(), [](unsigned char c) {
        return std::isalnum(c) != 0 || c == '_' || c == '-';
    });
}

inline void dormir(int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

inline std::string formatar(const char* fmt, double v) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), fmt, v);
    return buf;
}

}

class PhysicalClock {
public:
    explicit PhysicalClock(double skew) : skew_(skew) {}

    double raw() const { return util::agora() + skew_; }

    double now() const { return raw() + offset_; }

    double sync(double horaRemota, double t0, double t1) {
        double estimada = horaRemota + (t1 - t0) / 2.0;
        offset_ = estimada - t1;
        return offset_;
    }

private:
    double skew_;
    double offset_ = 0.0;
};

class Messenger {
public:
    Messenger(std::string name, PhysicalClock* physical) : name_(std::move(name)), physical_(physical) {}

    const std::string& name() const { return name_; }

    void setName(const std::string& n) { name_ = n; }

    int64_t clock() const { return clock_; }

    Envelope build(Type type) {
        clock_++;
        Envelope e;
        e.set_type(type);
        e.set_sender(name_);
        e.set_timestamp(physical_->now());
        e.set_clock(clock_);
        return e;
    }

    bool parse(const std::string& data, Envelope* env) {
        if (!env->ParseFromString(data)) {
            return false;
        }
        clock_ = std::max(clock_, env->clock());
        return true;
    }

    void show(const std::string& direction, const Envelope& env, const std::string& other,
              const std::string& extra = "") const {
        const char* seta = direction == "ENVIA" ? "->" : "<-";
        char cab[160];
        std::snprintf(cab, sizeof(cab), "%s [%s|L=%lld] %-7s %s %-14s ", util::hora().c_str(), name_.c_str(),
                      static_cast<long long>(clock_), direction.c_str(), seta, other.c_str());
        std::cout << cab << env.Utf8DebugString() << extra << std::endl;
    }

private:
    std::string name_;
    PhysicalClock* physical_;
    int64_t clock_ = 0;
};

inline void enviar(zmq::socket_t& s, const std::string& dados, bool maisPartes = false) {
    s.send(zmq::buffer(dados), maisPartes ? zmq::send_flags::sndmore : zmq::send_flags::none);
}

inline std::optional<std::string> receber(zmq::socket_t& s, bool semBloquear) {
    zmq::message_t m;
    auto r = s.recv(m, semBloquear ? zmq::recv_flags::dontwait : zmq::recv_flags::none);
    if (!r.has_value()) {
        return std::nullopt;
    }
    return std::string(static_cast<const char*>(m.data()), m.size());
}
