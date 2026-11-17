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

inline std::string variavel(const std::string& nome, const std::string& padrao) {
    const char* valor = std::getenv(nome.c_str());
    return (valor == nullptr || *valor == '\0') ? padrao : std::string(valor);
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

inline bool nomeValido(const std::string& texto) {
    if (texto.empty() || texto.size() > 32) {
        return false;
    }
    return std::all_of(texto.begin(), texto.end(), [](unsigned char c) {
        return std::isalnum(c) != 0 || c == '_' || c == '-';
    });
}

inline void dormir(int milissegundos) {
    std::this_thread::sleep_for(std::chrono::milliseconds(milissegundos));
}

inline std::string formatar(const char* formato, double valor) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), formato, valor);
    return buf;
}

}

class PhysicalClock {
public:
    explicit PhysicalClock(double desvio) : desvio_(desvio) {}

    double bruto() const { return util::agora() + desvio_; }

    double agora() const { return bruto() + correcao_; }

    double sincronizar(double horaRemota, double t0, double t1) {
        double estimada = horaRemota + (t1 - t0) / 2.0;
        correcao_ = estimada - t1;
        return correcao_;
    }

private:
    double desvio_;
    double correcao_ = 0.0;
};

class Messenger {
public:
    Messenger(std::string nome, PhysicalClock* fisico) : nome_(std::move(nome)), fisico_(fisico) {}

    const std::string& nome() const { return nome_; }

    void setNome(const std::string& n) { nome_ = n; }

    int64_t relogio() const { return relogio_; }

    Envelope montar(Type tipo) {
        relogio_++;
        Envelope e;
        e.set_tipo(tipo);
        e.set_remetente(nome_);
        e.set_instante(fisico_->agora());
        e.set_relogio(relogio_);
        return e;
    }

    bool ler(const std::string& dados, Envelope* envelope) {
        if (!envelope->ParseFromString(dados)) {
            return false;
        }
        relogio_ = std::max(relogio_, envelope->relogio());
        return true;
    }

    void exibir(const std::string& direcao, const Envelope& envelope, const std::string& outro,
                const std::string& extra = "") const {
        const char* seta = direcao == "ENVIA" ? "->" : "<-";
        char cab[160];
        std::snprintf(cab, sizeof(cab), "%s [%s|L=%lld] %-7s %s %-14s ", util::hora().c_str(), nome_.c_str(),
                      static_cast<long long>(relogio_), direcao.c_str(), seta, outro.c_str());
        std::cout << cab << envelope.Utf8DebugString() << extra << std::endl;
    }

private:
    std::string nome_;
    PhysicalClock* fisico_;
    int64_t relogio_ = 0;
};

inline void enviar(zmq::socket_t& socket, const std::string& dados, bool maisPartes = false) {
    socket.send(zmq::buffer(dados), maisPartes ? zmq::send_flags::sndmore : zmq::send_flags::none);
}

inline std::optional<std::string> receber(zmq::socket_t& socket, bool semBloquear) {
    zmq::message_t mensagem;
    auto resultado = socket.recv(mensagem, semBloquear ? zmq::recv_flags::dontwait : zmq::recv_flags::none);
    if (!resultado.has_value()) {
        return std::nullopt;
    }
    return std::string(static_cast<const char*>(mensagem.data()), mensagem.size());
}
