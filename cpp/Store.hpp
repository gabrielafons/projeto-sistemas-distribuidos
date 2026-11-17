#pragma once

#include <fcntl.h>
#include <unistd.h>

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
    std::string origem;
    std::string tipo;
    double instante = 0;
    int64_t relogio = 0;
    std::string usuario;
    std::string canal;
    std::string texto;

    LogEntry paraProto() const {
        LogEntry le;
        le.set_id(id);
        le.set_origem(origem);
        le.set_tipo(tipo);
        le.set_instante(instante);
        le.set_relogio(relogio);
        le.set_usuario(usuario);
        le.set_canal(canal);
        le.set_texto(texto);
        return le;
    }

    static Entry deProto(const LogEntry& le) {
        Entry e;
        e.id = le.id();
        e.origem = le.origem();
        e.tipo = le.tipo();
        e.instante = le.instante();
        e.relogio = le.relogio();
        e.usuario = le.usuario();
        e.canal = le.canal();
        e.texto = le.texto();
        return e;
    }

    std::string paraJson() const {
        nlohmann::json j = {{"id", id},   {"origin", origem}, {"kind", tipo}, {"ts", instante},
                            {"clock", relogio}, {"user", usuario}, {"channel", canal}, {"text", texto}};
        return j.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
    }

    static Entry deJson(const nlohmann::json& j) {
        Entry e;
        e.id = j.value("id", "");
        e.origem = j.value("origin", "");
        e.tipo = j.value("kind", "");
        e.instante = j.value("ts", 0.0);
        e.relogio = j.value("clock", static_cast<int64_t>(0));
        e.usuario = j.value("user", "");
        e.canal = j.value("channel", "");
        e.texto = j.value("text", "");
        return e;
    }
};

class Store {
public:
    Store(const std::string& pastaDados, const std::string& nomeServidor) : nomeServidor_(nomeServidor) {
        std::filesystem::create_directories(pastaDados);
        caminho_ = pastaDados + "/" + nomeServidor + ".jsonl";
        std::ifstream entrada(caminho_);
        std::string linha;
        while (std::getline(entrada, linha)) {
            if (linha.find_first_not_of(" \t\r\n") == std::string::npos) {
                continue;
            }
            auto j = nlohmann::json::parse(linha, nullptr, false);
            if (!j.is_discarded()) {
                aplicar(Entry::deJson(j));
            }
        }
        fd_ = ::open(caminho_.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
    }

    ~Store() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    const std::string& caminho() const { return caminho_; }

    const std::vector<Entry>& entradas() const { return entradas_; }

    Entry novaEntrada(const std::string& tipo, double instante, int64_t relogio, const std::string& usuario,
                      const std::string& canal, const std::string& texto) const {
        Entry e;
        e.id = novoId();
        e.origem = nomeServidor_;
        e.tipo = tipo;
        e.instante = instante;
        e.relogio = relogio;
        e.usuario = usuario;
        e.canal = canal;
        e.texto = texto;
        return e;
    }

    bool adicionar(const Entry& e) {
        if (ids_.count(e.id) > 0) {
            return false;
        }
        aplicar(e);
        std::string linha = e.paraJson() + "\n";
        if (::write(fd_, linha.data(), linha.size()) < 0) {
            return true;
        }
        ::fsync(fd_);
        return true;
    }

    bool temCanal(const std::string& nome) const { return canais_.count(nome) > 0; }

    std::vector<std::string> nomesCanais() const { return {canais_.begin(), canais_.end()}; }

private:
    static std::string novoId() {
        static std::random_device rd;
        static std::mt19937_64 gerador(rd());
        static const char* hex = "0123456789abcdef";
        std::string id(32, '0');
        for (auto& c : id) {
            c = hex[gerador() & 0xF];
        }
        return id;
    }

    void aplicar(const Entry& e) {
        entradas_.push_back(e);
        ids_.insert(e.id);
        if (e.tipo == "channel") {
            canais_.insert(e.canal);
        }
    }

    std::string nomeServidor_;
    std::string caminho_;
    int fd_ = -1;
    std::vector<Entry> entradas_;
    std::unordered_set<std::string> ids_;
    std::set<std::string> canais_;
};
