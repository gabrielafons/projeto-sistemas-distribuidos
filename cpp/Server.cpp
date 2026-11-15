#include <cctype>
#include <cmath>
#include <map>
#include <random>
#include <set>
#include <vector>

#include "Common.hpp"
#include "Store.hpp"

namespace {

const std::string NAME = util::env("SERVER_NAME", "visao-cpp-1");
const std::string BROKER_HOST = util::env("BROKER_HOST", "broker");
const std::string PROXY_HOST = util::env("PROXY_HOST", "proxy");
const std::string REF_HOST = util::env("REF_HOST", "reference");
const std::string DATA_DIR = util::env("DATA_DIR", "/data");
const std::string PEER_PORT = util::env("PEER_PORT", "6000");
const std::string ADDRESS = util::env("ADVERTISE_ADDR", "tcp://" + NAME + ":" + PEER_PORT);
const double CLOCK_SKEW = std::stod(util::env("CLOCK_SKEW", "0"));

constexpr int HEARTBEAT_A_CADA = 10;
constexpr int SINCRONIZA_A_CADA = 15;
constexpr int TIMEOUT_REQ = 2000;
constexpr int TIMEOUT_SERVIDOR = 1000;
constexpr int ESPERA_ANUNCIO = 5000;
constexpr int ANTI_ENTROPIA = 20000;
constexpr int TIMEOUT_SYNC = 3000;
constexpr size_t HISTORICO = 10;

const std::string TOPICO_SERVIDORES = "servers";
const std::string TOPICO_REPLICA = "replica";

long long agoraMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

bool reservado(const std::string& nome) {
    return nome == TOPICO_SERVIDORES || nome == TOPICO_REPLICA;
}

class Server {
public:
    Server()
        : relogio_(CLOCK_SKEW),
          msgr_(NAME, &relogio_),
          store_(DATA_DIR, NAME),
          ctx_(1),
          rep_(ctx_, zmq::socket_type::rep),
          peer_(ctx_, zmq::socket_type::rep),
          pub_(ctx_, zmq::socket_type::pub),
          sub_(ctx_, zmq::socket_type::sub),
          proximaSync_(agoraMs() + ANTI_ENTROPIA),
          gen_(std::random_device{}()) {
        rep_.connect("tcp://" + BROKER_HOST + ":5556");
        peer_.bind("tcp://*:" + PEER_PORT);
        pub_.connect("tcp://" + PROXY_HOST + ":5557");
        sub_.connect("tcp://" + PROXY_HOST + ":5558");
        sub_.set(zmq::sockopt::subscribe, TOPICO_SERVIDORES);
        sub_.set(zmq::sockopt::subscribe, TOPICO_REPLICA);
    }

    void run() {
        std::cout << "[" << NAME << "] iniciado. " << store_.channelNames().size()
                  << " canais carregados de " << store_.path() << std::endl;
        registrar();
        antiEntropia(true);
        while (true) {
            std::vector<zmq::pollitem_t> itens = {{rep_.handle(), 0, ZMQ_POLLIN, 0},
                                                  {peer_.handle(), 0, ZMQ_POLLIN, 0},
                                                  {sub_.handle(), 0, ZMQ_POLLIN, 0}};
            zmq::poll(itens, std::chrono::milliseconds(1000));
            if (itens[0].revents & ZMQ_POLLIN) {
                atenderCliente();
            }
            if (itens[1].revents & ZMQ_POLLIN) {
                atenderPeer();
            }
            if (itens[2].revents & ZMQ_POLLIN) {
                receberDoSub();
            }
            if (agoraMs() >= proximaSync_) {
                antiEntropia(false);
            }
            if (precisaEleicao_ || (coordinator_.empty() && agoraMs() >= proximaEleicao_)) {
                eleicao();
            }
        }
    }

private:
    void publicar(const std::string& topico, const Envelope& env) {
        enviar(pub_, topico, true);
        enviar(pub_, env.SerializeAsString());
    }

    std::optional<Envelope> pedir(const std::string& endereco, const Envelope& env, int timeoutMs) {
        try {
            zmq::socket_t s(ctx_, zmq::socket_type::req);
            s.set(zmq::sockopt::linger, 0);
            s.connect(endereco);
            enviar(s, env.SerializeAsString());
            auto pos = endereco.find("//");
            msgr_.show("ENVIA", env, pos == std::string::npos ? endereco : endereco.substr(pos + 2));
            long long fim = agoraMs() + timeoutMs;
            while (true) {
                long long resto = fim - agoraMs();
                if (resto <= 0) {
                    semResposta(endereco, timeoutMs);
                    return std::nullopt;
                }
                std::vector<zmq::pollitem_t> itens = {{s.handle(), 0, ZMQ_POLLIN, 0},
                                                      {peer_.handle(), 0, ZMQ_POLLIN, 0}};
                zmq::poll(itens, std::chrono::milliseconds(resto));
                if (itens[1].revents & ZMQ_POLLIN) {
                    atenderPeer();
                }
                if (itens[0].revents & ZMQ_POLLIN) {
                    auto dados = receber(s, false);
                    Envelope resp;
                    if (!dados || !msgr_.parse(*dados, &resp)) {
                        return std::nullopt;
                    }
                    msgr_.show("RECEBE", resp, resp.sender());
                    return resp;
                }
            }
        } catch (const zmq::error_t& e) {
            std::cout << "[" << NAME << "] falha ao falar com " << endereco << ": " << e.what() << std::endl;
            return std::nullopt;
        }
    }

    static void semResposta(const std::string& endereco, int timeoutMs) {
        std::cout << "[" << NAME << "] sem resposta de " << endereco << " em " << timeoutMs / 1000.0 << "s"
                  << std::endl;
    }

    std::optional<Envelope> referencia(const Envelope& env) {
        return pedir("tcp://" + REF_HOST + ":5559", env, TIMEOUT_REQ);
    }

    void registrar() {
        while (true) {
            Envelope req = msgr_.build(GET_RANK);
            req.set_address(ADDRESS);
            auto resp = referencia(req);
            if (resp && resp->status() == OK) {
                rank_ = resp->rank();
                std::cout << "[" << NAME << "] rank " << rank_ << ", endereço " << ADDRESS << std::endl;
                return;
            }
            util::dormir(2000);
        }
    }

    void atualizarServidores() {
        auto lista = referencia(msgr_.build(LIST_SERVERS));
        if (lista && lista->status() == OK) {
            servidores_.clear();
            std::string nomes;
            for (const auto& s : lista->servers()) {
                servidores_[s.name()] = s;
                if (!nomes.empty()) {
                    nomes += ", ";
                }
                nomes += s.name() + "(rank " + std::to_string(s.rank()) + ")";
            }
            std::cout << "[" << NAME << "] servidores disponíveis: " << nomes << std::endl;
        }
    }

    void heartbeat() {
        Envelope req = msgr_.build(HEARTBEAT);
        req.set_address(ADDRESS);
        auto resp = referencia(req);
        if (!resp) {
            return;
        }
        if (resp->status() != OK) {
            registrar();
        }
        atualizarServidores();
    }

    void eleicao() {
        precisaEleicao_ = false;
        std::cout << "[" << NAME << "] iniciando eleição (rank " << rank_ << ")" << std::endl;
        atualizarServidores();
        std::vector<ServerInfo> superiores;
        for (const auto& [nome, s] : servidores_) {
            if (s.rank() > rank_) {
                superiores.push_back(s);
            }
        }
        std::sort(superiores.begin(), superiores.end(),
                  [](const ServerInfo& a, const ServerInfo& b) { return a.rank() > b.rank(); });
        for (const auto& s : superiores) {
            Envelope req = msgr_.build(ELECTION);
            req.set_rank(rank_);
            auto resp = pedir(s.address(), req, TIMEOUT_SERVIDOR);
            if (resp && resp->status() == OK) {
                std::cout << "[" << NAME << "] " << s.name() << " (rank " << s.rank()
                          << ") respondeu OK: aguardando o anúncio" << std::endl;
                coordinator_.clear();
                proximaEleicao_ = agoraMs() + ESPERA_ANUNCIO;
                return;
            }
        }
        virarCoordenador();
    }

    void virarCoordenador() {
        coordinator_ = NAME;
        Envelope anuncio = msgr_.build(COORDINATOR);
        anuncio.set_coordinator(NAME);
        anuncio.set_rank(rank_);
        publicar(TOPICO_SERVIDORES, anuncio);
        msgr_.show("ENVIA", anuncio, "#" + TOPICO_SERVIDORES);
        std::cout << "[" << NAME << "] eleito coordenador" << std::endl;
    }

    void sincronizarRelogio() {
        if (coordinator_.empty()) {
            return;
        }
        if (coordinator_ == NAME) {
            std::cout << "[" << NAME << "] sou o coordenador: meu relógio é a referência" << std::endl;
            return;
        }
        if (servidores_.count(coordinator_) == 0) {
            atualizarServidores();
        }
        std::optional<Envelope> resp;
        double t0 = 0;
        double t1 = 0;
        auto it = servidores_.find(coordinator_);
        if (it != servidores_.end()) {
            std::string endereco = it->second.address();
            t0 = relogio_.raw();
            resp = pedir(endereco, msgr_.build(CLOCK), TIMEOUT_SERVIDOR);
            t1 = relogio_.raw();
        }
        if (!resp || resp->status() != OK) {
            std::cout << "[" << NAME << "] coordenador " << coordinator_ << " indisponível: pedindo eleição"
                      << std::endl;
            coordinator_.clear();
            precisaEleicao_ = true;
            return;
        }
        double offset = relogio_.sync(resp->time(), t0, t1);
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%+.3f", offset);
        std::cout << "[" << NAME << "] relógio ajustado pelo coordenador " << coordinator_ << ": correção de "
                  << buf << "s" << std::endl;
    }

    Envelope erro(Type type, const std::string& texto) {
        Envelope e = msgr_.build(type);
        e.set_status(ERROR);
        e.set_error(texto);
        return e;
    }

    void atenderPeer() {
        auto dados = receber(peer_, false);
        Envelope env;
        if (!dados || !msgr_.parse(*dados, &env)) {
            enviar(peer_, erro(UNKNOWN, "mensagem inválida").SerializeAsString());
            return;
        }
        msgr_.show("RECEBE", env, env.sender());
        Envelope resp;
        switch (env.type()) {
            case ELECTION:
                resp = msgr_.build(env.type());
                resp.set_status(OK);
                resp.set_rank(rank_);
                precisaEleicao_ = true;
                break;
            case SYNC:
                resp = msgr_.build(env.type());
                resp.set_status(OK);
                for (const auto& e : store_.entries()) {
                    *resp.add_entries() = e.toProto();
                }
                break;
            case CLOCK:
                if (coordinator_ == NAME) {
                    resp = msgr_.build(env.type());
                    resp.set_status(OK);
                    resp.set_time(relogio_.now());
                } else {
                    resp = erro(env.type(), "este servidor não é o coordenador");
                }
                break;
            default:
                resp = erro(env.type(), "tipo de mensagem desconhecido");
        }
        enviar(peer_, resp.SerializeAsString());
        msgr_.show("ENVIA", resp, env.sender());
    }

    void receberDoSub() {
        auto topico = receber(sub_, false);
        std::optional<std::string> dados;
        while (sub_.get(zmq::sockopt::rcvmore)) {
            dados = receber(sub_, false);
        }
        if (!topico || !dados) {
            return;
        }
        if (*topico != TOPICO_SERVIDORES && *topico != TOPICO_REPLICA) {
            return;
        }
        Envelope env;
        if (!msgr_.parse(*dados, &env)) {
            return;
        }
        if (env.sender() == NAME) {
            return;
        }
        msgr_.show("RECEBE", env, "#" + *topico);
        if (*topico == TOPICO_REPLICA) {
            aplicarEntradas(env.entries(), env.sender());
            return;
        }
        coordinator_ = env.coordinator();
        std::cout << "[" << NAME << "] coordenador agora é " << coordinator_ << std::endl;
        if (rank_ != 0 && env.rank() < rank_) {
            precisaEleicao_ = true;
        }
    }

    void gravar(const Entry& entrada) {
        if (store_.add(entrada)) {
            Envelope msg = msgr_.build(REPLICATE);
            *msg.add_entries() = entrada.toProto();
            publicar(TOPICO_REPLICA, msg);
            msgr_.show("ENVIA", msg, "#" + TOPICO_REPLICA);
        }
    }

    template <typename Colecao>
    int aplicarEntradas(const Colecao& entradas, const std::string& origem) {
        int novas = 0;
        for (const auto& le : entradas) {
            if (store_.add(Entry::fromProto(le))) {
                novas++;
            }
        }
        if (novas > 0) {
            std::cout << "[" << NAME << "] replicação: " << novas << " entrada(s) nova(s) recebida(s) de "
                      << origem << std::endl;
        }
        return novas;
    }

    void puxarDe(const ServerInfo& info) {
        auto resp = pedir(info.address(), msgr_.build(SYNC), TIMEOUT_SYNC);
        if (resp && resp->status() == OK) {
            aplicarEntradas(resp->entries(), info.name());
        }
    }

    void antiEntropia(bool todos) {
        proximaSync_ = agoraMs() + ANTI_ENTROPIA;
        atualizarServidores();
        std::vector<ServerInfo> outros;
        for (const auto& [nome, s] : servidores_) {
            if (nome != NAME) {
                outros.push_back(s);
            }
        }
        if (!todos && !outros.empty()) {
            std::uniform_int_distribution<size_t> d(0, outros.size() - 1);
            ServerInfo escolhido = outros[d(gen_)];
            outros = {escolhido};
        }
        for (const auto& info : outros) {
            puxarDe(info);
        }
    }

    Entry evento(const std::string& kind, const std::string& user, const std::string& channel,
                 const std::string& text) {
        return store_.newEntry(kind, relogio_.now(), msgr_.clock(), user, channel, text);
    }

    Envelope tratar(const Envelope& env) {
        switch (env.type()) {
            case LOGIN: {
                if (!util::nomeValido(env.user())) {
                    return erro(env.type(), "nome de usuário inválido (use 1 a 32 letras, números, _ ou -)");
                }
                gravar(evento("login", env.user(), "", ""));
                Envelope r = msgr_.build(env.type());
                r.set_status(OK);
                r.set_user(env.user());
                return r;
            }
            case CREATE_CHANNEL: {
                if (!util::nomeValido(env.channel())) {
                    return erro(env.type(), "nome de canal inválido (use 1 a 32 letras, números, _ ou -)");
                }
                if (reservado(env.channel())) {
                    return erro(env.type(), "'" + env.channel() + "' é um nome reservado");
                }
                if (store_.hasChannel(env.channel())) {
                    return erro(env.type(), "o canal '" + env.channel() + "' já existe");
                }
                gravar(evento("channel", env.sender(), env.channel(), ""));
                Envelope r = msgr_.build(env.type());
                r.set_status(OK);
                r.set_channel(env.channel());
                return r;
            }
            case PUBLISH: {
                if (!util::nomeValido(env.channel())) {
                    return erro(env.type(), "nome de canal inválido");
                }
                if (reservado(env.channel())) {
                    return erro(env.type(), "'" + env.channel() + "' é um nome reservado");
                }
                if (env.text().empty()) {
                    return erro(env.type(), "mensagem vazia");
                }
                Envelope publicacao = msgr_.build(PUBLICATION);
                publicacao.set_user(env.sender());
                publicacao.set_channel(env.channel());
                publicacao.set_text(env.text());
                gravar(store_.newEntry("message", publicacao.timestamp(), publicacao.clock(), env.sender(),
                                       env.channel(), env.text()));
                publicar(env.channel(), publicacao);
                msgr_.show("ENVIA", publicacao, "#" + env.channel());
                Envelope r = msgr_.build(env.type());
                r.set_status(OK);
                r.set_channel(env.channel());
                return r;
            }
            case LIST_CHANNELS: {
                Envelope r = msgr_.build(env.type());
                r.set_status(OK);
                for (const auto& c : store_.channelNames()) {
                    r.add_channels(c);
                }
                return r;
            }
            case HISTORY: {
                if (!util::nomeValido(env.channel())) {
                    return erro(env.type(), "nome de canal inválido");
                }
                auto msgs = store_.messages(env.channel());
                Envelope r = msgr_.build(env.type());
                r.set_status(OK);
                r.set_channel(env.channel());
                r.set_total(static_cast<int32_t>(msgs.size()));
                size_t inicio = msgs.size() > HISTORICO ? msgs.size() - HISTORICO : 0;
                for (size_t i = inicio; i < msgs.size(); i++) {
                    *r.add_entries() = msgs[i].toProto();
                }
                return r;
            }
            default:
                return erro(env.type(), "tipo de mensagem desconhecido");
        }
    }

    void atenderCliente() {
        auto dados = receber(rep_, false);
        Envelope env;
        if (!dados || !msgr_.parse(*dados, &env)) {
            Envelope resp = erro(UNKNOWN, "mensagem inválida (não é um Envelope Protobuf)");
            enviar(rep_, resp.SerializeAsString());
            msgr_.show("ENVIA", resp, "");
            return;
        }
        msgr_.show("RECEBE", env, env.sender());
        Envelope resp = tratar(env);
        enviar(rep_, resp.SerializeAsString());
        msgr_.show("ENVIA", resp, env.sender());
        nMsgs_++;
        if (nMsgs_ % HEARTBEAT_A_CADA == 0) {
            heartbeat();
        }
        if (nMsgs_ % SINCRONIZA_A_CADA == 0) {
            sincronizarRelogio();
        }
    }

    PhysicalClock relogio_;
    Messenger msgr_;
    Store store_;
    zmq::context_t ctx_;
    zmq::socket_t rep_;
    zmq::socket_t peer_;
    zmq::socket_t pub_;
    zmq::socket_t sub_;
    int rank_ = 0;
    long long nMsgs_ = 0;
    std::string coordinator_;
    std::map<std::string, ServerInfo> servidores_;
    bool precisaEleicao_ = false;
    long long proximaEleicao_ = 0;
    long long proximaSync_;
    std::mt19937 gen_;
};

}  // namespace

int main() {
    Server server;
    server.run();
    return 0;
}
