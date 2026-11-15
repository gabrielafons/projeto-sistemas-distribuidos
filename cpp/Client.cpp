#include <cctype>
#include <random>
#include <set>
#include <vector>

#include "Common.hpp"

namespace {

const std::string BROKER_HOST = util::env("BROKER_HOST", "broker");
const std::string PROXY_HOST = util::env("PROXY_HOST", "proxy");
constexpr int TIMEOUT_MS = 5000;

const std::vector<std::string> FRASES = {
    "bom dia",          "alguém aí?",       "olá, pessoal",
    "testando o canal", "mensagem aleatória", "tudo bem por aí?",
    "sistemas distribuídos são legais", "ping", "mais uma mensagem", "o que vocês acham?"};

long long agoraMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

class Bot {
public:
    Bot()
        : gen_(std::random_device{}()),
          relogio_(0.0),
          msgr_(inicial(), &relogio_),
          ctx_(1),
          sub_(ctx_, zmq::socket_type::sub) {
        name_ = msgr_.name();
        sub_.connect("tcp://" + PROXY_HOST + ":5558");
        conectar();
    }

    void run() {
        login();
        while (true) {
            auto canais = listarCanais();
            if (canais.size() < 5) {
                criarCanal("canal-" + std::to_string(sorteio(1000, 9999)));
                canais = listarCanais();
            }
            if (inscritos_.size() < 3) {
                std::vector<std::string> disponiveis;
                for (const auto& c : canais) {
                    if (inscritos_.count(c) == 0) {
                        disponiveis.push_back(c);
                    }
                }
                if (!disponiveis.empty()) {
                    inscrever(disponiveis[sorteio(0, static_cast<int>(disponiveis.size()) - 1)]);
                }
            }
            if (!canais.empty()) {
                std::string canal = canais[sorteio(0, static_cast<int>(canais.size()) - 1)];
                for (int i = 0; i < 10; i++) {
                    publicar(canal, FRASES[sorteio(0, static_cast<int>(FRASES.size()) - 1)]);
                    esperar(1000);
                }
                historico(canal);
            } else {
                esperar(1000);
            }
        }
    }

private:
    static std::string inicial() {
        std::string n = util::env("BOT_NAME", "");
        if (!n.empty()) {
            return n;
        }
        std::mt19937 g(std::random_device{}());
        return "cpp-bot-" + std::to_string(1000 + g() % 9000);
    }

    int sorteio(int a, int b) {
        std::uniform_int_distribution<int> d(a, b);
        return d(gen_);
    }

    void conectar() {
        req_ = std::make_unique<zmq::socket_t>(ctx_, zmq::socket_type::req);
        req_->set(zmq::sockopt::linger, 0);
        req_->connect("tcp://" + BROKER_HOST + ":5555");
    }

    Envelope requisitar(const Envelope& env) {
        while (true) {
            enviar(*req_, env.SerializeAsString());
            msgr_.show("ENVIA", env, "servidor");
            zmq::pollitem_t item = {req_->handle(), 0, ZMQ_POLLIN, 0};
            std::vector<zmq::pollitem_t> itens = {item};
            zmq::poll(itens, std::chrono::milliseconds(TIMEOUT_MS));
            if (itens[0].revents & ZMQ_POLLIN) {
                auto dados = receber(*req_, false);
                Envelope resp;
                if (dados && msgr_.parse(*dados, &resp)) {
                    msgr_.show("RECEBE", resp, resp.sender());
                    return resp;
                }
            }
            std::cout << "[" << name_ << "] sem resposta em " << TIMEOUT_MS << " ms, tentando de novo"
                      << std::endl;
            conectar();
        }
    }

    void login() {
        while (true) {
            Envelope req = msgr_.build(LOGIN);
            req.set_user(name_);
            if (requisitar(req).status() == OK) {
                return;
            }
            util::dormir(1000);
            name_ = "cpp-bot-" + std::to_string(sorteio(1000, 9999));
            msgr_.setName(name_);
        }
    }

    std::vector<std::string> listarCanais() {
        Envelope resp = requisitar(msgr_.build(LIST_CHANNELS));
        std::vector<std::string> canais;
        if (resp.status() == OK) {
            for (const auto& c : resp.channels()) {
                canais.push_back(c);
            }
        }
        return canais;
    }

    bool criarCanal(const std::string& nome) {
        Envelope req = msgr_.build(CREATE_CHANNEL);
        req.set_channel(nome);
        return requisitar(req).status() == OK;
    }

    void inscrever(const std::string& canal) {
        sub_.set(zmq::sockopt::subscribe, canal);
        inscritos_.insert(canal);
        std::cout << "[" << name_ << "] inscrito no canal #" << canal << " (" << inscritos_.size()
                  << " inscrições)" << std::endl;
    }

    bool publicar(const std::string& canal, const std::string& texto) {
        Envelope req = msgr_.build(PUBLISH);
        req.set_channel(canal);
        req.set_text(texto);
        return requisitar(req).status() == OK;
    }

    void historico(const std::string& canal) {
        Envelope req = msgr_.build(HISTORY);
        req.set_channel(canal);
        Envelope resp = requisitar(req);
        if (resp.status() != OK) {
            return;
        }
        std::cout << "[" << name_ << "] histórico de #" << canal << ": " << resp.total()
                  << " mensagens no total (respondido por " << resp.sender() << "); últimas "
                  << resp.entries_size() << ":" << std::endl;
        for (const auto& e : resp.entries()) {
            std::cout << "    [L=" << e.clock() << "] " << e.user() << ": " << e.text() << "  (recebida por "
                      << e.origin() << ")" << std::endl;
        }
    }

    void receberPublicacao() {
        auto canal = receber(sub_, false);
        std::optional<std::string> dados;
        while (sub_.get(zmq::sockopt::rcvmore)) {
            dados = receber(sub_, false);
        }
        if (!canal || !dados || inscritos_.count(*canal) == 0) {
            return;
        }
        Envelope env;
        if (msgr_.parse(*dados, &env)) {
            msgr_.show("RECEBE", env, "#" + *canal, "  | recebida em " + util::hora());
        }
    }

    void esperar(int milissegundos) {
        long long fim = agoraMs() + milissegundos;
        while (true) {
            long long resto = fim - agoraMs();
            if (resto <= 0) {
                return;
            }
            std::vector<zmq::pollitem_t> itens = {{sub_.handle(), 0, ZMQ_POLLIN, 0}};
            zmq::poll(itens, std::chrono::milliseconds(resto));
            if (itens[0].revents & ZMQ_POLLIN) {
                receberPublicacao();
            }
        }
    }

    std::mt19937 gen_;
    PhysicalClock relogio_;
    Messenger msgr_;
    std::string name_;
    zmq::context_t ctx_;
    zmq::socket_t sub_;
    std::unique_ptr<zmq::socket_t> req_;
    std::set<std::string> inscritos_;
};

}  // namespace

int main() {
    Bot bot;
    bot.run();
    return 0;
}
