#include <cctype>
#include <random>
#include <set>
#include <vector>

#include "Common.hpp"

namespace {

const std::string HOST_BROKER = util::variavel("BROKER_HOST", "broker");
const std::string HOST_PROXY = util::variavel("PROXY_HOST", "proxy");
constexpr int TEMPO_LIMITE_MS = 5000;

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
        : gerador_(std::random_device{}()),
          relogio_(0.0),
          mensageiro_(inicial(), &relogio_),
          contexto_(1),
          assinante_(contexto_, zmq::socket_type::assinante) {
        nome_ = mensageiro_.nome();
        assinante_.connect("tcp://" + HOST_PROXY + ":5558");
        conectar();
    }

    void executar() {
        login();
        while (true) {
            auto canais = listarCanais();
            if (canais.size() < 5) {
                criarCanal("canal-" + std::to_string(sortear(1000, 9999)));
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
                    inscrever(disponiveis[sortear(0, static_cast<int>(disponiveis.size()) - 1)]);
                }
            }
            if (!canais.empty()) {
                std::string canal = canais[sortear(0, static_cast<int>(canais.size()) - 1)];
                for (int i = 0; i < 10; i++) {
                    publicar(canal, FRASES[sortear(0, static_cast<int>(FRASES.size()) - 1)]);
                    esperar(1000);
                }
            } else {
                esperar(1000);
            }
        }
    }

private:
    static std::string inicial() {
        std::string n = util::variavel("BOT_NAME", "");
        if (!n.empty()) {
            return n;
        }
        std::mt19937 g(std::random_device{}());
        return "cpp-bot-" + std::to_string(1000 + g() % 9000);
    }

    int sortear(int inicio, int fim) {
        std::uniform_int_distribution<int> d(inicio, fim);
        return d(gerador_);
    }

    void conectar() {
        requisicao_ = std::make_unique<zmq::socket_t>(contexto_, zmq::socket_type::requisicao);
        requisicao_->set(zmq::sockopt::linger, 0);
        requisicao_->connect("tcp://" + HOST_BROKER + ":5555");
    }

    Envelope requisitar(const Envelope& envelope) {
        while (true) {
            enviar(*requisicao_, envelope.SerializeAsString());
            mensageiro_.exibir("ENVIA", envelope, "servidor");
            zmq::pollitem_t item = {requisicao_->handle(), 0, ZMQ_POLLIN, 0};
            std::vector<zmq::pollitem_t> itens = {item};
            zmq::poll(itens, std::chrono::milliseconds(TEMPO_LIMITE_MS));
            if (itens[0].revents & ZMQ_POLLIN) {
                auto dados = receber(*requisicao_, false);
                Envelope resposta;
                if (dados && mensageiro_.ler(*dados, &resposta)) {
                    mensageiro_.exibir("RECEBE", resposta, resposta.remetente());
                    return resposta;
                }
            }
            std::cout << "[" << nome_ << "] sem resposta em " << TEMPO_LIMITE_MS << " ms, tentando de novo"
                      << std::endl;
            conectar();
        }
    }

    void login() {
        while (true) {
            Envelope requisicao = mensageiro_.montar(LOGIN);
            requisicao.set_usuario(nome_);
            if (requisitar(requisicao).status() == OK) {
                return;
            }
            util::dormir(1000);
            nome_ = "cpp-bot-" + std::to_string(sortear(1000, 9999));
            mensageiro_.setNome(nome_);
        }
    }

    std::vector<std::string> listarCanais() {
        Envelope resposta = requisitar(mensageiro_.montar(LISTAR_CANAIS));
        std::vector<std::string> canais;
        if (resposta.status() == OK) {
            for (const auto& c : resposta.canais()) {
                canais.push_back(c);
            }
        }
        return canais;
    }

    bool criarCanal(const std::string& nomeCanal) {
        Envelope requisicao = mensageiro_.montar(CRIAR_CANAL);
        requisicao.set_canal(nomeCanal);
        return requisitar(requisicao).status() == OK;
    }

    void inscrever(const std::string& canal) {
        assinante_.set(zmq::sockopt::subscribe, canal);
        inscritos_.insert(canal);
        std::cout << "[" << nome_ << "] inscrito no canal #" << canal << " (" << inscritos_.size()
                  << " inscrições)" << std::endl;
    }

    bool publicar(const std::string& canal, const std::string& texto) {
        Envelope requisicao = mensageiro_.montar(PUBLICAR);
        requisicao.set_canal(canal);
        requisicao.set_texto(texto);
        return requisitar(requisicao).status() == OK;
    }

    void receberPublicacao() {
        auto canal = receber(assinante_, false);
        std::optional<std::string> dados;
        while (assinante_.get(zmq::sockopt::rcvmore)) {
            dados = receber(assinante_, false);
        }
        if (!canal || !dados || inscritos_.count(*canal) == 0) {
            return;
        }
        Envelope envelope;
        if (mensageiro_.ler(*dados, &envelope)) {
            mensageiro_.exibir("RECEBE", envelope, "#" + *canal, "  | recebida em " + util::hora());
        }
    }

    void esperar(int milissegundos) {
        long long fim = agoraMs() + milissegundos;
        while (true) {
            long long resto = fim - agoraMs();
            if (resto <= 0) {
                return;
            }
            std::vector<zmq::pollitem_t> itens = {{assinante_.handle(), 0, ZMQ_POLLIN, 0}};
            zmq::poll(itens, std::chrono::milliseconds(resto));
            if (itens[0].revents & ZMQ_POLLIN) {
                receberPublicacao();
            }
        }
    }

    std::mt19937 gerador_;
    PhysicalClock relogio_;
    Messenger mensageiro_;
    std::string nome_;
    zmq::context_t contexto_;
    zmq::socket_t assinante_;
    std::unique_ptr<zmq::socket_t> requisicao_;
    std::set<std::string> inscritos_;
};

}  // namespace

int main() {
    Bot bot;
    bot.executar();
    return 0;
}
