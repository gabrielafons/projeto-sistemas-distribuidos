#include <cctype>
#include <cmath>
#include <map>
#include <random>
#include <set>
#include <vector>

#include "Common.hpp"
#include "Armazenamento.hpp"

namespace {

const std::string NOME = util::variavel("SERVER_NAME", "visao-cpp-1");
const std::string HOST_BROKER = util::variavel("BROKER_HOST", "broker");
const std::string HOST_PROXY = util::variavel("PROXY_HOST", "proxy");
const std::string HOST_REFERENCIA = util::variavel("REF_HOST", "reference");
const std::string DIRETORIO_DADOS = util::variavel("DATA_DIR", "/data");
const std::string PORTA_PAR = util::variavel("PEER_PORT", "6000");
const std::string ENDERECO = util::variavel("ADVERTISE_ADDR", "tcp://" + NOME + ":" + PORTA_PAR);
const double DESVIO_RELOGIO = std::stod(util::variavel("CLOCK_SKEW", "0"));

constexpr int BATIMENTO_A_CADA = 10;
constexpr int SINCRONIZA_A_CADA = 15;
constexpr int TEMPO_LIMITE_REQ = 2000;
constexpr int TEMPO_LIMITE_SERVIDOR = 1000;
constexpr int ESPERA_ANUNCIO = 5000;
constexpr int ANTI_ENTROPIA = 20000;
constexpr int TEMPO_LIMITE_SYNC = 3000;

const std::string TOPICO_SERVIDORES = "servers";
const std::string TOPICO_REPLICA = "replica";

long long agoraMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

bool reservado(const std::string& nome) {
    return nome == TOPICO_SERVIDORES || nome == TOPICO_REPLICA;
}

class Servidor {
public:
    Servidor()
        : relogio_(DESVIO_RELOGIO),
          mensageiro_(NOME, &relogio_),
          armazenamento_(DIRETORIO_DADOS, NOME),
          contexto_(1),
          atendimento_(contexto_, zmq::socket_type::atendimento),
          par_(contexto_, zmq::socket_type::atendimento),
          publicador_(contexto_, zmq::socket_type::publicador),
          assinante_(contexto_, zmq::socket_type::assinante),
          proximaSync_(agoraMs() + ANTI_ENTROPIA),
          sorteio_(std::random_device{}()) {
        atendimento_.connect("tcp://" + HOST_BROKER + ":5556");
        par_.bind("tcp://*:" + PORTA_PAR);
        publicador_.connect("tcp://" + HOST_PROXY + ":5557");
        assinante_.connect("tcp://" + HOST_PROXY + ":5558");
        assinante_.set(zmq::sockopt::subscribe, TOPICO_SERVIDORES);
        assinante_.set(zmq::sockopt::subscribe, TOPICO_REPLICA);
    }

    void executar() {
        std::cout << "[" << NOME << "] iniciado. " << armazenamento_.nomesCanais().size()
                  << " canais carregados de " << armazenamento_.caminho() << std::endl;
        registrar();
        antiEntropia(true);
        while (true) {
            std::vector<zmq::pollitem_t> itens = {{atendimento_.handle(), 0, ZMQ_POLLIN, 0},
                                                  {par_.handle(), 0, ZMQ_POLLIN, 0},
                                                  {assinante_.handle(), 0, ZMQ_POLLIN, 0}};
            zmq::poll(itens, std::chrono::milliseconds(1000));
            if (itens[0].revents & ZMQ_POLLIN) {
                atenderCliente();
            }
            if (itens[1].revents & ZMQ_POLLIN) {
                atenderPar();
            }
            if (itens[2].revents & ZMQ_POLLIN) {
                receberDoSub();
            }
            if (agoraMs() >= proximaSync_) {
                antiEntropia(false);
            }
            if (precisaEleicao_ || (coordenador_.empty() && agoraMs() >= proximaEleicao_)) {
                eleicao();
            }
        }
    }

private:
    void publicar(const std::string& topico, const Envelope& envelope) {
        enviar(publicador_, topico, true);
        enviar(publicador_, envelope.SerializeAsString());
    }

    std::optional<Envelope> pedir(const std::string& endereco, const Envelope& envelope, int tempoLimiteMs) {
        try {
            zmq::socket_t requisicao(contexto_, zmq::socket_type::requisicao);
            requisicao.set(zmq::sockopt::linger, 0);
            requisicao.connect(endereco);
            enviar(requisicao, envelope.SerializeAsString());
            auto pos = endereco.find("//");
            mensageiro_.exibir("ENVIA", envelope, pos == std::string::npos ? endereco : endereco.substr(pos + 2));
            long long fim = agoraMs() + tempoLimiteMs;
            while (true) {
                long long resto = fim - agoraMs();
                if (resto <= 0) {
                    semResposta(endereco, tempoLimiteMs);
                    return std::nullopt;
                }
                std::vector<zmq::pollitem_t> itens = {{requisicao.handle(), 0, ZMQ_POLLIN, 0},
                                                      {par_.handle(), 0, ZMQ_POLLIN, 0}};
                zmq::poll(itens, std::chrono::milliseconds(resto));
                if (itens[1].revents & ZMQ_POLLIN) {
                    atenderPar();
                }
                if (itens[0].revents & ZMQ_POLLIN) {
                    auto dados = receber(requisicao, false);
                    Envelope resposta;
                    if (!dados || !mensageiro_.ler(*dados, &resposta)) {
                        return std::nullopt;
                    }
                    mensageiro_.exibir("RECEBE", resposta, resposta.remetente());
                    return resposta;
                }
            }
        } catch (const zmq::error_t& e) {
            std::cout << "[" << NOME << "] falha ao falar com " << endereco << ": " << e.what() << std::endl;
            return std::nullopt;
        }
    }

    static void semResposta(const std::string& endereco, int tempoLimiteMs) {
        std::cout << "[" << NOME << "] sem resposta de " << endereco << " em " << tempoLimiteMs / 1000.0 << "s"
                  << std::endl;
    }

    std::optional<Envelope> referencia(const Envelope& envelope) {
        return pedir("tcp://" + HOST_REFERENCIA + ":5559", envelope, TEMPO_LIMITE_REQ);
    }

    void registrar() {
        while (true) {
            Envelope requisicao = mensageiro_.montar(OBTER_RANK);
            requisicao.set_endereco(ENDERECO);
            auto resposta = referencia(requisicao);
            if (resposta && resposta->status() == OK) {
                rank_ = resposta->rank();
                std::cout << "[" << NOME << "] rank " << rank_ << ", endereço " << ENDERECO << std::endl;
                return;
            }
            util::dormir(2000);
        }
    }

    void atualizarServidores() {
        auto lista = referencia(mensageiro_.montar(LISTAR_SERVIDORES));
        if (lista && lista->status() == OK) {
            servidores_.clear();
            std::string nomes;
            for (const auto& s : lista->servidores()) {
                servidores_[s.nome()] = s;
                if (!nomes.empty()) {
                    nomes += ", ";
                }
                nomes += s.nome() + "(rank " + std::to_string(s.rank()) + ")";
            }
            std::cout << "[" << NOME << "] servidores disponíveis: " << nomes << std::endl;
        }
    }

    void batimento() {
        Envelope requisicao = mensageiro_.montar(BATIMENTO);
        requisicao.set_endereco(ENDERECO);
        auto resposta = referencia(requisicao);
        if (!resposta) {
            return;
        }
        if (resposta->status() != OK) {
            registrar();
        }
        atualizarServidores();
    }

    void eleicao() {
        precisaEleicao_ = false;
        std::cout << "[" << NOME << "] iniciando eleição (rank " << rank_ << ")" << std::endl;
        atualizarServidores();
        std::vector<InfoServidor> superiores;
        for (const auto& [nome, s] : servidores_) {
            if (s.rank() > rank_) {
                superiores.push_back(s);
            }
        }
        std::sort(superiores.begin(), superiores.end(),
                  [](const InfoServidor& a, const InfoServidor& b) { return a.rank() > b.rank(); });
        for (const auto& s : superiores) {
            Envelope requisicao = mensageiro_.montar(ELEICAO);
            requisicao.set_rank(rank_);
            auto resposta = pedir(s.endereco(), requisicao, TEMPO_LIMITE_SERVIDOR);
            if (resposta && resposta->status() == OK) {
                std::cout << "[" << NOME << "] " << s.nome() << " (rank " << s.rank()
                          << ") respondeu OK: aguardando o anúncio" << std::endl;
                coordenador_.clear();
                proximaEleicao_ = agoraMs() + ESPERA_ANUNCIO;
                return;
            }
        }
        virarCoordenador();
    }

    void virarCoordenador() {
        coordenador_ = NOME;
        Envelope anuncio = mensageiro_.montar(COORDENADOR);
        anuncio.set_coordenador(NOME);
        anuncio.set_rank(rank_);
        publicar(TOPICO_SERVIDORES, anuncio);
        mensageiro_.exibir("ENVIA", anuncio, "#" + TOPICO_SERVIDORES);
        std::cout << "[" << NOME << "] eleito coordenador" << std::endl;
    }

    void sincronizarRelogio() {
        if (coordenador_.empty()) {
            return;
        }
        if (coordenador_ == NOME) {
            std::cout << "[" << NOME << "] sou o coordenador: meu relógio é a referência" << std::endl;
            return;
        }
        if (servidores_.count(coordenador_) == 0) {
            atualizarServidores();
        }
        std::optional<Envelope> resposta;
        double t0 = 0;
        double t1 = 0;
        auto it = servidores_.find(coordenador_);
        if (it != servidores_.end()) {
            std::string endereco = it->second.endereco();
            t0 = relogio_.bruto();
            resposta = pedir(endereco, mensageiro_.montar(RELOGIO), TEMPO_LIMITE_SERVIDOR);
            t1 = relogio_.bruto();
        }
        if (!resposta || resposta->status() != OK) {
            std::cout << "[" << NOME << "] coordenador " << coordenador_ << " indisponível: pedindo eleição"
                      << std::endl;
            coordenador_.clear();
            precisaEleicao_ = true;
            return;
        }
        double correcao = relogio_.sincronizar(resposta->hora(), t0, t1);
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%+.3f", correcao);
        std::cout << "[" << NOME << "] relógio ajustado pelo coordenador " << coordenador_ << ": correção de "
                  << buf << "s" << std::endl;
    }

    Envelope erro(Tipo tipo, const std::string& texto) {
        Envelope e = mensageiro_.montar(tipo);
        e.set_status(ERRO);
        e.set_erro(texto);
        return e;
    }

    void atenderPar() {
        auto dados = receber(par_, false);
        Envelope envelope;
        if (!dados || !mensageiro_.ler(*dados, &envelope)) {
            enviar(par_, erro(DESCONHECIDO, "mensagem inválida").SerializeAsString());
            return;
        }
        mensageiro_.exibir("RECEBE", envelope, envelope.remetente());
        Envelope resposta;
        switch (envelope.tipo()) {
            case ELEICAO:
                resposta = mensageiro_.montar(envelope.tipo());
                resposta.set_status(OK);
                resposta.set_rank(rank_);
                precisaEleicao_ = true;
                break;
            case SINCRONIZAR:
                resposta = mensageiro_.montar(envelope.tipo());
                resposta.set_status(OK);
                for (const auto& e : armazenamento_.entradas()) {
                    *resposta.add_entradas() = e.paraProto();
                }
                break;
            case RELOGIO:
                if (coordenador_ == NOME) {
                    resposta = mensageiro_.montar(envelope.tipo());
                    resposta.set_status(OK);
                    resposta.set_hora(relogio_.agora());
                } else {
                    resposta = erro(envelope.tipo(), "este servidor não é o coordenador");
                }
                break;
            default:
                resposta = erro(envelope.tipo(), "tipo de mensagem desconhecido");
        }
        enviar(par_, resposta.SerializeAsString());
        mensageiro_.exibir("ENVIA", resposta, envelope.remetente());
    }

    void receberDoSub() {
        auto topico = receber(assinante_, false);
        std::optional<std::string> dados;
        while (assinante_.get(zmq::sockopt::rcvmore)) {
            dados = receber(assinante_, false);
        }
        if (!topico || !dados) {
            return;
        }
        if (*topico != TOPICO_SERVIDORES && *topico != TOPICO_REPLICA) {
            return;
        }
        Envelope envelope;
        if (!mensageiro_.ler(*dados, &envelope)) {
            return;
        }
        if (envelope.remetente() == NOME) {
            return;
        }
        mensageiro_.exibir("RECEBE", envelope, "#" + *topico);
        if (*topico == TOPICO_REPLICA) {
            aplicarEntradas(envelope.entradas(), envelope.remetente());
            return;
        }
        coordenador_ = envelope.coordenador();
        std::cout << "[" << NOME << "] coordenador agora é " << coordenador_ << std::endl;
        if (rank_ != 0 && envelope.rank() < rank_) {
            precisaEleicao_ = true;
        }
    }

    void gravar(const Entrada& entrada) {
        if (armazenamento_.adicionar(entrada)) {
            Envelope mensagem = mensageiro_.montar(REPLICAR);
            *mensagem.add_entradas() = entrada.paraProto();
            publicar(TOPICO_REPLICA, mensagem);
            mensageiro_.exibir("ENVIA", mensagem, "#" + TOPICO_REPLICA);
        }
    }

    template <typename Colecao>
    int aplicarEntradas(const Colecao& entradas, const std::string& origem) {
        int novas = 0;
        for (const auto& le : entradas) {
            if (armazenamento_.adicionar(Entrada::deProto(le))) {
                novas++;
            }
        }
        if (novas > 0) {
            std::cout << "[" << NOME << "] replicação: " << novas << " entrada(s) nova(s) recebida(s) de "
                      << origem << std::endl;
        }
        return novas;
    }

    void puxarDe(const InfoServidor& info) {
        auto resposta = pedir(info.endereco(), mensageiro_.montar(SINCRONIZAR), TEMPO_LIMITE_SYNC);
        if (resposta && resposta->status() == OK) {
            aplicarEntradas(resposta->entradas(), info.nome());
        }
    }

    void antiEntropia(bool todos) {
        proximaSync_ = agoraMs() + ANTI_ENTROPIA;
        atualizarServidores();
        std::vector<InfoServidor> outros;
        for (const auto& [nome, s] : servidores_) {
            if (nome != NOME) {
                outros.push_back(s);
            }
        }
        if (!todos && !outros.empty()) {
            std::uniform_int_distribution<size_t> d(0, outros.size() - 1);
            InfoServidor escolhido = outros[d(sorteio_)];
            outros = {escolhido};
        }
        for (const auto& info : outros) {
            puxarDe(info);
        }
    }

    Entrada evento(const std::string& tipo, const std::string& usuario, const std::string& canal,
                 const std::string& texto) {
        return armazenamento_.novaEntrada(tipo, relogio_.agora(), mensageiro_.relogio(), usuario, canal, texto);
    }

    Envelope tratar(const Envelope& envelope) {
        switch (envelope.tipo()) {
            case LOGIN: {
                if (!util::nomeValido(envelope.usuario())) {
                    return erro(envelope.tipo(), "nome de usuário inválido (use 1 a 32 letras, números, _ ou -)");
                }
                gravar(evento("login", envelope.usuario(), "", ""));
                Envelope r = mensageiro_.montar(envelope.tipo());
                r.set_status(OK);
                r.set_usuario(envelope.usuario());
                return r;
            }
            case CRIAR_CANAL: {
                if (!util::nomeValido(envelope.canal())) {
                    return erro(envelope.tipo(), "nome de canal inválido (use 1 a 32 letras, números, _ ou -)");
                }
                if (reservado(envelope.canal())) {
                    return erro(envelope.tipo(), "'" + envelope.canal() + "' é um nome reservado");
                }
                if (armazenamento_.temCanal(envelope.canal())) {
                    return erro(envelope.tipo(), "o canal '" + envelope.canal() + "' já existe");
                }
                gravar(evento("channel", envelope.remetente(), envelope.canal(), ""));
                Envelope r = mensageiro_.montar(envelope.tipo());
                r.set_status(OK);
                r.set_canal(envelope.canal());
                return r;
            }
            case PUBLICAR: {
                if (!util::nomeValido(envelope.canal())) {
                    return erro(envelope.tipo(), "nome de canal inválido");
                }
                if (reservado(envelope.canal())) {
                    return erro(envelope.tipo(), "'" + envelope.canal() + "' é um nome reservado");
                }
                if (envelope.texto().empty()) {
                    return erro(envelope.tipo(), "mensagem vazia");
                }
                Envelope publicacao = mensageiro_.montar(PUBLICACAO);
                publicacao.set_usuario(envelope.remetente());
                publicacao.set_canal(envelope.canal());
                publicacao.set_texto(envelope.texto());
                gravar(armazenamento_.novaEntrada("message", publicacao.instante(), publicacao.relogio(),
                                                  envelope.remetente(), envelope.canal(), envelope.texto()));
                publicar(envelope.canal(), publicacao);
                mensageiro_.exibir("ENVIA", publicacao, "#" + envelope.canal());
                Envelope r = mensageiro_.montar(envelope.tipo());
                r.set_status(OK);
                r.set_canal(envelope.canal());
                return r;
            }
            case LISTAR_CANAIS: {
                Envelope r = mensageiro_.montar(envelope.tipo());
                r.set_status(OK);
                for (const auto& c : armazenamento_.nomesCanais()) {
                    r.add_canais(c);
                }
                return r;
            }
            default:
                return erro(envelope.tipo(), "tipo de mensagem desconhecido");
        }
    }

    void atenderCliente() {
        auto dados = receber(atendimento_, false);
        Envelope envelope;
        if (!dados || !mensageiro_.ler(*dados, &envelope)) {
            Envelope resposta = erro(DESCONHECIDO, "mensagem inválida (não é um Envelope Protobuf)");
            enviar(atendimento_, resposta.SerializeAsString());
            mensageiro_.exibir("ENVIA", resposta, "");
            return;
        }
        mensageiro_.exibir("RECEBE", envelope, envelope.remetente());
        Envelope resposta = tratar(envelope);
        enviar(atendimento_, resposta.SerializeAsString());
        mensageiro_.exibir("ENVIA", resposta, envelope.remetente());
        mensagensRecebidas_++;
        if (mensagensRecebidas_ % BATIMENTO_A_CADA == 0) {
            batimento();
        }
        if (mensagensRecebidas_ % SINCRONIZA_A_CADA == 0) {
            sincronizarRelogio();
        }
    }

    RelogioFisico relogio_;
    Mensageiro mensageiro_;
    Armazenamento armazenamento_;
    zmq::context_t contexto_;
    zmq::socket_t atendimento_;
    zmq::socket_t par_;
    zmq::socket_t publicador_;
    zmq::socket_t assinante_;
    int rank_ = 0;
    long long mensagensRecebidas_ = 0;
    std::string coordenador_;
    std::map<std::string, InfoServidor> servidores_;
    bool precisaEleicao_ = false;
    long long proximaEleicao_ = 0;
    long long proximaSync_;
    std::mt19937 sorteio_;
};

}  // namespace

int main() {
    Servidor servidor;
    servidor.executar();
    return 0;
}
