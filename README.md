# BBS Marvel — CC7261 / Sistemas Distribuídos

Versão simples de um sistema no estilo BBS/IRC: usuários (bots) fazem login em servidores, criam canais e publicam mensagens nos canais. Os processos têm relógios lógicos, os servidores elegem um coordenador que fornece a hora correta e todos os servidores mantêm uma **réplica completa dos dados**. Tudo roda em containers, orquestrados pelo Docker Compose, e a troca de mensagens usa **ZeroMQ**.

**Integrantes:**

- Gabriel Afonso dos Santos
- Giovane Contreras Oba
- Gustavo Trovó Ramos de Souza
- Igor de Araujo Borges


## Nomes dos servidores

Os personagens identificam os processos; as implementações continuam nas quatro linguagens.
Os nomes também são os endereços dos serviços na rede do Compose.

| Serviço | Linguagem |
|---|---|
| `homem-de-ferro-python-1` | Python |
| `maquina-de-combate-python-2` | Python |
| `capitao-america-java-1` | Java |
| `soldado-invernal-java-2` | Java |
| `viuva-negra-csharp-1` | C# |
| `gaviao-arqueiro-csharp-2` | C# |
| `visao-cpp-1` | C++ |
| `feiticeira-escarlate-cpp-2` | C++ |

Esta cópia usa o projeto Compose `bbs-marvel` e grava os dados em sua própria pasta `data/`.
O histórico da pasta antiga não foi copiado. Veja `ROTEIRO.txt` para a demonstração e
`REVISAO.txt` para o resultado da revisão e as limitações conhecidas.

## Como executar

```bash
docker compose up --build
```

Sobe o broker, o proxy do Pub/Sub, o serviço de referência e, para cada uma das 4 linguagens (**Python**, **Java**, **C#** e **C++**), 2 servidores e 2 clientes (bots): 8 servidores e 8 clientes ao todo. Não há interação com o usuário: os bots agem sozinhos e cada processo exibe no terminal todas as mensagens que envia e recebe. Os dados dos servidores ficam em `./data/<servidor>/`.

A primeira execução demora porque compila as imagens (Java, C# e C++ são os mais lentos). Requisitos: Docker com o plugin Compose e acesso à internet para baixar as dependências.

Comandos úteis:

```bash
docker compose logs -f visao-cpp-1
docker compose stop homem-de-ferro-python-1
docker compose start homem-de-ferro-python-1
docker compose down
```

Para começar do zero, apague a pasta `data/` depois do `docker compose down`.

### Passo a passo da demonstração

Requisitos: **Docker Desktop** aberto e a pasta do projeto (onde está o `docker-compose.yaml`) aberta no terminal. Rode cada bloco em um terminal separado e espere o anterior terminar.

**1. Professor (recebe)**

```bash
docker compose up --build -d
docker compose stop client-python-1 client-python-2 client-java-1 client-java-2 client-csharp-1 client-csharp-2 client-cpp-1 client-cpp-2
docker compose run --rm --no-deps -v "${PWD}/demonstracao.py:/app/demonstracao.py:ro" broker python -u demonstracao.py receber --nome professor --todos
```

Espere aparecer `PRONTO PARA RECEBER` e deixe este terminal aberto.

**2. Gabriel (envia)**

```bash
docker compose run --rm --no-deps -v "${PWD}/demonstracao.py:/app/demonstracao.py:ro" broker python -u demonstracao.py enviar --nome gabriel
```

Quando aparecer `gabriel>`, digite a mensagem e pressione Enter. A mensagem aparece no terminal do Professor. Em `gabriel>`, `/sair` encerra.

**3. Logs dos servidores (um terminal para cada linha)**

```bash
docker compose logs --tail 10 -f homem-de-ferro-python-1 maquina-de-combate-python-2
docker compose logs --tail 10 -f viuva-negra-csharp-1 gaviao-arqueiro-csharp-2
docker compose logs --tail 10 -f capitao-america-java-1 soldado-invernal-java-2
docker compose logs --tail 10 -f visao-cpp-1 feiticeira-escarlate-cpp-2
```

**4. Relógio, heartbeat e coordenador**

Liga os bots (geram mensagens automaticamente) e acompanha os registros:

```bash
docker compose start client-python-1 client-python-2 client-java-1 client-java-2 client-csharp-1 client-csharp-2 client-cpp-1 client-cpp-2
docker compose logs --tail 10 -f | Select-String -Pattern 'RELOGIO|BATIMENTO|coordenador|corre'
```

O segundo comando usa `Select-String`, do PowerShell. Para parar os bots e voltar a enviar só pelo Gabriel:

```bash
docker compose stop client-python-1 client-python-2 client-java-1 client-java-2 client-csharp-1 client-csharp-2 client-cpp-1 client-cpp-2
```

**Para encerrar tudo**

No terminal do Gabriel digite `/sair`; nos terminais de logs e do Professor pressione `Ctrl+C`. Depois rode:

```bash
docker compose down --remove-orphans
```

## Escolhas feitas

| Item | Escolha | Motivo |
|---|---|---|
| Linguagens | **Python**, **Java**, **C#** e **C++** (uma por integrante) | O grupo tem 4 integrantes: cada um implementa cliente e servidor em uma linguagem diferente, e todos conversam entre si. O broker, o proxy e o serviço de referência são em Python |
| Troca de mensagens | **ZeroMQ** (`pyzmq`, `JeroMQ`, `NetMQ` e `libzmq`/`cppzmq`) | Exigido pelo enunciado |
| Serialização | **Protocol Buffers (Protobuf)**, com um único arquivo `proto/chat.proto` usado pelas quatro linguagens (`protobuf`, `protobuf-java`, `Google.Protobuf` e `libprotobuf`) | Formato binário visto em aula. As mensagens nunca são enviadas em JSON, XML ou texto simples |
| Persistência | **JSON Lines** (um evento por linha) em `data/<servidor>/<servidor>.jsonl` | Simples, legível e recuperável: ao reiniciar, o servidor relê o arquivo e reconstrói o estado |
| Imagens Docker | `python:3.13-slim`, `eclipse-temurin:21-jre`, `dotnet/runtime:8.0` e `ubuntu:24.04` | Python usa binários prontos do `pyzmq`. Java (JeroMQ) e C# (NetMQ) usam implementações do ZeroMQ escritas na própria linguagem. C++ usa a `libzmq` do sistema. Java, C# e C++ são compilados em um estágio de build e só o resultado vai para a imagem final |

### Formato da mensagem

Toda mensagem é um `Envelope` Protobuf (`proto/chat.proto`):

| Campo | Significado |
|---|---|
| `tipo` | `LOGIN`, `LISTAR_CANAIS`, `CRIAR_CANAL`, `PUBLICAR` (pedido de publicação), `PUBLICACAO` (mensagem publicada no canal), `OBTER_RANK`, `LISTAR_SERVIDORES` e `BATIMENTO` (servidor ⇄ referência), `ELEICAO`, `RELOGIO` e `COORDENADOR` (entre servidores) e `REPLICAR` e `SINCRONIZAR` (replicação) |
| `remetente` | nome de quem enviou |
| `instante` | instante do envio, pelo relógio físico do remetente (**obrigatório em todas as mensagens**) |
| `relogio` | valor do relógio lógico (Lamport) de quem envia |
| `status` / `erro` | `OK` ou `ERRO` e a descrição do erro (nas respostas) |
| `usuario`, `canal`, `canais`, `texto` | dados da requisição/resposta e texto da mensagem |
| `rank`, `servidores`, `endereco` | rank do servidor, lista de servidores (`ServerInfo`: nome, rank e endereço) e endereço do servidor, usados com o serviço de referência |
| `hora`, `coordenador` | hora correta informada pelo coordenador e nome do coordenador eleito |
| `entradas` | entradas replicadas (`LogEntry`: id, origem, tipo, instante, relógio lógico, usuário, canal, texto) |

### Arquitetura

```
  Request-Reply:  clientes (REQ) ──► broker ROUTER :5555 ═ DEALER :5556 ──► servidores (REP)
  Pub-Sub:        servidores (PUB) ──► proxy XSUB :5557 ═ XPUB :5558 ──► clientes (SUB)
  Referência:     servidores (REQ) ──► serviço de referência (REP :5559)
  Entre servidores: REQ/REP direto (porta 6000 de cada servidor) + tópicos `servers` e `replica` no Pub-Sub
```

Os clientes usam `REQ` e os servidores `REP`. O broker (`python/broker.py`) é um `ROUTER`/`DEALER`: o `DEALER` distribui as requisições entre os servidores conectados em **round-robin**. Assim é possível adicionar ou remover servidores sem mexer nos clientes. O proxy (`python/proxy.py`) é separado do broker e usa `XSUB` (porta 5557, onde os servidores publicam) e `XPUB` (porta 5558, onde os clientes se inscrevem).

### Login, erros e repetição

O bot envia `LOGIN` (apenas uma mensagem) e o servidor responde `OK` ou `ERRO`. O nome do usuário precisa ter de 1 a 32 caracteres entre letras, números, `_` e `-`. Em caso de erro o bot **repete o login** (com outro nome). Não há senha, só o nome do usuário.

### Canais

- `CRIAR_CANAL`: o servidor responde `ERRO` se o nome for inválido (mesma regra do usuário) ou se o canal já existir **naquele servidor**.
- `LISTAR_CANAIS`: o servidor devolve a lista com os nomes dos canais que conhece.
- Critério do bot: se a lista tiver menos de 5 canais, ele cria um canal com nome `canal-NNNN`.

### Publicação em canais (Parte 2)

```
Usuário ->> Servidor: REQ  PUBLICAR (canal, texto)
Servidor ->> Canal:   PUB  PUBLICACAO  (tópico = nome do canal)
Servidor ->> Usuário: REP  OK/ERRO
```

- O **nome do canal é o tópico** da mensagem PUB. A mensagem tem 2 frames: o tópico (nome do canal) e o `Envelope` serializado.
- Tanto o pedido (`PUBLICAR`) quanto a publicação (`PUBLICACAO`) levam o timestamp do envio; a publicação tem o seu próprio timestamp, do instante em que o servidor publica.
- O servidor responde `ERRO` se o nome do canal for inválido ou se o texto for vazio. O canal pode ter sido criado em **outro** servidor e a replicação é eventual (pode ainda não ter chegado), então o servidor não rejeita canais que não conhece.
- **Inscrição:** é feita só no cliente (`SUB` no XPUB do proxy), que pode se inscrever em vários tópicos na mesma conexão. O filtro do ZeroMQ casa o *início* do tópico (`canal-1` também receberia `canal-12`), então o cliente confere se o tópico é exatamente igual a um canal em que está inscrito. Cada publicação recebida é exibida com o canal, a mensagem, o timestamp do envio e o timestamp do recebimento.

### Funcionamento padronizado dos bots

Em loop infinito, a cada rodada o bot:
1. lista os canais; se existirem menos de 5, cria um novo;
2. se estiver inscrito em menos de 3 canais, se inscreve em mais um, escolhido aleatoriamente;
3. escolhe um canal da lista e envia 10 mensagens aleatórias, com 1 s de intervalo entre elas (enquanto espera, continua exibindo as publicações recebidas).

O enunciado descreve os passos 1 e 2 "ao conectar". Como cada passo cria/inscreve em **um** canal, repetimos a verificação a cada rodada para o bot chegar a 5 canais e 3 inscrições.

### Relógio lógico (Parte 3)

Clientes, servidores e o serviço de referência têm um contador (Lamport) que começa em 0 junto com o processo:
1. **antes de enviar** qualquer mensagem o contador é incrementado e o valor vai no campo `relogio`;
2. **ao receber**, o contador passa a ser `max(valor local, valor da mensagem)`.

Isso está em `Mensageiro.montar()` / `Mensageiro.ler()` (`python/common.py`, `java/.../Mensageiro.java`, `csharp/Util.cs` e `cpp/Common.hpp`), então vale para todas as mensagens. O valor do contador aparece nos logs como `[nome|L=...]`. O relógio lógico também é gravado em cada evento persistido (campo `relogio`).

### Serviço de referência e heartbeat (Partes 3 e 4)

Processo separado (`python/reference.py`, porta 5559) que só conversa com os servidores:

| Pedido (REQ) | Resposta (REP) |
|---|---|
| `OBTER_RANK` (o servidor se identifica pelo `remetente` e informa o seu `endereco`) | `rank` do servidor. O nome é cadastrado sem repetição e o rank de um nome nunca muda (1, 2, 3... na ordem de cadastro) |
| `LISTAR_SERVIDORES` | nome, rank e endereço de todos os servidores disponíveis |
| `BATIMENTO` | `OK` |

- **Heartbeat:** cada servidor envia um `BATIMENTO` a cada **10 mensagens de clientes recebidas**. A referência remove da lista o servidor que ficar mais de 30 s sem enviar (`HEARTBEAT_TIMEOUT`); se ele voltar a enviar, volta para a lista com o mesmo rank. Se a referência não conhecer o servidor (por exemplo, se ela reiniciou), responde `ERRO` e o servidor pede o rank de novo. Depois de cada heartbeat o servidor pede `LISTAR_SERVIDORES` e guarda a lista.
- Na Parte 3 a resposta do heartbeat trazia a hora correta. **Na Parte 4 a referência não devolve mais a hora**: quem fornece a hora é o coordenador.

### Eleição do coordenador e sincronização do relógio (Parte 4)

Cada servidor guarda em `coordenador` o nome do servidor eleito.

**Eleição (algoritmo do valentão):** o rank vem da referência.
1. O servidor pede a lista de servidores e envia `ELEICAO` (REQ) para os de **rank maior**, do maior para o menor, direto no endereço deles (porta 6000, `PEER_PORT`).
2. Se algum responder `OK`, ele "assume" a eleição: quem recebe um `ELEICAO` responde `OK` e inicia a sua própria eleição. O servidor que perguntou fica esperando o anúncio (até 5 s; depois tenta de novo).
3. Se nenhum de rank maior responder, o servidor vence: passa a se considerar coordenador e **publica** `COORDENADOR` (com o seu nome) no tópico `servers` do Pub-Sub. Todos os servidores assinam o tópico e atualizam a variável `coordenador`.
4. A eleição acontece quando o servidor inicia (ainda sem coordenador), quando recebe um `ELEICAO` de outro servidor, quando o coordenador não responde, ou quando chega o anúncio de um coordenador com rank menor que o dele.

`servers` (e `replica`, da Parte 5) são nomes reservados: nenhum usuário pode criar ou publicar em um canal com esse nome.

**Sincronização do relógio:** a cada **15 mensagens de clientes recebidas** o servidor que não é coordenador faz:

```
Servidor ->> Coordenador: REQ: RELOGIO
Coordenador ->> Servidor: REP: hora correta
```

O servidor estima a hora atual como `hora_do_coordenador + (t1 - t0) / 2` (com `t0` e `t1` as horas locais do envio e do recebimento, para descontar o tempo da mensagem) e aplica a diferença como correção ao seu relógio. A implementação segue a consulta ao coordenador mostrada no diagrama do enunciado: o coordenador é a referência de hora e os outros ajustam o relógio com a hora dele, descontando a latência (não há média entre os relógios). Portanto, esta implementação não realiza a etapa de média do Berkeley completo; é um ponto de atenção para a exigência da Parte 4. Se o coordenador não responde (1 s), o servidor considera que ele caiu e pede eleição.

- **Só para teste:** a variável `CLOCK_SKEW` (no `docker-compose.yaml`) faz o relógio de cada servidor andar adiantado ou atrasado alguns segundos, para o ajuste aparecer nos logs (`relógio ajustado pelo coordenador gaviao-arqueiro-csharp-2: correção de -5.000s`; o valor é a correção total aplicada sobre o relógio local). Com `CLOCK_SKEW=0` (padrão) o relógio é o do sistema.

### Persistência

Cada servidor tem o **seu próprio** arquivo (não é compartilhado entre os servidores; a cópia dos dados é feita pela replicação da Parte 5). Cada evento tem `id` único, `origin` (servidor que o criou), `kind` (`login`, `channel` ou `message`), `ts` e os dados (`user`, `channel`, `text`). Ficam registrados todos os logins (com o timestamp), todos os canais criados e **todas as mensagens publicadas** (autor, canal, texto e timestamp). Exemplo de linha:

```json
{"id": "79206a11...", "origin": "homem-de-ferro-python-1", "kind": "message", "ts": 1793673838.53, "user": "js-bot-2", "channel": "canal-6462", "text": "alguém aí?"}
```

Escolhemos um log de eventos (só acrescenta no fim) porque qualquer informação pode ser recuperada relendo o arquivo.

### Timeout do cliente

Se o serviço não responder em 5 s, o cliente recria o socket `REQ` e reenvia a requisição (o `REQ` do ZeroMQ fica preso esperando resposta quando o servidor cai no meio da requisição).

## Replicação dos dados (Parte 5)

### O problema

O broker distribui os pedidos em round-robin, então cada servidor recebia só uma parte dos logins, canais e mensagens. Se um servidor parasse, essa parte se perdia, e um cliente que pedisse o histórico a um servidor receberia só o que estava guardado nele. No projeto, **todos os servidores devem ter todos os dados**.

### Método escolhido: consistência eventual com replicação ativa (push) e pull

Das formas vistas na aula, usamos a **consistência eventual**: se não houver novas escritas, todas as atualizações acabam propagadas para todas as réplicas e elas ficam iguais. Para distribuir o conteúdo usamos:

- **réplica ativa por push:** quem recebe a escrita do cliente propaga a *operação* (a nova entrada) para os outros servidores, sem esperar que eles peçam;
- **pull:** um servidor pede ao outro os dados que faltam (quando entra no sistema e de tempos em tempos).

Por que resolve o problema: depois de cada escrita, os servidores ativos recebem a entrada por push ou a recuperam por pull. Após a propagação, cada réplica pode responder ao histórico completo. Uma falha antes de o evento alcançar outra réplica não tem garantia de recuperação apenas pela replicação. Escolhemos o modelo eventual, e não um modelo forte (sequencial/linearizável), porque os dados são só acréscimos (logins, canais e mensagens nunca são alterados): não há escritas conflitantes para ordenar, e evitar a sincronização global deixa o sistema mais simples e rápido.

### Como foi implementado

1. **Unidade de replicação:** cada evento é uma entrada do log (`LogEntry`) com `id` único (UUID), servidor de origem, tipo (`login`, `channel` ou `message`), timestamp e relógio lógico. Replicamos **logins, canais e mensagens**, ou seja, todos os dados.
2. **Push:** depois de gravar em disco uma escrita de cliente, o servidor publica uma mensagem `REPLICAR` com a entrada no tópico `replica` do Pub-Sub (o mesmo proxy). Todos os servidores assinam o tópico, ignoram o que eles mesmos publicaram e gravam as entradas novas. O cliente recebe a resposta sem esperar a replicação.
3. **Pull (anti-entropia):** `SINCRONIZAR` é um REQ/REP direto entre servidores (porta 6000, o mesmo canal da eleição). O servidor responde com todas as suas entradas e quem pediu grava as que não tinha. O pull acontece **ao iniciar** (de todos os servidores disponíveis) e **a cada 20 s** (de um servidor sorteado).
4. **Sem duplicatas:** gravar é idempotente, pois uma entrada com `id` já conhecido é descartada. Por isso o push e o pull podem entregar a mesma entrada sem problema.
5. **Ordem:** as mensagens de um canal são ordenadas por (relógio lógico, servidor de origem, id), a mesma ordem em todas as réplicas.
6. **Servidor que cai e volta, ou servidor novo:** ao iniciar, ele lê o próprio arquivo e depois faz o pull dos outros, recuperando tudo o que perdeu. A recuperação deve ser verificada parando e reiniciando um servidor durante as escritas e comparando os históricos após a sincronização. Esse cenário ainda precisa ser ensaiado nesta versão.

Cada arquivo `data/<servidor>/<servidor>.jsonl` passa a conter **todas** as entradas (o campo `origin` diz qual servidor recebeu a escrita do cliente).

### Mudanças em relação ao método da aula

- **Pull além do push.** O Pub-Sub não é confiável (como vimos: assinante que entra tarde, mensagem perdida, servidor que cai). Só o push não garantiria que todos os servidores ficassem com todos os dados, então acrescentamos o pull ao iniciar e periódico.
- **Escritas idempotentes com id único**, porque push e pull se sobrepõem.
- **Qualquer servidor aceita escritas** (não há réplica primária). Funciona porque as escritas são só acréscimos. Se dois servidores criarem o mesmo canal ao mesmo tempo, os dois respondem `OK` e depois da replicação existe um canal só (união pelo nome).
- **Transferimos o log inteiro no `SINCRONIZAR`.** É simples e suficiente para o volume deste projeto; um sistema real enviaria só o que falta.

## Estrutura

```
proto/chat.proto        formato das mensagens (Protobuf)
python/                 broker.py, proxy.py, reference.py, server.py, client.py, common.py, store.py (+ chat_pb2.py gerado)
java/                   src/main/java/bbs/Servidor.java, Cliente.java, Mensageiro.java, RelogioFisico.java, Armazenamento.java, Util.java, pom.xml
csharp/                 Servidor.cs, Cliente.cs, Util.cs (relógios e Mensageiro), Armazenamento.cs, Program.cs, Bbs.csproj
cpp/                    Servidor.cpp, Cliente.cpp, Common.hpp, Armazenamento.hpp
Dockerfile.python, Dockerfile.java, Dockerfile.csharp, Dockerfile.cpp, docker-compose.yaml
```

Java, C# e C++ geram o código do `.proto` na hora do build da imagem (`protoc` e `Grpc.Tools`), então não há arquivo gerado para essas linguagens no repositório. O `.proto` define `java_package`, `java_outer_classname` e `csharp_namespace` para o código gerado.

Para regerar `python/chat_pb2.py` após mudar o `.proto`:
`python -m grpc_tools.protoc -Iproto --python_out=python proto/chat.proto`

## Apresentação e bots identificados

Leia `COMO-RODAR.txt` para os sete terminais, na ordem de abertura.
Os oito bots agora têm serviços individuais e `BOT_NAME` fixo no Compose,
um por personagem/linguagem/instância. Continuam sendo dois clientes por linguagem.
As pastas de fontes mantêm os nomes das linguagens.
Para acompanhar todas as publicações no cliente de demonstração, use `--todos`.
Os comandos antigos `docker compose start client-py ...` foram substituídos
pelos nomes individuais listados no roteiro.
