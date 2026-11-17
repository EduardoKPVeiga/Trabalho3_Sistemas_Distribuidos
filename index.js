const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

// 1. IDENTIFICAÇÃO E ESTADO DO NÓ
const NODE_ID = process.argv[2] || 'node1'; // Ex: passado via terminal (node index.js node1)

let state = {
    role: 'FOLLOWER',      // 'FOLLOWER', 'CANDIDATE', 'LEADER'
    currentTerm: 0,
    votedFor: null,
    commitIndex: 0,
    log: []
};

// 2. TIMERS (Substituem qualquer loop while)
let electionTimer = null;
let fetchInterval = null;

function resetElectionTimeout() {
    clearTimeout(electionTimer);
    // Tempo aleatório (ex: entre 150ms e 300ms) para evitar empate de votos
    const timeout = Math.floor(Math.random() * 150) + 150;
    electionTimer = setTimeout(() => {
        startElection();
    }, timeout);
}

function startElection() {
    console.log(`[${NODE_ID}] Líder inativo. Virando CANDIDATO...`);
    state.role = 'CANDIDATE';
    state.currentTerm += 1;
    state.votedFor = NODE_ID;
    resetElectionTimeout();
    // Dispara RequestVote para os outros nós via stubs gRPC...
}

function startFetchLoop() {
    // A réplica consulta o líder a cada 100ms (modelo Pull / Heartbeat)
    fetchInterval = setInterval(() => {
        if (state.role === 'FOLLOWER') {
            // Chama FetchRequest no líder...
        }
    }, 100);
}

// 3. IMPLEMENTAÇÃO DOS HANDLERS gRPC (Callbacks de entrada)
const serviceImpl = {
    RequestVote: (call, callback) => {
        // Processa pedido de voto e responde
        callback(null, { term: state.currentTerm, vote_granted: false });
    },
    Fetch: (call, callback) => {
        // Líder recebe o heartbeat/pull do seguidor e responde novos logs
        callback(null, { term: state.currentTerm, success: true, entries: [], leader_commit_index: state.commitIndex });
    },
    Write: (call, callback) => {
        // Cliente tentando gravar
        if (state.role !== 'LEADER') {
            return callback(null, { success: false, message: 'Redirect', leader_id: 'nodeX' });
        }
        // Grava no log local e aguarda quórum via Fetch...
    },
    Read: (call, callback) => {
        // Cliente consultando (retorna apenas dados comitados)
        callback(null, { success: true, value: 'exemplo' });
    }
};

// 4. INICIALIZAÇÃO DO SERVIDOR gRPC
function main() {
    const packageDef = protoLoader.loadSync(path.join(__dirname, 'proto', 'cluster.proto'), {
        keepCase: true, longs: String, enums: String, defaults: true, oneofs: true
    });
    const proto = grpc.loadPackageDefinition(packageDef).cluster;
    const server = new grpc.Server();
    server.addService(proto.ClusterService.service, serviceImpl);
    const PORT = '50051'; // Cada nó terá sua porta
    server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), () => {
        console.log(`[${NODE_ID}] Rodando na porta ${PORT} como ${state.role}`);

        // Inicia o temporizador de eleição e o loop de fetch
        resetElectionTimeout();
        startFetchLoop();
    });
}

main();