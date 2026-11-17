const fs = require('fs');
const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const NODES = {
    node1: { id: 'node1', host: '127.0.0.1', port: 50051 },
    node2: { id: 'node2', host: '127.0.0.1', port: 50052 },
    node3: { id: 'node3', host: '127.0.0.1', port: 50053 },
    node4: { id: 'node4', host: '127.0.0.1', port: 50054 }
};

const NODE_ID = process.argv[2] || 'node1';

if (!NODES[NODE_ID]) {
    console.error(`Nó inválido: ${NODE_ID}. Escolha entre: ${Object.keys(NODES).join(', ')}`);
    process.exit(1);
}

const SELF = NODES[NODE_ID];
const PEERS = Object.values(NODES).filter(n => n.id !== NODE_ID);
const QUORUM = 3;
const HEARTBEAT_TIMEOUT_MS = 1000;

const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, `${NODE_ID}.json`);

function loadPersistedState() {
    if (fs.existsSync(DATA_FILE)) {
        try {
            const data = JSON.parse(fs.readFileSync(DATA_FILE, 'utf-8'));
            return {
                role: 'FOLLOWER',
                currentTerm: data.currentTerm || 0,
                votedFor: data.votedFor || null,
                commitIndex: data.commitIndex || 0,
                log: data.log || [],
                db: data.db || {}
            };
        } catch (err) {
            console.error(`Erro ao carregar estado em disco:`, err.message);
        }
    }
    return {
        role: 'FOLLOWER',
        currentTerm: 0,
        votedFor: null,
        commitIndex: 0,
        log: [],
        db: {}
    };
}

let state = loadPersistedState();
let currentLeaderId = null;

function saveState() {
    if (!fs.existsSync(DATA_DIR)) {
        fs.mkdirSync(DATA_DIR, { recursive: true });
    }
    const toSave = {
        currentTerm: state.currentTerm,
        votedFor: state.votedFor,
        commitIndex: state.commitIndex,
        log: state.log,
        db: state.db
    };
    fs.writeFileSync(DATA_FILE, JSON.stringify(toSave, null, 2), 'utf-8');
}

// Salva o estado inicial carregado
saveState();

function printStatus(eventMsg = '') {
    const leaderInfo = currentLeaderId ? `Líder: ${currentLeaderId}` : 'Sem líder';
    const logLen = `Logs: ${state.log.length}`;
    console.log(`[${NODE_ID}] Papel: ${state.role.padEnd(9)} | Termo: ${String(state.currentTerm).padEnd(2)} | Commit: ${String(state.commitIndex).padEnd(2)} | ${leaderInfo} | ${logLen} ${eventMsg ? '>> ' + eventMsg : ''}`);
}

let clusterProto = null;
const peerClients = {};

function getPeerClient(peerId) {
    if (!peerClients[peerId] && NODES[peerId]) {
        const peer = NODES[peerId];
        peerClients[peerId] = new clusterProto.ClusterService(
            `${peer.host}:${peer.port}`,
            grpc.credentials.createInsecure()
        );
    }
    return peerClients[peerId];
}

const lastFetchReceived = {};
const matchIndex = {};
const pendingWrites = new Map();

function getActiveNodesCount() {
    const now = Date.now();
    let count = 1;
    for (const peer of PEERS) {
        if (lastFetchReceived[peer.id] && (now - lastFetchReceived[peer.id] < HEARTBEAT_TIMEOUT_MS)) {
            count++;
        }
    }
    return count;
}

function hasQuorum() {
    return getActiveNodesCount() >= QUORUM;
}

function getLastLog() {
    if (state.log.length === 0) {
        return { index: 0, term: 0 };
    }
    const last = state.log[state.log.length - 1];
    return { index: last.index, term: last.term };
}

let electionTimer = null;
let fetchInterval = null;
let peerProbeIndex = 0;

function stepDown(newTerm, knownLeaderId = null) {
    state.currentTerm = newTerm;
    state.role = 'FOLLOWER';
    state.votedFor = null;
    currentLeaderId = knownLeaderId;
    saveState();
    resetElectionTimeout();
    printStatus(`Transitou para Seguidor (novo Termo ${newTerm})`);
}

function resetElectionTimeout() {
    clearTimeout(electionTimer);
    const timeout = Math.floor(Math.random() * 1200) + 1200;
    electionTimer = setTimeout(() => {
        if (state.role !== 'LEADER') {
            startElection();
        }
    }, timeout);
}

function becomeLeader() {
    state.role = 'LEADER';
    currentLeaderId = NODE_ID;
    clearTimeout(electionTimer);
    electionTimer = null;

    // Inicializa monitoramento de inatividade e matchIndex das réplicas
    const now = Date.now();
    for (const peer of PEERS) {
        lastFetchReceived[peer.id] = now;
        matchIndex[peer.id] = 0;
    }

    printStatus('*** ELEITO LÍDER DO CLUSTER ***');
}

function startElection() {
    state.role = 'CANDIDATE';
    state.currentTerm += 1;
    state.votedFor = NODE_ID;
    currentLeaderId = null;
    saveState();

    printStatus(`Iniciou eleição para o Termo ${state.currentTerm}`);
    resetElectionTimeout();

    let votes = 1;
    const lastLog = getLastLog();

    const req = {
        term: state.currentTerm,
        candidate_id: NODE_ID,
        last_log_index: lastLog.index,
        last_log_term: lastLog.term
    };

    for (const peer of PEERS) {
        const client = getPeerClient(peer.id);
        if (!client) continue;

        client.RequestVote(req, { deadline: Date.now() + 400 }, (err, res) => {
            if (err) return; // Nó inativo ou desligado
            if (state.role !== 'CANDIDATE') return;

            if (res.term > state.currentTerm) {
                stepDown(res.term);
                return;
            }

            if (res.vote_granted && res.term === state.currentTerm) {
                votes++;
                printStatus(`Recebeu voto de ${peer.id} (${votes}/${QUORUM})`);

                if (votes >= QUORUM && state.role === 'CANDIDATE') {
                    becomeLeader();
                }
            }
        });
    }
}

function startFetchLoop() {
    fetchInterval = setInterval(() => {
        if (state.role !== 'FOLLOWER') return;

        // Se conhece o líder, faz Fetch nele; caso contrário, sonda os pares em round-robin
        const targetId = (currentLeaderId && currentLeaderId !== NODE_ID)
            ? currentLeaderId
            : PEERS[peerProbeIndex++ % PEERS.length].id;

        const leaderClient = getPeerClient(targetId);
        if (!leaderClient) return;

        const lastLog = getLastLog();
        const req = {
            replica_id: NODE_ID,
            term: state.currentTerm,
            last_log_index: lastLog.index,
            last_log_term: lastLog.term
        };

        leaderClient.Fetch(req, { deadline: Date.now() + 250 }, (err, res) => {
            if (err) {
                if (currentLeaderId === targetId) {
                    currentLeaderId = null;
                }
                return;
            }

            if (state.role !== 'FOLLOWER') return;

            if (res.term > state.currentTerm) {
                stepDown(res.term, res.leader_id || null);
            }

            const isFromLeader = (res.leader_id === targetId);
            if (!isFromLeader) {
                currentLeaderId = (res.leader_id && res.leader_id !== NODE_ID) ? res.leader_id : null;
                return;
            }

            resetElectionTimeout();
            if (currentLeaderId !== targetId) {
                currentLeaderId = targetId;
                printStatus(`Reconheceu ${currentLeaderId} como Líder atual`);
            }

            if (!res.success) {
                if (res.conflict_index > 0 && state.log.length >= res.conflict_index) {
                    printStatus(`Divergência detectada no índice ${res.conflict_index}. Truncando log conflitante...`);
                    state.log = state.log.slice(0, res.conflict_index - 1);
                    saveState();
                }
                return;
            }

            let updated = false;

            if (res.entries && res.entries.length > 0) {
                for (const e of res.entries) {
                    const entry = {
                        index: Number(e.index),
                        term: Number(e.term),
                        key: e.key,
                        value: e.value
                    };
                    if (!state.log.some(local => local.index === entry.index)) {
                        state.log.push(entry);
                        updated = true;
                    }
                }
            }

            const targetCommit = Number(res.leader_commit_index);
            if (targetCommit > state.commitIndex) {
                const newCommit = Math.min(targetCommit, state.log.length);
                while (state.commitIndex < newCommit) {
                    state.commitIndex++;
                    const entry = state.log[state.commitIndex - 1];
                    if (entry) {
                        state.db[entry.key] = entry.value;
                    }
                }
                updated = true;
                printStatus(`Efetivou (commit) dados locais até índice ${state.commitIndex}`);
            }

            if (updated) {
                saveState();
            }
        });
    }, 100);
}

function checkAndAdvanceCommitIndex() {
    if (state.role !== 'LEADER') return;

    for (let idx = state.commitIndex + 1; idx <= state.log.length; idx++) {
        const entry = state.log[idx - 1];
        if (entry.term !== state.currentTerm) continue;

        let count = 1;
        for (const peer of PEERS) {
            if ((matchIndex[peer.id] || 0) >= idx) {
                count++;
            }
        }

        if (count >= QUORUM) {
            state.commitIndex = idx;
            state.db[entry.key] = entry.value;
            saveState();
            printStatus(`Índice de Commit avançou para ${idx} (Quórum ${count}/3 confirmado)`);

            if (pendingWrites.has(idx)) {
                const resolveWrite = pendingWrites.get(idx);
                pendingWrites.delete(idx);
                resolveWrite({
                    success: true,
                    message: 'Escrita replicada e comitada com sucesso no cluster',
                    leader_id: NODE_ID,
                    leader_address: `${SELF.host}:${SELF.port}`,
                    commit_index: idx
                });
            }
        }
    }
}

const serviceImpl = {
    // Eleição de Líder
    RequestVote: (call, callback) => {
        const req = call.request;

        if (req.term > state.currentTerm) {
            stepDown(req.term);
        }

        let voteGranted = false;

        if (req.term === state.currentTerm) {
            const canVote = (state.votedFor === null || state.votedFor === req.candidate_id);

            const localLastLog = getLastLog();
            const candidateUpToDate =
                (req.last_log_term > localLastLog.term) ||
                (req.last_log_term === localLastLog.term && req.last_log_index >= localLastLog.index);

            if (canVote && candidateUpToDate) {
                voteGranted = true;
                state.votedFor = req.candidate_id;
                currentLeaderId = req.candidate_id;
                saveState();
                resetElectionTimeout();
                printStatus(`Concedeu voto para ${req.candidate_id} no Termo ${state.currentTerm}`);
            }
        }

        callback(null, {
            term: state.currentTerm,
            vote_granted: voteGranted
        });
    },

    // Replicação Pull e Heartbeat
    Fetch: (call, callback) => {
        const req = call.request;

        if (req.term > state.currentTerm) {
            stepDown(req.term);
        }

        if (state.role !== 'LEADER') {
            return callback(null, {
                term: state.currentTerm,
                success: false,
                entries: [],
                leader_commit_index: state.commitIndex,
                conflict_index: 0,
                leader_id: (currentLeaderId && currentLeaderId !== NODE_ID) ? currentLeaderId : ''
            });
        }

        lastFetchReceived[req.replica_id] = Date.now();

        // Verificação de consistência do log (Log Matching)
        const replicaLastIndex = Number(req.last_log_index);
        const replicaLastTerm = Number(req.last_log_term);

        if (replicaLastIndex > 0) {
            if (replicaLastIndex > state.log.length) {
                return callback(null, {
                    term: state.currentTerm,
                    success: false,
                    entries: [],
                    leader_commit_index: state.commitIndex,
                    conflict_index: state.log.length + 1,
                    leader_id: NODE_ID
                });
            }

            const leaderEntry = state.log[replicaLastIndex - 1];
            if (leaderEntry.term !== replicaLastTerm) {
                return callback(null, {
                    term: state.currentTerm,
                    success: false,
                    entries: [],
                    leader_commit_index: state.commitIndex,
                    conflict_index: replicaLastIndex,
                    leader_id: NODE_ID
                });
            }
        }

        matchIndex[req.replica_id] = replicaLastIndex;

        const newEntries = state.log.slice(replicaLastIndex);

        checkAndAdvanceCommitIndex();

        callback(null, {
            term: state.currentTerm,
            success: true,
            entries: newEntries,
            leader_commit_index: state.commitIndex,
            conflict_index: 0,
            leader_id: NODE_ID
        });
    },

    // Gravação do Cliente
    Write: (call, callback) => {
        const req = call.request;

        if (state.role !== 'LEADER') {
            const leaderNode = currentLeaderId ? NODES[currentLeaderId] : null;
            const leaderAddr = leaderNode ? `${leaderNode.host}:${leaderNode.port}` : '';
            printStatus(`Escrita rejeitada (não é líder). Redirecionando cliente para ${currentLeaderId || 'líder desconhecido'}`);
            return callback(null, {
                success: false,
                message: currentLeaderId ? `Redirecionamento: o líder atual é ${currentLeaderId}` : 'Nenhum líder eleito no momento',
                leader_id: currentLeaderId || '',
                leader_address: leaderAddr,
                commit_index: state.commitIndex
            });
        }

        if (!hasQuorum()) {
            const activeCount = getActiveNodesCount();
            printStatus(`Escrita RECUSADA: sem quórum (${activeCount}/${QUORUM} nós ativos)`);
            return callback(null, {
                success: false,
                message: `Escrita bloqueada/recusada: quórum insuficiente (${activeCount}/${QUORUM} nós ativos no cluster)`,
                leader_id: NODE_ID,
                leader_address: `${SELF.host}:${SELF.port}`,
                commit_index: state.commitIndex
            });
        }

        const newIndex = state.log.length + 1;
        const entry = {
            index: newIndex,
            term: state.currentTerm,
            key: req.key,
            value: req.value
        };

        state.log.push(entry);
        saveState();
        printStatus(`Nova gravação recebida [Index: ${newIndex}, Key: "${req.key}", Value: "${req.value}"]. Aguardando quórum...`);

        const writeTimeout = setTimeout(() => {
            if (pendingWrites.has(newIndex)) {
                pendingWrites.delete(newIndex);
                callback(null, {
                    success: false,
                    message: 'Tempo limite excedido aguardando confirmação do quórum',
                    leader_id: NODE_ID,
                    leader_address: `${SELF.host}:${SELF.port}`,
                    commit_index: state.commitIndex
                });
            }
        }, 3000);

        pendingWrites.set(newIndex, (res) => {
            clearTimeout(writeTimeout);
            callback(null, res);
        });
    },

    Read: (call, callback) => {
        const { key } = call.request;

        if (state.db && Object.prototype.hasOwnProperty.call(state.db, key)) {
            printStatus(`Consulta atendida: "${key}" = "${state.db[key]}" (Comitado)`);
            callback(null, {
                success: true,
                value: state.db[key],
                message: 'Sucesso',
                commit_index: state.commitIndex
            });
        } else {
            printStatus(`Consulta atendida: Chave "${key}" não encontrada em dados comitados`);
            callback(null, {
                success: false,
                value: '',
                message: `Chave "${key}" não encontrada ou ainda não comitada`,
                commit_index: state.commitIndex
            });
        }
    }
};

function main() {
    const PROTO_PATH = path.join(__dirname, 'proto', 'cluster.proto');
    const packageDef = protoLoader.loadSync(PROTO_PATH, {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    });
    clusterProto = grpc.loadPackageDefinition(packageDef).cluster;

    const server = new grpc.Server();
    server.addService(clusterProto.ClusterService.service, serviceImpl);

    server.bindAsync(`0.0.0.0:${SELF.port}`, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
        if (err) {
            console.error(`Falha ao iniciar nó ${NODE_ID} na porta ${SELF.port}:`, err.message);
            process.exit(1);
        }

        console.log(`====================================================`);
        console.log(`NÓ ${NODE_ID} INICIALIZADO COM SUCESSO`);
        console.log(`Endereço: 0.0.0.0:${boundPort}`);
        console.log(`====================================================`);

        printStatus('Nó iniciado');

        resetElectionTimeout();
        startFetchLoop();
    });
}

main();
