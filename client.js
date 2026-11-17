const readline = require('readline');
const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const NODES = {
  node1: '127.0.0.1:50051',
  node2: '127.0.0.1:50052',
  node3: '127.0.0.1:50053',
  node4: '127.0.0.1:50054'
};

const PROTO_PATH = path.join(__dirname, 'proto', 'cluster.proto');
const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});
const clusterProto = grpc.loadPackageDefinition(packageDef).cluster;

function getClient(address) {
  return new clusterProto.ClusterService(address, grpc.credentials.createInsecure());
}

// Gravação com suporte a Redirecionamento Automático
function writeData(targetAddress, key, value, attempts = 0) {
  if (attempts > 4) {
    console.error('[Cliente] Limite de tentativas de redirecionamento atingido.');
    return;
  }

  const client = getClient(targetAddress);
  console.log(`[Cliente] Enviando WRITE ("${key}" = "${value}") para ${targetAddress}...`);

  client.Write({ key, value }, { deadline: Date.now() + 4000 }, (err, res) => {
    if (err) {
      console.error(`[Cliente] Erro de comunicação com ${targetAddress}:`, err.message);
      return;
    }

    if (res.success) {
      console.log(`✅ [Cliente] Sucesso! Gravado no índice de commit ${res.commit_index} via líder ${res.leader_id}`);
    } else {
      console.log(`⚠️ [Cliente] Resposta: ${res.message}`);
      if (res.leader_address && res.leader_address !== targetAddress) {
        console.log(`🔀 [Cliente] Redirecionando automaticamente para o líder em ${res.leader_address}...`);
        writeData(res.leader_address, key, value, attempts + 1);
      }
    }
  });
}

// Consulta em qualquer nó do cluster (leitura eventual)
function readData(targetAddress, key) {
  const client = getClient(targetAddress);
  console.log(`[Cliente] Enviando READ ("${key}") para ${targetAddress}...`);

  client.Read({ key }, { deadline: Date.now() + 2000 }, (err, res) => {
    if (err) {
      console.error(`[Cliente] Erro ao consultar ${targetAddress}:`, err.message);
      return;
    }

    if (res.success) {
      console.log(`✅ [Cliente] Chave "${key}" encontrada com valor: "${res.value}" (CommitIndex do nó: ${res.commit_index})`);
    } else {
      console.log(`ℹ️ [Cliente] Chave "${key}" não encontrada (CommitIndex do nó: ${res.commit_index}). Detalhes: ${res.message}`);
    }
  });
}

// Interface de Linha de Comando (CLI)
const args = process.argv.slice(2);
const command = args[0];

if (!command) {
  console.log(`
Uso do Cliente:
  node client.js write <chave> <valor> [node1|node2|node3|node4]
  node client.js read <chave> [node1|node2|node3|node4]

Exemplos:
  node client.js write usuario "Carlos" node2
  node client.js read usuario node3
  node client.js read usuario node1
  `);
  process.exit(0);
}

const targetNode = args[3] || args[2] && (command === 'read' ? args[2] : 'node1') || 'node1';
const targetAddress = NODES[targetNode] || targetNode;

if (command === 'write') {
  const key = args[1];
  const value = args[2];
  if (!key || !value) {
    console.error('Uso: node client.js write <chave> <valor> [nó]');
    process.exit(1);
  }
  writeData(targetAddress, key, value);
} else if (command === 'read') {
  const key = args[1];
  if (!key) {
    console.error('Uso: node client.js read <chave> [nó]');
    process.exit(1);
  }
  readData(targetAddress, key);
} else {
  console.error(`Comando desconhecido: ${command}. Use "write" ou "read".`);
}
