var zmq = require('zeromq');

// socket to talk to clients
var repSocket = zmq.socket('rep');

const ipCoordinador = `127.0.0.1`;
const puertoCoordinador = 1234;

//CLIENTE -> COORDINADOR
const COD_PUB = 1; // Cliente publica un nuevo mensaje

const COD_ALTA_SUB = 2; // Cliente se suscribe a un tópico


//COORDINADOR -> BROKER
const COD_ADD_TOPICO_BROKER = 3;// Coordinador informa al broker un nuevo topico 


//SERVIDOR HTTP -> BROKER
const COD_GET_TOPICOS = 4; // Servidor solicita a todos los sus topicos 

const COD_BORRAR_MENSAJES = 6; // Servidor solicita a un broker que borre sus mensajes

//SERVIDOR HTTP -> TODOS LOS BROKERS
const COD_GET_MENSAJES_COLA = 5;// Servidor solicita a todos los brokers todos sus mensajes


//key: el topico
//value: la id del broker q lo tiene
//por ej: 'topico1':1
//el 1 es la posicion en el arreglo de brokers
let topicoIdBroker = {};


//key la id del broker
// value el ippuerto
let brokerIpPuerto = [
    { ip: '127.0.0.1', puertoPub: 3000, puertoSub: 3001 },
    { ip: '127.0.0.1', puertoPub: 3002, puertoSub: 3003 },
    { ip: '127.0.0.1', puertoPub: 3004, puertoSub: 3005 }];

let contadorTopicosPorBroker = [0, 0, 0];

function getIdBrokerMenosTopicos() {
    let minCantTopicos = Math.min(...contadorTopicosPorBroker);
    return contadorTopicosPorBroker.indexOf(minCantTopicos);
}


function getRespuestaTopico(topico,isPub) {
    if (!topicoIdBroker.hasOwnProperty(topico)) {
        //nadie lo tiene, se lo asignamos a alguien
        let idBrokerMenosTopicos = getIdBrokerMenosTopicos();
        topicoIdBroker[topico] = idBrokerMenosTopicos;
        contadorTopicosPorBroker[idBrokerMenosTopicos]++;// TODO PREGUNTAR peligro concurrencia
    }


    let idBroker = topicoIdBroker[topico];
    let ipPuertoBroker = brokerIpPuerto[idBroker];


    let respuesta = {
        ip: ipPuertoBroker.ip,
        puerto: isPub         ? ipPuertoBroker.puertoPub : ipPuertoBroker.puertoSub,
        topico: topico
    }

    return respuesta;
}

repSocket.on('message', function (requestJSON) { //un cliente quiere saber donde estan 1 o 3 topicos
    let request = JSON.parse(requestJSON);
    switch (request.accion) {
        case COD_PUB: {
            repSocket.send(JSON.stringify(getRespuestaTopico(request.topico,true)));
            break;
        }
        case COD_ALTA_SUB: {
            //mandamos los 3 juntos xq una request admite una sola reply (es el protocolo de ZMQ)
            let mensaje = [getRespuestaTopico('heartbeat',false),getRespuestaTopico(request.topico,false),getRespuestaTopico('message/all',false)]; 
            repSocket.send(JSON.stringify(mensaje));
            break;
        }
        default:
            console.error("ERROR 329969420: codigo binario no binario llegad esperado invalido de operacion en la solicitud");
        //algo salio mal esto no tendria que estar aca.
    }
    console.log("Received request: [", request, "]");
});

repSocket.bind(`tcp://${ipCoordinador}:${puertoCoordinador}`);