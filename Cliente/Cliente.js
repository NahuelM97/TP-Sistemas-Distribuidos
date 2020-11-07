var zmq = require('zeromq')
    , pubSock = zmq.socket('pub')
    // , subSock = zmq.socket('sub')
    , reqSock = zmq.socket('req');

let userId = 'carlitos';
let coordinadorIP = '127.0.0.1';
let coordinadorPuerto = 1234;

const MESSAGE_TOPIC_PREFIX = 'message';
const HEARTBEAT_TOPIC_NAME = 'heartbeat';

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

var arregloSockets = []; //almacena en formato clave valor nuestros brokers conectados


{// PP
    initClient();

}

function initClient() {
    reqSock.on("message", cbAltaTopico);
    reqSock.connect(`tcp://${coordinadorIP}:${coordinadorPuerto}`);
    var messageInicial = {
        accion: COD_ALTA_SUB,
        topico: `${MESSAGE_TOPIC_PREFIX}/${userId}`
    };

    console.log(`Enviando: ${JSON.stringify(messageInicial)}`);
    reqSock.send(JSON.stringify(messageInicial));



    var messageHeartbeatPub = {
        accion: COD_PUB,
        topico: HEARTBEAT_TOPIC_NAME
    }

    console.log(`Enviando: ${JSON.stringify(messageHeartbeatPub)}`);
    reqSock.send(JSON.stringify(messageHeartbeatPub));
}

function cbAltaTopico(replyJSON) { //me devolvieron el nombre de algun topico
    
    let reply = JSON.parse(replyJSON);
    console.log("Received reply : [", reply, ']');// tiene el formato de un arreglo con 3 objetos que corresponden a 3 brokers


    reply.forEach((broker) => {
        let ipPuerto = `${broker.ip.toString()}:${broker.puerto.toString()}`;
        let socket = getSocketByURL(ipPuerto);
        socket.subscribe(broker.topico.toString());
        socket.on('message', cbMensaje);
    })


}

function cbMensaje(topic, message) {
    if (topic = HEARTBEAT_TOPIC_NAME) { // lo revisamos en todos para mayor flexibilidad
        //actualizo el tiempo de conexion de alguien
    }
    else {
        console.log('Recibio topico:', topic.toString(), 'con mensaje:', message.toString());
    }

}

function getSocketByURL(ipPuerto) {
    if (!arregloSockets.hasOwnProperty(ipPuerto)) { //con este broker no hable nunca, lo agrego a mi lista de brokers
        let nuevoSocket = zmq.socket('sub');

        arregloSockets[ipPuerto.toString()] = nuevoSocket;
        nuevoSocket.connect(`tcp://${ipPuerto.toString()}`);
    }

    return arregloSockets[ipPuerto];
}






