const zmq = require('../zeromq/node_modules/zeromq');

var pubSocket = zmq.socket('pub')
, reqSocket = zmq.socket('req');

//Es un clave:valor
//    - clave: idPeticion
//    - valor: { idPeticion: globals.generateUUID(), accion: globals.COD_PUB, topico: topico }
var pendingRequests = {};

//Es un clave:valor
//    - clave: idPeticion (mismo que en pendingRequest)
//    - valor: {idPeticion: globals.generateUUID(),emisor: userId, mensaje: contenido, fecha: *}
//
// *La fecha no se guarda, porque se calcula y se guarda antes de enviar el mensaje
var pendingPublications = {};

// Guardamos todas las conexiones abiertas para evitar hacer connect a ippuerto que ya nos conectamos
let conexiones = [];

const DEBUG_MODE = false;


//////////////////////////////////////////////////////////////////////////////////////
//                                      PUB                                         //                                        
//////////////////////////////////////////////////////////////////////////////////////

// Trata de enviar un mensaje, creandolo a partir de un contenido, en un topico (broker).
//  - Si tiene el ip:puerto almacenado del broker, publica
//  - Sino, le envia una solicitud al coordinador para obtener esos datos
//30faab00-2339-4e57-928a-b78cabb4af6c

function initReq(ip,puerto, cbRespuestaCoordinador){
    reqSocket.connect(`tcp://${ip}:${puerto}`);
    reqSocket.on('message', cbRespuestaCoordinador);
}
function intentaPublicarNuevoMensaje(contenido, topico, fechaActual) {
    //Si tengo la ubicacion del topico (broker) guardada, lo envio
    let mensajePub = {
        emisor: userId,
        mensaje: contenido,
        fecha: fechaActual
    }
    if (topicoIpPuertoPub.hasOwnProperty(topico)) {
        publicaEnBroker(mensajePub, topico);
    } else { //Si no lo tengo, se lo pido al coordinador
        let mensajeReq = {
            idPeticion: globals.generateUUID(),
            accion: globals.COD_PUB,
            topico: topico
        }
        solicitarBrokerPubACoordinador(mensajeReq, mensajePub);

    }
}

function intentaPublicarMensajeDeCola(mensaje, topico) {
	//Si tengo la ubicacion del topico (broker) guardada, lo envio   
    if (topicoIpPuertoPub.hasOwnProperty(topico)) {
        publicaEnBroker(mensaje, topico);
    } else { //Si no lo tengo, se lo pido al coordinador
        let mensajeReq = {
            idPeticion: globals.generateUUID(),
            accion: globals.COD_PUB,
            topico: topico
        }
        solicitarBrokerPubACoordinador(mensajeReq, mensaje);

    }
}

//Dado un mensaje, realiza el envio por pubSocket
//  el mensaje es guardado en pendingRequest, para cuando el coordinador nos responda
//  podamos saber que queriamos mandar y que topico
function solicitarBrokerPubACoordinador(mensajeReq, mensajePub) {
    pendingRequests[mensajeReq.idPeticion] = mensajeReq;
    pendingPublications[mensajeReq.idPeticion] = mensajePub;
    socketSendMessage(reqSocket, JSON.stringify(mensajeReq));
}

function conectarseParaPub(ipPuerto) {
    if (!conexiones.includes(ipPuerto)) {
        pubSocket.connect(`tcp://${ipPuerto}`);
        conexiones.push(ipPuerto);
    }
}

//Dado un mensaje, realiza el envio por pubSocket
function publicaEnBroker(mensaje, topico) {
    let mensajePub = [topico, JSON.stringify(mensaje)];
    socketSendMessage(pubSocket, mensajePub);
}

//Dado un socket y un mensaje, realiza el socket.send()
function socketSendMessage(socket, mensaje) {
    // Se guarda en pending requests el envío
    if (mensaje != null) {
        debugConsoleLog(`Se realiza envio: ${JSON.stringify(mensaje)}`);
        socket.send(mensaje);
    }
    else {
        console.error('Mensaje sin idPeticion no pudo ser enviado');
    }
}

function enviarMensajePendiente(reply){
    let broker = reply.resultados.datosBroker[0];
    let ipPuerto = `${broker.ip}:${broker.puerto}`;
    conectarseParaPub(ipPuerto);

    //!!!MUY FEO: DEPENDE DE LA RED, HARDWARE TODO!!!!
    //TODO
    setTimeout(() => {

        //conseguir el mensaje y topico que queriamos enviar
        let mensaje = pendingPublications[reply.idPeticion];
        let topico = pendingRequests[reply.idPeticion].topico;
        delete pendingPublications[reply.idPeticion];
        delete pendingRequests[reply.idPeticion];

        topicoIpPuertoPub[topico] = ipPuerto;
        publicaEnBroker(mensaje, topico);

    }, 200);
}

//////////////////////////////////////////////////////////////////////////////////////
//                                    </PUB>                                        //                                        
//////////////////////////////////////////////////////////////////////////////////////


function debugConsoleLog(message) {
    if (DEBUG_MODE) {
        console.log(message);
    }
}

module.exports = {
    //variables
    reqSocket: reqSocket,
    pendingPublications: pendingPublications,
    pendingRequests: pendingRequests,

    // funciones
    publicaEnBroker: publicaEnBroker,
    conectarseParaPub: conectarseParaPub,
    socketSendMessage: socketSendMessage,
    intentaPublicarNuevoMensaje: intentaPublicarNuevoMensaje,
    intentaPublicarMensajeDeCola:intentaPublicarMensajeDeCola,
    enviarMensajePendiente: enviarMensajePendiente,
    initReq: initReq
}
